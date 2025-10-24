#!/usr/bin/env python3
"""
trade_tcl_ntp_safe.py

Bybit trading run helper (NTP-based time patch + re-entrant safe).

Features:
- Applies NTP-based time offset (preferred) and falls back to Bybit endpoints if needed.
- Patches time.time() and time.time_ns() for the duration of each run so HMAC timestamps align.
- Safe to call trade_tcl(...) multiple times inside the same Python process: global runtime
  state is cleared and all threads/sessions are cleaned up at the end of the run.
- Treats Bybit retCode 34040 ("not modified") as success for TPSL setting.
- Keeps debug logging similar to the original script.

Requirements:
    pip install ntplib requests pybit

Usage:
    from trade_tcl_ntp_safe import trade_tcl
    trade_tcl(keys_dict, order_dict, tpsl_dict, demo=True)

"""

import threading
import time
import uuid
import queue
import requests
import contextlib
import hmac
import hashlib
import json
import ntplib
import traceback
import gc

from pybit.unified_trading import HTTP  # your original import

# ---------------------- CONFIG ----------------------
RECV_WINDOW_MS = 600000  # 10 minutes
NTP_SERVERS = ["pool.ntp.org", "time.google.com", "time.cloudflare.com"]

# ---------------------- Global runtime/shared state ----------------------
# Only a rate-limit map is global; it's cleared at the start of each run
last_request_time = {}
_state_lock = threading.RLock()

# ---------------------- Time helpers ----------------------
def _fetch_ntp_time_ms(servers=None, timeout=5):
    """Return time from NTP server in milliseconds, or None on failure."""
    if servers is None:
        servers = NTP_SERVERS
    client = ntplib.NTPClient()
    for s in servers:
        try:
            resp = client.request(s, version=3, timeout=timeout)
            return int(resp.tx_time * 1000)
        except Exception:
            continue
    return None


def _fetch_bybit_server_time_ms(demo=True, timeout=5):
    """Try several Bybit time endpoints and return server_time_ms or None."""
    candidates = []
    if demo:
        candidates += [
            "https://api-demo.bybit.com/v5/public/time",
            "https://api-demo.bybit.com/v2/public/time",
        ]
    candidates += [
        "https://api.bybit.com/v5/public/time",
        "https://api.bybit.com/v2/public/time",
    ]
    for url in candidates:
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            server_ts = None
            if isinstance(j, dict):
                if "time" in j:
                    server_ts = int(j["time"])
                elif "time_now" in j:
                    try:
                        server_ts = int(float(j["time_now"]) * 1000)
                    except Exception:
                        server_ts = int(j["time_now"])
                elif "result" in j and isinstance(j["result"], dict) and "time" in j["result"]:
                    server_ts = int(j["result"]["time"])
            if server_ts is not None:
                if server_ts < 10**12:
                    server_ts = int(server_ts * 1000)
                return server_ts
        except Exception:
            continue
    return None


@contextlib.contextmanager
def use_ntp_time_patch(verbose=True, ntp_servers=None, demo_fallback=True):
    """
    Patch time.time() and time.time_ns() so HMAC timestamps use authoritative NTP/Bybit time.
    Yields True if patched; False if no patch applied.
    Restores originals on exit.
    """
    ntp_ts = _fetch_ntp_time_ms(servers=ntp_servers or NTP_SERVERS)
    source = "NTP"
    server_ts = ntp_ts
    if server_ts is None and demo_fallback:
        server_ts = _fetch_bybit_server_time_ms(demo=True)
        source = "Bybit"

    if server_ts is None:
        if verbose:
            print("[time-patch] WARNING: could not fetch NTP/Bybit time; not patching time()")
        yield False
        return

    local_ts = int(time.time() * 1000)
    offset_ms = server_ts - local_ts
    offset_s = offset_ms / 1000.0

    if verbose:
        print(f"[time-patch] source={source} server_ms={server_ts}, local_ms={local_ts}, offset_ms={offset_ms}")

    orig_time = time.time
    orig_time_ns = getattr(time, "time_ns", None)

    def patched_time():
        return orig_time() + offset_s

    def patched_time_ns():
        return int((orig_time() + offset_s) * 1_000_000_000)

    # Apply patch
    time.time = patched_time
    if orig_time_ns is not None:
        time.time_ns = patched_time_ns

    try:
        yield True
    finally:
        # Restore originals
        time.time = orig_time
        if orig_time_ns is not None:
            time.time_ns = orig_time_ns
        if verbose:
            print("[time-patch] restored original time() and time_ns()")


# ---------------------- Rate limiting helper ----------------------
def rate_limited_request(account_name, func, *args, **kwargs):
    """
    Simple per-account 1 request/sec rate limiter.
    Uses global last_request_time map which is reset at the start of each trade_tcl run.
    """
    with _state_lock:
        now = time.time()
        last = last_request_time.get(account_name)
        if last is not None:
            elapsed = now - last
            if elapsed < 1:
                # sleep outside lock in small increments
                sleep_for = 1 - elapsed
                # release temporarily to avoid blocking other accounts
                _state_lock.release()
                try:
                    time.sleep(sleep_for)
                finally:
                    _state_lock.acquire()
        last_request_time[account_name] = time.time()
    return func(*args, **kwargs)


# ---------------------- Signature helper ----------------------
def _make_signature(api_key, api_secret, recv_window, timestamp_ms, body_json):
    payload = f"{timestamp_ms}{api_key}{recv_window}{body_json}"
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_post(base_url, api_key, api_secret, path, body, recv_window_ms=RECV_WINDOW_MS, timeout=10):
    """
    Send signed POST to Bybit v5 using patched time() when active.
    Returns parsed JSON response or dict with http_status/text on non-json.
    """
    timestamp = str(int(time.time() * 1000))
    body_json = json.dumps(body, separators=(",", ":"))
    sign = _make_signature(api_key, api_secret, str(recv_window_ms), timestamp, body_json)

    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": sign,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-RECV-WINDOW": str(recv_window_ms),
        "Content-Type": "application/json",
    }

    url = base_url.rstrip("/") + path
    resp = requests.post(url, headers=headers, data=body_json, timeout=timeout)
    try:
        return resp.json()
    except ValueError:
        return {"http_status": resp.status_code, "text": resp.text}


def make_account_actions(api_key, api_secret, demo=True, recv_window_ms=RECV_WINDOW_MS):
    """Manual signed POST wrappers per-account (so we don't rely on pybit POST quirks)."""
    base = "https://api-demo.bybit.com" if demo else "https://api.bybit.com"

    def place_order(body):
        return signed_post(base, api_key, api_secret, "/v5/order/create", body, recv_window_ms)

    def cancel_order(body):
        return signed_post(base, api_key, api_secret, "/v5/order/cancel", body, recv_window_ms)

    def set_trading_stop(body):
        return signed_post(base, api_key, api_secret, "/v5/position/trading-stop", body, recv_window_ms)

    def set_leverage(body):
        return signed_post(base, api_key, api_secret, "/v5/position/set-leverage", body, recv_window_ms)

    return {
        "place_order": place_order,
        "cancel_order": cancel_order,
        "set_trading_stop": set_trading_stop,
        "set_leverage": set_leverage,
        "base_url": base,
    }


# ---------------------- Safe runtime reset ----------------------
def reset_runtime_state():
    """Clear global runtime maps and attempt to encourage GC so leftover sessions/threads are freed."""
    global last_request_time
    with _state_lock:
        last_request_time.clear()
    # Attempt garbage collection - helpful if some objects reference requests sessions
    try:
        gc.collect()
    except Exception:
        pass
    print("[DEBUG] reset_runtime_state(): cleared global runtime maps and ran GC.")


# ---------------------- Helper to fetch open orders (as before) ----------------------
def fetch_open_orders_safe(session, symbol):
    """
    Try several common pybit method names to obtain open/active orders.
    Returns a list (possibly empty) of order dicts.
    Raises exception if none of the method calls work (bubbles last exception).
    """
    candidates = [
        "get_open_orders",
        "query_active_order",
        "get_active_order",
        "query_order",
        "get_order_list",
        "get_open_order",
        "get_orders",
        "get_order_history",
    ]
    last_exc = None
    for name in candidates:
        fn = getattr(session, name, None)
        if not callable(fn):
            continue
        try:
            resp = fn(category="linear", symbol=symbol)
            # Normalize common response shapes
            if isinstance(resp, dict):
                result = resp.get("result")
                if isinstance(result, dict):
                    if "list" in result and isinstance(result["list"], list):
                        return result["list"]
                    if "data" in result and isinstance(result["data"], list):
                        return result["data"]
                if isinstance(result, list):
                    return result
                if "data" in resp and isinstance(resp["data"], list):
                    return resp["data"]
            if isinstance(resp, list):
                return resp
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise last_exc
    raise AttributeError("No supported open-order fetch method found on session")


# ---------------------- Main run (re-entrant) ----------------------
def trade_tcl(keys_dict, order_dict, tpsl_dict, demo=True, max_wait_seconds=300):
    """
    Main trading function (safe to call repeatedly in the same interpreter).

    keys_dict: {"acc1": {"api_key": "...", "api_secret": "..."}, ...}
    order_dict: {"coin":"BTCUSDT","side":"Buy","leverage":..,"qty1":..,"limit1":..,...}
    tpsl_dict: {"symbol":"BTCUSDT","tp1":..,"sl1":..,...}
    demo: True -> use Bybit demo endpoints
    """
    # Reset global runtime state for a clean run
    reset_runtime_state()

    # Informational check: NTP vs local drift (best-effort)
    try:
        ntp_ts = _fetch_ntp_time_ms()
        local_ts = int(time.time() * 1000)
        if ntp_ts is not None:
            drift = local_ts - ntp_ts
            print(f"[INFO] Local ms: {local_ts} | NTP ms: {ntp_ts} | drift (local - server) = {drift} ms")
            if abs(drift) > RECV_WINDOW_MS:
                print(f"[WARN] Absolute drift ({abs(drift)} ms) exceeds recv_window ({RECV_WINDOW_MS} ms).")
        else:
            bybit_ts = _fetch_bybit_server_time_ms(demo=demo)
            if bybit_ts is not None:
                drift = local_ts - bybit_ts
                print(f"[INFO] Local ms: {local_ts} | Bybit ms: {bybit_ts} | drift (local - server) = {drift} ms (NTP unavailable)")
            else:
                print("[INFO] Could not determine authoritative server time before start.")
    except Exception:
        print("[INFO] Time check failed (exception). Continuing.")

    # Use authoritative time for the duration of the run
    with use_ntp_time_patch(verbose=True, ntp_servers=NTP_SERVERS, demo_fallback=True):
        # Per-run local state (guaranteed fresh each call)
        results = {}   # per-account placed orders list of {"orderLinkId":...}
        sessions = {}  # per-account HTTP (pybit) session
        actions = {}   # per-account manual POST actions (place/cancel/set)
        final_summary = {acc: {"filled": [], "canceled": [], "timeout": False, "done": False, "user_cancel": False}
                         for acc in keys_dict.keys()}
        order_timestamps = {}
        cancel_requested = {"flag": False}
        stop_event = threading.Event()

        # per-account mapping orderLinkId -> limit number
        orderlinkid_to_limit = {acc: {} for acc in keys_dict.keys()}
        pending_orderlinks = {acc: set() for acc in keys_dict.keys()}

        # processed markers to avoid duplicate handling
        processed_fills = {acc: set() for acc in keys_dict.keys()}

        # flag indicating account currently has a monitored active position (TP/SL set)
        active_position_flag = {acc: False for acc in keys_dict.keys()}

        # lock for modifying shared structures safely
        lock = threading.RLock()

        fill_events = queue.Queue()

        # ---------- Place Orders ----------
        def place_orders(account_name, creds):
            print(f"[DEBUG] [{account_name}] Initializing HTTP session (recv_window={RECV_WINDOW_MS})...")
            # keep pybit session for GETs
            try:
                session = HTTP(api_key=creds["api_key"], api_secret=creds["api_secret"], demo=demo, recv_window=RECV_WINDOW_MS)
            except TypeError:
                session = HTTP(api_key=creds["api_key"], api_secret=creds["api_secret"], testnet=demo, recv_window=RECV_WINDOW_MS)
            sessions[account_name] = session
            actions[account_name] = make_account_actions(creds["api_key"], creds["api_secret"], demo=demo, recv_window_ms=RECV_WINDOW_MS)

            # set leverage via signed manual POST (pybit's POST had issues)
            try:
                lev_body = {
                    "category": "linear",
                    "symbol": order_dict["coin"],
                    "buyLeverage": str(order_dict["leverage"]),
                    "sellLeverage": str(order_dict["leverage"])
                }
                rate_limited_request(account_name, actions[account_name]["set_leverage"], lev_body)
            except Exception as e:
                print(f"[{account_name}] ⚠️ Error setting leverage (manual): {e}")

            results[account_name] = []
            order_timestamps[account_name] = time.time()

            for i in range(1, 4):
                order_link_id = f"{account_name}_limit{i}_{uuid.uuid4().hex[:8]}"
                try:
                    body = {
                        "category": "linear",
                        "symbol": order_dict["coin"],
                        "side": order_dict["side"],
                        "orderType": "Limit",
                        "qty": str(order_dict[f"qty{i}"]),
                        "price": str(order_dict[f"limit{i}"]),
                        "timeInForce": "GTC",
                        "orderLinkId": order_link_id
                    }
                    resp = rate_limited_request(account_name, actions[account_name]["place_order"], body)
                    # check success (Bybit v5 typical success is retCode == 0)
                    if isinstance(resp, dict) and resp.get("retCode") == 0:
                        with lock:
                            results[account_name].append({"orderLinkId": order_link_id})
                            orderlinkid_to_limit[account_name][order_link_id] = i
                            pending_orderlinks[account_name].add(order_link_id)
                        print(f"[{account_name}] 📌 Limit{i} placed (orderLinkId={order_link_id}) @ {order_dict[f'limit{i}']}")
                    else:
                        print(f"[{account_name}] ⚠️ Error placing Limit{i} (manual): {resp}")
                except Exception as e:
                    print(f"[{account_name}] ⚠️ Exception placing Limit{i}: {e}")
                # respect 1 req/sec between placement calls (rate_limited_request will throttle but keep small sleep)
                time.sleep(1)

        # run placement for all accounts in parallel (join before continuing)
        place_threads = []
        for acc, creds in keys_dict.items():
            t = threading.Thread(target=place_orders, args=(acc, creds), name=f"place_{acc}", daemon=False)
            place_threads.append(t)
            t.start()
        for t in place_threads:
            t.join()

        print("[DEBUG] ✅ All accounts placed orders.")

        # ---------- TPSL Worker: sets TP/SL when a tracked orderLinkId fills ----------
        def tpsl_worker():
            while not stop_event.is_set():
                try:
                    account_name, order_link_id = fill_events.get(timeout=1)
                except queue.Empty:
                    continue

                if stop_event.is_set():
                    break

                with lock:
                    if order_link_id in processed_fills[account_name]:
                        continue
                    processed_fills[account_name].add(order_link_id)

                limit_num = orderlinkid_to_limit.get(account_name, {}).get(order_link_id)
                if limit_num is None:
                    print(f"[{account_name}] ⚠️ Unknown orderLinkId {order_link_id} in TPSL worker")
                    continue

                tp = tpsl_dict.get(f"tp{limit_num}")
                sl = tpsl_dict.get(f"sl{limit_num}")
                if tp is None or sl is None:
                    print(f"[{account_name}] ⚠️ Missing TP/SL for limit {limit_num}")
                    continue

                # set trading stop (TP/SL) via manual signed POST
                try:
                    body = {
                        "category": "linear",
                        "symbol": tpsl_dict["symbol"],
                        "takeProfit": str(tp),
                        "stopLoss": str(sl),
                        "positionIdx": 0
                    }
                    resp = rate_limited_request(account_name, actions[account_name]["set_trading_stop"], body)
                    code = None
                    if isinstance(resp, dict):
                        code = resp.get("retCode")
                    # Treat normal success (0) and "not modified" (34040) as okay
                    if code in (0, 34040):
                        with lock:
                            final_summary[account_name]["filled"].append(f"Limit{limit_num}")
                            active_position_flag[account_name] = True
                        if code == 0:
                            print(f"[{account_name}] ✅ Limit{limit_num} filled → TP/SL set (tp={tp} sl={sl}).")
                        else:
                            print(f"[{account_name}] ⚙️ Limit{limit_num} TP/SL already correct (not modified).")
                        # Start position monitor for this account if not already running
                        t = threading.Thread(target=position_monitor, args=(account_name,), name=f"posmon_{account_name}", daemon=False)
                        t.start()
                    else:
                        print(f"[{account_name}] ⚠️ set_trading_stop failed: {resp}")
                except Exception as e:
                    print(f"[{account_name}] ⚠️ Error setting TP/SL for Limit{limit_num}: {e}")

        # ---------- Polling Worker: detect fills by orderLinkId ----------
        def polling_worker():
            processed = {acc: set() for acc in keys_dict.keys()}

            while not stop_event.is_set():
                for acc in keys_dict.keys():
                    if stop_event.is_set():
                        break
                    session = sessions.get(acc)
                    if session is None:
                        continue

                    # skip if no pending orders for this account
                    if not pending_orderlinks[acc]:
                        continue

                    try:
                        try:
                            orders = fetch_open_orders_safe(session, tpsl_dict["symbol"])
                        except Exception:
                            orders = []

                        found_links = set()
                        for order in orders:
                            order_link = order.get("orderLinkId")
                            status = order.get("orderStatus") or order.get("status") or order.get("order_status")
                            if not order_link:
                                continue
                            found_links.add(order_link)

                            if order_link not in orderlinkid_to_limit.get(acc, {}):
                                continue

                            # If filled, enqueue TPSL handling (only once)
                            if str(status).lower() in ("filled", "complete", "closed"):
                                if order_link not in processed[acc]:
                                    processed[acc].add(order_link)
                                    with lock:
                                        pending_orderlinks[acc].discard(order_link)
                                    fill_events.put((acc, order_link))
                                    print(f"[DEBUG] [{acc}] Order {order_link} detected as filled (status={status}).")

                        # Fallback: orders might disappear from open-orders when filled.
                        missing = set(pending_orderlinks[acc]) - found_links
                        if missing:
                            for missing_link in list(missing):
                                if stop_event.is_set():
                                    break
                                try:
                                    history_fn = getattr(session, "get_order_history", None) or getattr(session, "query_order", None) or getattr(session, "query_active_order", None) or getattr(session, "get_orders", None)
                                    if callable(history_fn):
                                        resp = rate_limited_request(acc, history_fn,
                                                                    category="linear",
                                                                    symbol=tpsl_dict["symbol"],
                                                                    orderLinkId=missing_link,
                                                                    limit=20)
                                        hist = []
                                        if isinstance(resp, dict):
                                            res = resp.get("result")
                                            if isinstance(res, dict):
                                                hist = res.get("list") or res.get("data") or []
                                            elif isinstance(res, list):
                                                hist = res
                                        for rec in hist:
                                            status = rec.get("orderStatus") or rec.get("status")
                                            if str(status).lower() in ("filled", "complete", "closed"):
                                                with lock:
                                                    pending_orderlinks[acc].discard(missing_link)
                                                fill_events.put((acc, missing_link))
                                                print(f"[DEBUG] [{acc}] (history) Order {missing_link} detected as filled (status={status}).")
                                                break
                                except Exception as e:
                                    print(f"[{acc}] ⚠️ Error checking history for {missing_link}: {e}")

                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error polling orders: {e}")

                # responsive sleep (breakable by stop_event)
                for _ in range(10):
                    if stop_event.is_set():
                        break
                    time.sleep(0.1)

        # ---------- Position monitor: waits until position closes, then cancels remaining limits ----------
        def position_monitor(account_name):
            waited_for_position = False
            while not stop_event.is_set():
                if not active_position_flag.get(account_name):
                    time.sleep(0.5)
                    continue

                try:
                    pos_resp = rate_limited_request(account_name, sessions[account_name].get_positions,
                                                    category="linear", symbol=tpsl_dict["symbol"])
                    positions = pos_resp.get("result", {}).get("list", [])
                    size = 0.0
                    if positions:
                        try:
                            size = float(positions[0].get("size", 0))
                        except Exception:
                            size = 0.0
                    if not waited_for_position:
                        if size > 0:
                            waited_for_position = True
                            print(f"[{account_name}] 🔎 Position detected (size={size}). Now monitoring for close (TP/SL).")
                    else:
                        if size == 0:
                            print(f"[{account_name}] ✅ Position closed (TP/SL hit or manual close). Cancelling remaining limit orders...")
                            try:
                                with lock:
                                    to_cancel = list(pending_orderlinks[account_name])
                                for link in to_cancel:
                                    try:
                                        cancel_body = {"category":"linear","symbol":tpsl_dict["symbol"], "orderLinkId": link}
                                        resp = rate_limited_request(account_name, actions[account_name]["cancel_order"], cancel_body)
                                        with lock:
                                            final_summary[account_name]["canceled"].append(link)
                                            pending_orderlinks[account_name].discard(link)
                                        print(f"[{account_name}] ❌ Cancelled leftover order {link} after position closed. resp={resp}")
                                    except Exception as e:
                                        print(f"[{account_name}] ⚠️ Error cancelling {link}: {e}")
                            except Exception as e:
                                print(f"[{account_name}] ⚠️ Error during cancel-after-close: {e}")

                            with lock:
                                active_position_flag[account_name] = False
                            return
                except Exception as e:
                    print(f"[{account_name}] ⚠️ Error fetching positions: {e}")
                for _ in range(5):
                    if stop_event.is_set():
                        break
                    time.sleep(0.2)

        # ---------- Start background threads ----------
        threads = []

        t_poll = threading.Thread(target=polling_worker, name="polling_worker", daemon=False)
        t_poll.start()
        threads.append(t_poll)

        t_tpsl = threading.Thread(target=tpsl_worker, name="tpsl_worker", daemon=False)
        t_tpsl.start()
        threads.append(t_tpsl)

        # ---------- User cancel listener ----------
        def listen_for_cancel():
            while True:
                try:
                    user_input = input().strip().lower()
                except Exception:
                    # input might be closed in some contexts
                    break
                if user_input == "cancel":
                    cancel_requested["flag"] = True
                    print("[DEBUG] Cancel requested by user.")
                    break

        t_listen = threading.Thread(target=listen_for_cancel, name="listen_for_cancel", daemon=False)
        t_listen.start()
        threads.append(t_listen)

        # ---------- Monitor/Controller loop ----------
        try:
            while True:
                all_done = True
                now = time.time()

                for acc in keys_dict.keys():
                    if final_summary[acc]["done"]:
                        continue

                    if cancel_requested["flag"]:
                        print(f"[{acc}] ⛔ User requested cancel. Cancelling outstanding orders and closing positions...")
                        try:
                            with lock:
                                to_cancel = [o.get("orderLinkId") for o in results.get(acc, []) if o.get("orderLinkId")]
                            for olnk in to_cancel:
                                try:
                                    cancel_body = {"category":"linear","symbol":tpsl_dict["symbol"], "orderLinkId":olnk}
                                    resp = rate_limited_request(acc, actions[acc]["cancel_order"], cancel_body)
                                    with lock:
                                        final_summary[acc]["canceled"].append(olnk)
                                except Exception as e:
                                    print(f"[{acc}] ⚠️ Error cancelling {olnk}: {e}")
                            # close positions (if any) using manual signed POST market close
                            pos_info = rate_limited_request(acc, sessions[acc].get_positions,
                                                            category="linear", symbol=tpsl_dict["symbol"])
                            for p in pos_info.get("result", {}).get("list", []):
                                size = float(p.get("size", 0))
                                side = p.get("side")
                                if size > 0:
                                    close_side = "Sell" if side == "Buy" else "Buy"
                                    close_body = {
                                        "category":"linear",
                                        "symbol": tpsl_dict["symbol"],
                                        "side": close_side,
                                        "orderType": "Market",
                                        "qty": str(size),
                                        "reduceOnly": True,
                                        "timeInForce":"GTC",
                                        "orderLinkId": f"close_{acc}_{int(time.time()*1000)}"
                                    }
                                    resp = rate_limited_request(acc, actions[acc]["place_order"], close_body)
                                    print(f"[{acc}] 🛑 Close resp: {resp}")
                        except Exception as e:
                            print(f"[{acc}] ⚠️ Error during cancel sequence: {e}")
                        final_summary[acc]["user_cancel"] = True
                        final_summary[acc]["done"] = True
                        stop_event.set()
                        continue

                    # timeout handling (per-account)
                    if order_timestamps.get(acc) and now - order_timestamps[acc] > max_wait_seconds:
                        print(f"[{acc}] ⏳ Timeout reached, cancelling remaining orders.")
                        try:
                            with lock:
                                to_cancel = [o.get("orderLinkId") for o in results.get(acc, []) if o.get("orderLinkId")]
                            for olnk in to_cancel:
                                try:
                                    cancel_body = {"category":"linear","symbol":tpsl_dict["symbol"], "orderLinkId":olnk}
                                    resp = rate_limited_request(acc, actions[acc]["cancel_order"], cancel_body)
                                    with lock:
                                        final_summary[acc]["canceled"].append(olnk)
                                except Exception as e:
                                    print(f"[{acc}] ⚠️ Error cancelling {olnk}: {e}")
                        except Exception as e:
                            print(f"[{acc}] ⚠️ Error during timeout cancel: {e}")
                        final_summary[acc]["timeout"] = True
                        final_summary[acc]["done"] = True
                        continue

                    # if there are pending orders or active position, we are not done yet
                    if pending_orderlinks[acc] or active_position_flag[acc]:
                        all_done = False
                    else:
                        final_summary[acc]["done"] = True

                if all_done:
                    stop_event.set()
                    break

                # responsive sleep
                for _ in range(10):
                    if stop_event.is_set():
                        break
                    time.sleep(0.1)

        except KeyboardInterrupt:
            print("[DEBUG] KeyboardInterrupt received, stopping.")
            stop_event.set()
        except Exception as e:
            print("[DEBUG] Unexpected exception in controller loop:", e)
            traceback.print_exc()
            stop_event.set()

        # ------- Clean up threads and sessions -------
        # Wait for threads to exit (with timeout). Threads created locally are non-daemon so they should exit quickly.
        for t in threads:
            if t.is_alive():
                try:
                    t.join(timeout=2)
                except Exception:
                    pass

        # Also join any position monitor threads (they were created dynamically)
        # We attempt to be defensive: iterate over threading.enumerate() and join any named posmon_* or place_* threads
        for t in threading.enumerate():
            if t.name.startswith(("posmon_", "place_")) and t is not threading.current_thread():
                try:
                    t.join(timeout=1)
                except Exception:
                    pass

        # Close sessions if possible
        for acc, s in list(sessions.items()):
            try:
                # pybit HTTP wrapper may hold a requests.Session in attribute 'session' or 'http'
                sess_obj = getattr(s, "session", None) or getattr(s, "http", None)
                if hasattr(sess_obj, "close"):
                    sess_obj.close()
            except Exception:
                pass

        # final garbage collect
        try:
            gc.collect()
        except Exception:
            pass

    # after exiting 'with', restored original time()
    print("[DEBUG] Exiting trade_tcl, summary:")
    print(json.dumps(final_summary, indent=2))
    return final_summary
