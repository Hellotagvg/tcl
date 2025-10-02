#!/usr/bin/env python3
import threading
import time
import uuid
import queue
import requests
import contextlib
import hmac
import hashlib
import json

from pybit.unified_trading import HTTP

# ---------------------- CONFIG ----------------------
# Receive window in milliseconds (10 minutes = 600000)
RECV_WINDOW_MS = 600000

# ---------- Bybit server-time helper & patch ----------
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
def use_bybit_server_time_patch(demo=True, verbose=True):
    """Patch time.time() and time.time_ns() so signatures use Bybit server time.

    Yields True if patched; False if fetch failed (no patch applied).
    Restores originals on exit.
    """
    server_ts = _fetch_bybit_server_time_ms(demo=demo)
    local_ts = int(time.time() * 1000)
    if server_ts is None:
        if verbose:
            print("[bybit-time] WARNING: could not fetch Bybit server time; not patching time()")
        yield False
        return

    offset_ms = server_ts - local_ts
    offset_s = offset_ms / 1000.0
    if verbose:
        print(f"[bybit-time] server_ms={server_ts}, local_ms={local_ts}, offset_ms={offset_ms}")

    orig_time = time.time
    orig_time_ns = getattr(time, "time_ns", None)

    def patched_time():
        return orig_time() + offset_s

    def patched_time_ns():
        return int((orig_time() + offset_s) * 1_000_000_000)

    time.time = patched_time
    if orig_time_ns is not None:
        time.time_ns = patched_time_ns

    try:
        yield True
    finally:
        time.time = orig_time
        if orig_time_ns is not None:
            time.time_ns = orig_time_ns
        if verbose:
            print("[bybit-time] restored original time() and time_ns()")


# ---------- Rate-limited REST requests (simple per-account 1req/sec) ----------
last_request_time = {}


def rate_limited_request(account_name, func, *args, **kwargs):
    now = time.time()
    if account_name in last_request_time:
        elapsed = now - last_request_time[account_name]
        if elapsed < 1:
            time.sleep(1 - elapsed)
    last_request_time[account_name] = time.time()
    return func(*args, **kwargs)


# ---------- Helper to fetch open orders (tries multiple method names) ----------
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
                # Some responses may be in resp["data"] directly
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


# ---------- Signed POST helper (v5) ----------
def _make_signature(api_key, api_secret, recv_window, timestamp_ms, body_json):
    """Bybit v5 sign: HMAC_SHA256(secret, timestamp + apiKey + recvWindow + body_json)"""
    payload = f"{timestamp_ms}{api_key}{recv_window}{body_json}"
    return hmac.new(api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def signed_post(base_url, api_key, api_secret, path, body, recv_window_ms=RECV_WINDOW_MS, timeout=10):
    """
    Send signed POST to Bybit v5.
    body: Python dict -> will be JSON-serialized with separators=(',', ':') (no spaces).
    Returns parsed JSON response.
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


# Action wrappers (per-account)
def make_account_actions(api_key, api_secret, demo=True, recv_window_ms=RECV_WINDOW_MS):
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


# ---------- Main function ----------
def trade_tcl(keys_dict, order_dict, tpsl_dict, demo=True, max_wait_seconds=300):
    """
    keys_dict: {"acc1": {"api_key": "...", "api_secret": "..."}, ...}
    order_dict: {"coin":"BTCUSDT","side":"Buy","leverage":..,"qty1":..,"limit1":..,...}
    tpsl_dict: {"symbol":"BTCUSDT","tp1":..,"sl1":..,...}
    demo: True -> use Bybit Demo environment (pass demo=demo to HTTP)
    """

    # quick check: print drift vs server time (best-effort)
    server_ts = _fetch_bybit_server_time_ms(demo=demo)
    local_ts = int(time.time() * 1000)
    drift = None
    if server_ts is not None:
        drift = local_ts - server_ts
        print(f"[INFO] Local ms: {local_ts} | Bybit server ms: {server_ts} | drift (local - server) = {drift} ms")
        if abs(drift) > RECV_WINDOW_MS:
            print(f"[WARN] Absolute drift ({abs(drift)} ms) exceeds configured recv_window ({RECV_WINDOW_MS} ms).")
    else:
        print("[INFO] Could not determine Bybit server time before start; will try to patch during run.")

    # Use Bybit server time for the entire trading run so every signed call uses corrected time.
    with use_bybit_server_time_patch(demo=demo):
        # State containers
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
        lock = threading.Lock()

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
            # create manual action wrappers bound to this account
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
                    # check success
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
                time.sleep(1)

        # run placement for all accounts in parallel
        threads = []
        for acc, creds in keys_dict.items():
            t = threading.Thread(target=place_orders, args=(acc, creds), daemon=True)
            threads.append(t)
            t.start()
        for t in threads:
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
                    if isinstance(resp, dict) and resp.get("retCode") == 0:
                        with lock:
                            final_summary[account_name]["filled"].append(f"Limit{limit_num}")
                            active_position_flag[account_name] = True
                        print(f"[{account_name}] ✅ Limit{limit_num} filled → TP/SL set (tp={tp} sl={sl}).")
                        t = threading.Thread(target=position_monitor, args=(account_name,), daemon=True)
                        t.start()
                    else:
                        print(f"[{account_name}] ⚠️ set_trading_stop failed: {resp}")
                except Exception as e:
                    print(f"[{account_name}] ⚠️ Error setting TP/SL for Limit{limit_num}: {e}")

        # ---------- Polling Worker: detect fills by orderLinkId (quiet) ----------
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
            """
            Wait until a position appears (size>0) then wait until it is closed (size==0).
            Once closed, cancel any remaining pending limit orders for the account.
            """
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
                                        # cancel via manual signed POST (use orderLinkId)
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

        # Start background threads
        t_poll = threading.Thread(target=polling_worker, daemon=True)
        t_poll.start()

        t_tpsl = threading.Thread(target=tpsl_worker, daemon=True)
        t_tpsl.start()

        # ---------- User cancel listener ----------
        def listen_for_cancel():
            while True:
                user_input = input().strip().lower()
                if user_input == "cancel":
                    cancel_requested["flag"] = True
                    print("[DEBUG] Cancel requested by user.")
                    break

        t_listen = threading.Thread(target=listen_for_cancel, daemon=True)
        t_listen.start()

        # ---------- Monitor orders, cancel/timeouts ----------
        try:
            while True:
                all_done = True
                now = time.time()

                for acc in keys_dict.keys():
                    if final_summary[acc]["done"]:
                        continue

                    if cancel_requested["flag"]:
                        # perform immediate cancel+close flow and stop everything
                        print(f"[{acc}] ⛔ User requested cancel. Cancelling outstanding orders and closing positions...")
                        try:
                            with lock:
                                to_cancel = list(results.get(acc, []))
                            for o in to_cancel:
                                olnk = o.get("orderLinkId")
                                if not olnk:
                                    continue
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

                    # timeout handling
                    if order_timestamps.get(acc) and now - order_timestamps[acc] > max_wait_seconds:
                        print(f"[{acc}] ⏳ Timeout reached, cancelling remaining orders.")
                        try:
                            with lock:
                                to_cancel = list(results.get(acc, []))
                            for o in to_cancel:
                                olnk = o.get("orderLinkId")
                                if not olnk:
                                    continue
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
                        # nothing pending and no active position => done
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

        # wait briefly for threads to exit
        t_poll.join(timeout=2)
        t_tpsl.join(timeout=2)
        t_listen.join(timeout=0.1)

    # after exiting the 'with' block, time() restored
    print("[DEBUG] Exiting trade_tcl, summary:")
    print(final_summary)
    return final_summary
