import threading
import time
import uuid
import queue
from pybit.unified_trading import HTTP

# ---------- Rate-limited REST requests ----------
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
    candidates = [
        "get_open_orders",
        "query_active_order",
        "get_active_order",
        "query_order",
        "get_order_list",
        "get_open_order",
        "get_orders",
    ]
    last_exc = None
    for name in candidates:
        fn = getattr(session, name, None)
        if not callable(fn):
            continue
        try:
            resp = fn(category="linear", symbol=symbol)
            if isinstance(resp, dict):
                result = resp.get("result")
                if isinstance(result, dict):
                    if "list" in result and isinstance(result["list"], list):
                        return result["list"]
                    if "data" in result and isinstance(result["data"], list):
                        return result["data"]
                if isinstance(result, list):
                    return result
            if isinstance(resp, list):
                return resp
        except Exception as e:
            last_exc = e
            continue
    if last_exc:
        raise last_exc
    raise AttributeError("No supported open-order fetch method found on session")

# ---------- Main function ----------
def trade_tcl(keys_dict, order_dict, tpsl_dict, demo=True, max_wait_seconds=300):
    results = {}
    sessions = {}
    final_summary = {acc: {"filled": [], "canceled": [], "timeout": False, "done": False, "user_cancel": False}
                     for acc in keys_dict.keys()}
    order_timestamps = {}
    cancel_requested = {"flag": False}
    stop_event = threading.Event()

    # per-account mapping: orderLinkId -> limit number
    orderlinkid_to_limit = {acc: {} for acc in keys_dict.keys()}
    # per-account pending orderLinkIds set
    pending_orderlinks = {acc: set() for acc in keys_dict.keys()}

    fill_events = queue.Queue()

    # ---------- Place Orders ----------
    def place_orders(account_name, creds):
        session = HTTP(api_key=creds["api_key"], api_secret=creds["api_secret"], demo=demo)
        sessions[account_name] = session

        try:
            rate_limited_request(account_name, session.set_leverage,
                                 category="linear",
                                 symbol=order_dict["coin"],
                                 buyLeverage=str(order_dict["leverage"]),
                                 sellLeverage=str(order_dict["leverage"]))
        except Exception as e:
            print(f"[{account_name}] ⚠️ Error setting leverage: {e}")

        results[account_name] = []
        order_timestamps[account_name] = time.time()

        for i in range(1, 4):
            order_link_id = f"{account_name}_limit{i}_{uuid.uuid4().hex[:8]}"
            try:
                resp = rate_limited_request(account_name, session.place_order,
                                            category="linear",
                                            symbol=order_dict["coin"],
                                            side=order_dict["side"],
                                            orderType="Limit",
                                            qty=str(order_dict[f"qty{i}"]),
                                            price=str(order_dict[f"limit{i}"]),
                                            timeInForce="GTC",
                                            orderLinkId=order_link_id)
                results[account_name].append({"orderLinkId": order_link_id})
                orderlinkid_to_limit[account_name][order_link_id] = i
                pending_orderlinks[account_name].add(order_link_id)
                print(f"[{account_name}] 📌 Limit{i} placed (orderLinkId={order_link_id}) @ {order_dict[f'limit{i}']}")
            except Exception as e:
                print(f"[{account_name}] ⚠️ Error placing Limit{i}: {e}")
            time.sleep(1)

    # place orders for all accounts in parallel
    threads = []
    for acc, creds in keys_dict.items():
        t = threading.Thread(target=place_orders, args=(acc, creds), daemon=True)
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    print("[DEBUG] ✅ All accounts placed orders.")

    # ---------- TPSL Worker ----------
    def tpsl_worker():
        while not stop_event.is_set():
            try:
                account_name, order_link_id = fill_events.get(timeout=1)
            except queue.Empty:
                continue

            if stop_event.is_set():
                break

            limit_num = orderlinkid_to_limit.get(account_name, {}).get(order_link_id)
            if limit_num is None:
                print(f"[{account_name}] ⚠️ Unknown orderLinkId {order_link_id} in TPSL worker")
                continue

            tp = tpsl_dict.get(f"tp{limit_num}")
            sl = tpsl_dict.get(f"sl{limit_num}")
            if tp is None or sl is None:
                print(f"[{account_name}] ⚠️ Missing TP/SL for limit {limit_num}")
                continue

            # wait for position to be visible
            start_time = time.time()
            position_filled = False
            while not stop_event.is_set() and time.time() - start_time < 15:
                try:
                    pos_resp = rate_limited_request(account_name, sessions[account_name].get_positions,
                                                    category="linear", symbol=tpsl_dict["symbol"])
                    positions = pos_resp.get("result", {}).get("list", [])
                    if positions and float(positions[0].get("size", 0)) > 0:
                        position_filled = True
                        break
                except Exception as e:
                    print(f"[{account_name}] ⚠️ Error checking position: {e}")
                time.sleep(0.5)

            if not position_filled:
                print(f"[{account_name}] ⚠️ Position not found in time for {order_link_id}. Skipping TP/SL for limit {limit_num}")
                continue

            # set trading stop (TP/SL)
            try:
                rate_limited_request(account_name, sessions[account_name].set_trading_stop,
                                     category="linear",
                                     symbol=tpsl_dict["symbol"],
                                     takeProfit=str(tp),
                                     stopLoss=str(sl),
                                     positionIdx=0)
                final_summary[account_name]["filled"].append(f"Limit{limit_num}")
                print(f"[{account_name}] ✅ Limit{limit_num} filled → TP/SL set.")
            except Exception as e:
                print(f"[{account_name}] ⚠️ Error setting TP/SL for Limit{limit_num}: {e}")
                continue

            # **New behavior**: once TP/SL is set for this filled order, cancel remaining pending limit orders for this account
            try:
                cancel_fn = getattr(sessions[account_name], "cancel_order", None) or getattr(sessions[account_name], "cancel_active_order", None)
                for other_link in list(pending_orderlinks[account_name]):
                    if other_link == order_link_id:
                        continue
                    try:
                        if callable(cancel_fn):
                            rate_limited_request(account_name, cancel_fn,
                                                 category="linear", symbol=tpsl_dict["symbol"], orderLinkId=other_link)
                            final_summary[account_name]["canceled"].append(other_link)
                            print(f"[{account_name}] ❌ Cancelled other order {other_link} after TP/SL set.")
                    except Exception as e:
                        print(f"[{account_name}] ⚠️ Error cancelling {other_link}: {e}")
                # clear pending set (we consider the others handled)
                pending_orderlinks[account_name].clear()
            except Exception as e:
                print(f"[{account_name}] ⚠️ Error during post-TP cancel flow: {e}")

    t_tpsl = threading.Thread(target=tpsl_worker, daemon=True)
    t_tpsl.start()

    # ---------- Polling Worker (quiet - no per-second prints) ----------
    def polling_worker():
        processed = {acc: set() for acc in keys_dict.keys()}

        while not stop_event.is_set():
            for acc in keys_dict.keys():
                if stop_event.is_set():
                    break

                session = sessions.get(acc)
                if session is None:
                    continue

                # skip if no pending orders
                if not pending_orderlinks[acc]:
                    continue

                try:
                    try:
                        orders = fetch_open_orders_safe(session, tpsl_dict["symbol"])
                    except Exception as e:
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

                        # if filled, enqueue TPSL if not already processed
                        if str(status).lower() in ("filled", "complete", "closed"):
                            if order_link not in processed[acc]:
                                processed[acc].add(order_link)
                                pending_orderlinks[acc].discard(order_link)
                                fill_events.put((acc, order_link))

                    # Fallback: if a tracked orderLinkId isn't present in open-orders, check order history
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
                                            processed[acc].add(missing_link)
                                            pending_orderlinks[acc].discard(missing_link)
                                            fill_events.put((acc, missing_link))
                                            break
                                else:
                                    pass
                            except Exception as e:
                                print(f"[{acc}] ⚠️ Error checking history for {missing_link}: {e}")

                except Exception as e:
                    print(f"[{acc}] ⚠️ Error polling orders: {e}")

            # sleep in short increments so stop_event is responsive
            for _ in range(10):
                if stop_event.is_set():
                    break
                time.sleep(0.1)

    t_poll = threading.Thread(target=polling_worker, daemon=True)
    t_poll.start()

    # ---------- User cancel listener ----------
    def listen_for_cancel():
        while True:
            user_input = input().strip().lower()
            if user_input == "cancel":
                cancel_requested["flag"] = True
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
                    print(f"[{acc}] ⛔ User requested cancel. Cancelling outstanding orders and closing positions...")
                    try:
                        cancel_fn = getattr(sessions[acc], "cancel_order", None) or getattr(sessions[acc], "cancel_active_order", None)
                        for o in list(results.get(acc, [])):
                            olnk = o.get("orderLinkId")
                            if not olnk:
                                continue
                            if callable(cancel_fn):
                                try:
                                    rate_limited_request(acc, cancel_fn,
                                                         category="linear", symbol=tpsl_dict["symbol"], orderLinkId=olnk)
                                    final_summary[acc]["canceled"].append(olnk)
                                except Exception as e:
                                    print(f"[{acc}] ⚠️ Error cancelling {olnk}: {e}")
                        pos_info = rate_limited_request(acc, sessions[acc].get_positions,
                                                        category="linear", symbol=tpsl_dict["symbol"])
                        for p in pos_info.get("result", {}).get("list", []):
                            size = float(p.get("size", 0))
                            side = p.get("side")
                            if size > 0:
                                close_side = "Sell" if side == "Buy" else "Buy"
                                rate_limited_request(acc, sessions[acc].place_order,
                                                     category="linear",
                                                     symbol=tpsl_dict["symbol"],
                                                     side=close_side,
                                                     orderType="Market",
                                                     qty=str(size),
                                                     reduceOnly=True)
                                print(f"[{acc}] 🛑 Closed {size} {side} position.")
                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error during cancel sequence: {e}")
                    final_summary[acc]["user_cancel"] = True
                    final_summary[acc]["done"] = True
                    stop_event.set()
                    continue

                # timeout handling per account
                if order_timestamps.get(acc) and now - order_timestamps[acc] > max_wait_seconds:
                    print(f"[{acc}] ⏳ Timeout reached, cancelling remaining orders.")
                    try:
                        cancel_fn = getattr(sessions[acc], "cancel_order", None) or getattr(sessions[acc], "cancel_active_order", None)
                        for o in list(results.get(acc, [])):
                            olnk = o.get("orderLinkId")
                            if not olnk:
                                continue
                            if callable(cancel_fn):
                                try:
                                    rate_limited_request(acc, cancel_fn,
                                                         category="linear", symbol=tpsl_dict["symbol"], orderLinkId=olnk)
                                    final_summary[acc]["canceled"].append(olnk)
                                except Exception as e:
                                    print(f"[{acc}] ⚠️ Error cancelling {olnk}: {e}")
                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error during timeout cancel: {e}")
                    final_summary[acc]["timeout"] = True
                    final_summary[acc]["done"] = True
                    continue

                # if any pending orderlinks remain, we're not done
                if pending_orderlinks[acc]:
                    all_done = False
                else:
                    # if no pending and we've recorded fills/cancels, mark done
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
        stop_event.set()

    # wait a short moment for threads to finish
    t_poll.join(timeout=2)
    t_tpsl.join(timeout=2)
    t_listen.join(timeout=0.1)

    print("[DEBUG] Exiting trade_tcl, summary:")
    print(final_summary)
    return final_summary
