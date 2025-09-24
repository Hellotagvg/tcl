import threading
import time
import uuid
import websocket
from pybit.unified_trading import HTTP, WebSocket

# ---------- Ignore ping exceptions ----------
_old_ping = WebSocket._send_custom_ping if hasattr(WebSocket, "_send_custom_ping") else None

def _patched_ping(self):
    try:
        if _old_ping:
            _old_ping(self)
    except websocket._exceptions.WebSocketConnectionClosedException:
        pass  # ignore closed exception

if _old_ping:
    WebSocket._send_custom_ping = _patched_ping

# ---------- Rate-limited REST requests per account ----------
last_request_time = {}
def rate_limited_request(account_name, func, *args, **kwargs):
    now = time.time()
    if account_name in last_request_time:
        elapsed = now - last_request_time[account_name]
        if elapsed < 1:
            time.sleep(1 - elapsed)
    last_request_time[account_name] = time.time()
    return func(*args, **kwargs)

# ---------- Main function ----------
def trade_tcl(keys_dict, order_dict, tpsl_dict, demo=True, max_wait_seconds=300):
    results = {}
    sessions = {}
    final_summary = {acc: {"filled": None, "canceled": [], "timeout": False, "done": False, "user_cancel": False}
                     for acc in keys_dict.keys()}
    order_timestamps = {}
    ws_sessions = {}
    cancel_requested = {"flag": False}

    # ---------- Place Orders ----------
    def place_orders(account_name, creds):
        session = HTTP(api_key=creds["api_key"], api_secret=creds["api_secret"], demo=demo)
        sessions[account_name] = session

        # set leverage
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

        # place 3 limits 1s apart
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
                order_id = resp["result"]["orderId"]
                results[account_name].append({"orderId": order_id, "orderLinkId": order_link_id})
                print(f"[{account_name}] 📌 Placed Limit{i}: {order_id} @ {order_dict[f'limit{i}']}")
            except Exception as e:
                print(f"[{account_name}] ⚠️ Error placing Limit{i}: {e}")
            time.sleep(1)

    threads = []
    for acc, creds in keys_dict.items():
        t = threading.Thread(target=place_orders, args=(acc, creds))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    print("✅ All accounts: 3 limit orders placed each (1s apart per account).")

    # ---------- WebSocket Handlers ----------
    def handle_order_update(account_name, data):
        if "data" not in data:
            return
        for update in data["data"]:
            order_status = update.get("orderStatus")
            order_id = update.get("orderId")
            for i, order_info in enumerate(results[account_name], start=1):
                if order_info["orderId"] == order_id and order_status == "Filled":
                    tp = tpsl_dict[f"tp{i}"]
                    sl = tpsl_dict[f"sl{i}"]
                    try:
                        rate_limited_request(account_name, sessions[account_name].set_trading_stop,
                                             category="linear",
                                             symbol=tpsl_dict["symbol"],
                                             takeProfit=str(tp),
                                             stopLoss=str(sl))
                        final_summary[account_name]["filled"] = f"Limit{i}"
                        print(f"[{account_name}] ✅ Limit{i} filled → TP={tp}, SL={sl} set.")
                    except Exception as e:
                        print(f"[{account_name}] ⚠️ Error setting TP/SL for Limit{i}: {e}")

    def handle_position_update(account_name, data):
        if "data" not in data:
            return
        for pos in data["data"]:
            size = float(pos.get("size", 0))
            if size == 0 and final_summary[account_name]["filled"] and not final_summary[account_name]["done"]:
                filled = final_summary[account_name]["filled"]
                idx = int(filled[-1])
                # cancel remaining limits
                for j in range(idx, 3):
                    try:
                        oid = results[account_name][j]["orderId"]
                        rate_limited_request(account_name, sessions[account_name].cancel_order,
                                             category="linear", symbol=tpsl_dict["symbol"], orderId=oid)
                        final_summary[account_name]["canceled"].append(f"Limit{j+1}")
                        print(f"[{account_name}] ❌ Cancelled Limit{j+1} after {filled} TP/SL closed.")
                    except Exception as e:
                        print(f"[{account_name}] ⚠️ Error cancelling Limit{j+1}: {e}")
                final_summary[account_name]["done"] = True
                print(f"[{account_name}] 🎯 Trading cycle complete.")

    # ---------- Start WebSockets ----------
    for acc, creds in keys_dict.items():
        def ws_thread(acc_name=acc, creds=creds):
            while True:
                try:
                    ws = WebSocket(testnet=demo, channel_type="private",
                                   api_key=creds["api_key"], api_secret=creds["api_secret"])
                    ws.order_stream(lambda d, acc=acc_name: handle_order_update(acc, d))
                    ws.position_stream(lambda d, acc=acc_name: handle_position_update(acc, d))
                    print(f"[{acc_name}] 🔗 WS connected, monitoring orders/positions...")

                    while True:
                        time.sleep(1)
                except Exception as e:
                    print(f"[{acc_name}] ⚠️ WS disconnected or error: {e}, reconnecting in 3s...")
                    time.sleep(3)

        t = threading.Thread(target=ws_thread, daemon=True)
        t.start()
        ws_sessions[acc] = t

    # ---------- User cancel ----------
    def listen_for_cancel():
        while True:
            user_input = input().strip().lower()
            if user_input == "cancel":
                cancel_requested["flag"] = True
                break

    threading.Thread(target=listen_for_cancel, daemon=True).start()

    # ---------- Monitor timeout & user cancel ----------
    while True:
        all_done = True
        now = time.time()

        for acc in keys_dict.keys():
            if not final_summary[acc]["done"]:
                all_done = False

                # user cancel
                if cancel_requested["flag"]:
                    print(f"[{acc}] ⛔ User requested cancel. Cancelling all remaining limits + closing positions...")
                    for i in range(3):
                        try:
                            oid = results[acc][i]["orderId"]
                            rate_limited_request(acc, sessions[acc].cancel_order,
                                                 category="linear", symbol=tpsl_dict["symbol"], orderId=oid)
                        except Exception as e:
                            print(f"[{acc}] ⚠️ Error cancelling Limit{i+1}: {e}")

                    # Close open positions at market
                    try:
                        pos_info = sessions[acc].get_positions(category="linear", symbol=tpsl_dict["symbol"])
                        for p in pos_info["result"]["list"]:
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
                                print(f"[{acc}] 🛑 Closed {size} {side} position at market.")
                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error closing position: {e}")

                    final_summary[acc]["user_cancel"] = True
                    final_summary[acc]["done"] = True
                    print(f"[{acc}] 🛑 Trading cycle ended by user cancel.")

                # timeout
                elif final_summary[acc]["filled"] is None and now - order_timestamps[acc] > max_wait_seconds:
                    print(f"[{acc}] ⏱ Timeout reached. Cancelling all 3 limits + closing positions...")
                    for i in range(3):
                        try:
                            oid = results[acc][i]["orderId"]
                            rate_limited_request(acc, sessions[acc].cancel_order,
                                                 category="linear", symbol=tpsl_dict["symbol"], orderId=oid)
                        except Exception as e:
                            print(f"[{acc}] ⚠️ Error cancelling Limit{i+1}: {e}")

                    try:
                        pos_info = sessions[acc].get_positions(category="linear", symbol=tpsl_dict["symbol"])
                        for p in pos_info["result"]["list"]:
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
                                print(f"[{acc}] 🛑 Closed {size} {side} position at market (timeout).")
                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error closing position on timeout: {e}")

                    final_summary[acc]["timeout"] = True
                    final_summary[acc]["done"] = True
                    print(f"[{acc}] 🛑 Trading cycle ended by timeout.")

        if all_done or cancel_requested["flag"]:
            print("🏁 All accounts complete. Exiting.")
            break

        time.sleep(1)
