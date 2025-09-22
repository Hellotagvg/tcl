import threading
import time
import uuid
from pybit.unified_trading import HTTP, WebSocket

# track last request time per account
last_request_time = {}

def rate_limited_request(account_name, func, *args, **kwargs):
    """Wraps REST calls so each account only makes 1 req/sec"""
    now = time.time()
    if account_name in last_request_time:
        elapsed = now - last_request_time[account_name]
        if elapsed < 1:
            time.sleep(1 - elapsed)
    last_request_time[account_name] = time.time()

    return func(*args, **kwargs)


def trade_tcl(keys_dict, order_dict, tpsl_dict, demo_trading=True, max_wait_seconds=300):
    """
    WebSocket-based limit order manager with TP/SL, timeout, and auto-stop.
    Enforces 1 req/sec per account.
    User can type 'cancel' in console to cancel all 3 limits and exit.
    """

    results = {}
    sessions = {}
    final_summary = {acc: {"filled": None, "canceled": [], "timeout": False, "done": False, "user_cancel": False}
                     for acc in keys_dict.keys()}
    order_timestamps = {}
    ws_sessions = {}
    cancel_requested = {"flag": False}  # shared cancel flag

    # ---------- Step 1: Place Orders ----------
    def place_orders(account_name, creds):
        session = HTTP(api_key=creds["api_key"], api_secret=creds["api_secret"], demo_trading=demo_trading)
        sessions[account_name] = session

        # leverage
        try:
            rate_limited_request(account_name, session.set_leverage,
                category="linear",
                symbol=order_dict["coin"],
                buyLeverage=str(order_dict["leverage"]),
                sellLeverage=str(order_dict["leverage"])
            )
        except Exception as e:
            print(f"[{account_name}] ⚠️ Error setting leverage: {e}")

        results[account_name] = []
        order_timestamps[account_name] = time.time()

        # place 3 limits with enforced 1s spacing
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
                    orderLinkId=order_link_id
                )
                order_id = resp["result"]["orderId"]
                results[account_name].append({"orderId": order_id, "orderLinkId": order_link_id})
                print(f"[{account_name}] 📌 Placed Limit{i}: {order_id} @ {order_dict[f'limit{i}']}")
            except Exception as e:
                print(f"[{account_name}] ⚠️ Error placing Limit{i}: {e}")

    threads = []
    for acc, creds in keys_dict.items():
        t = threading.Thread(target=place_orders, args=(acc, creds))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()

    print("✅ All accounts: 3 limit orders placed each (1s apart, per account).")

    # ---------- Step 2: WebSocket Monitoring ----------
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
                            stopLoss=str(sl)
                        )
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
                # cancel later limits
                if idx == 1:
                    for j in [2,3]:
                        try:
                            oid = results[account_name][j-1]["orderId"]
                            rate_limited_request(account_name, sessions[account_name].cancel_order,
                                category="linear", symbol=tpsl_dict["symbol"], orderId=oid)
                            final_summary[account_name]["canceled"].append(f"Limit{j}")
                            print(f"[{account_name}] ❌ Cancelled Limit{j} after Limit1 TP/SL closed.")
                        except Exception as e:
                            print(f"[{account_name}] ⚠️ Error cancelling Limit{j}: {e}")
                elif idx == 2:
                    try:
                        oid = results[account_name][2]["orderId"]
                        rate_limited_request(account_name, sessions[account_name].cancel_order,
                            category="linear", symbol=tpsl_dict["symbol"], orderId=oid)
                        final_summary[account_name]["canceled"].append("Limit3")
                        print(f"[{account_name}] ❌ Cancelled Limit3 after Limit2 TP/SL closed.")
                    except Exception as e:
                        print(f"[{account_name}] ⚠️ Error cancelling Limit3: {e}")
                elif idx == 3:
                    print(f"[{account_name}] ℹ️ Limit3 TP/SL closed. No cancels.")

                final_summary[account_name]["done"] = True
                print(f"[{account_name}] 🎯 Trading cycle complete.")

    # WebSocket subscriptions
    for acc, creds in keys_dict.items():
        try:
            ws = WebSocket(testnet=demo_trading, channel_type="private",
                           api_key=creds["api_key"], api_secret=creds["api_secret"])
            ws_sessions[acc] = ws
            ws.order_stream(lambda d, acc=acc: handle_order_update(acc, d))
            ws.position_stream(lambda d, acc=acc: handle_position_update(acc, d))
        except Exception as e:
            print(f"[{acc}] ⚠️ Error initializing WebSocket: {e}")

    print("🔎 WebSocket monitoring started (fills + TP/SL execution)...")
    print("💡 Type 'cancel' at any time to cancel all remaining limits and exit.")

    # ---------- Step 3: User Cancel Thread ----------
    def listen_for_cancel():
        while True:
            user_input = input().strip().lower()
            if user_input == "cancel":
                cancel_requested["flag"] = True
                break

    threading.Thread(target=listen_for_cancel, daemon=True).start()

        # ---------- Step 4: Monitor timeout & auto-stop ----------
    while True:
        now = time.time()
        all_done = True

        for acc in keys_dict.keys():
            if not final_summary[acc]["done"]:
                all_done = False

                # user cancel
                if cancel_requested["flag"]:
                    print(f"[{acc}] ⛔ User requested cancel. Cancelling all 3 limits + closing positions...")

                    # Cancel all 3 limit orders
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
                                    reduceOnly=True
                                )
                                print(f"[{acc}] 🛑 Closed {size} {side} position at market.")
                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error closing position: {e}")

                    final_summary[acc]["user_cancel"] = True
                    final_summary[acc]["done"] = True
                    print(f"[{acc}] 🛑 Trading cycle ended by user cancel.")

                # timeout
                elif final_summary[acc]["filled"] is None and now - order_timestamps[acc] > max_wait_seconds:
                    print(f"[{acc}] ⏱ Timeout reached. Cancelling all 3 limits + closing positions...")

                    # Cancel all 3 limit orders
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
                                    reduceOnly=True
                                )
                                print(f"[{acc}] 🛑 Closed {size} {side} position at market (timeout).")
                    except Exception as e:
                        print(f"[{acc}] ⚠️ Error closing position on timeout: {e}")

                    final_summary[acc]["timeout"] = True
                    final_summary[acc]["done"] = True
                    print(f"[{acc}] 🛑 Trading cycle ended by timeout.")

        if all_done or cancel_requested["flag"]:
            print("🏁 All accounts complete. Closing WebSockets.")
            for ws in ws_sessions.values():
                try:
                    ws.exit()
                except:
                    pass
            break

        time.sleep(1)

