import websocket
import threading
import signal
import sys
import json
from kraken_balances import KrakenBalances
import kraken_executions
import kraken_l2
import kraken_token
import kraken_order

ws_url = "wss://ws-auth.kraken.com/v2"
api_token = None
prompt_event = threading.Event()

myKrakenBalances = None
ws = None



def create_subscription_message_balances(token, snapshot=True):
    return {
        "method": "subscribe",
        "params": {
            "channel": "balances",
            "snapshot": snapshot,
            "token": token
        }
    }

def create_subscription_message_executions(token):
    return {
        "method": "subscribe",
        "params": {
        "channel": "executions",
        "token": token,
        "snap_orders": True,
        "snap_trades": True
        }
    }

def create_unsubscribe_message_balances(token):
    return {
        "method": "unsubscribe",
        "params": {
            "channel": "balances",
            "token": token
        }
    }


def handle_manual_order():
    orderString = input("Enter side, order_qty, symbol, type:\n")
    orderStringList = [x.strip() for x in orderString.split(',')]
    assert len(orderStringList)==4, "Order input length should be 4"
    orderSide = orderStringList[0]
    assert orderSide in ["buy", "sell"]
    orderQty = float(orderStringList[1])
    orderSymbol = orderStringList[2]
    orderType = orderStringList[3]
    if orderType == "market":
        kraken_order.market_order(ws, orderSide, orderQty, orderSymbol, api_token)
    

def signal_handler(sig, frame):
    global ws
    if ws:
        ws.close()
    sys.exit(0)

def on_message(ws, message):
    global myKrakenBalances
    message = json.loads(message)
    if message.get('channel')!='heartbeat':
        #print(message)
        pass
    if message.get('channel')=='balances' and message.get('type')=='snapshot':
        myKrakenBalances = KrakenBalances()
        myKrakenBalances.snapshot_balances(message.get('data'))
        print("Snapshot Balances")
        myKrakenBalances.print_balances()

        prompt_event.set()
        
        #unsubscribe_message_balances = create_unsubscribe_message_balances(api_token)
        #ws.send(json.dumps(unsubscribe_message_balances))
    
    elif message.get('channel')=='balances' and message.get('type')=='update':
        myKrakenBalances.update_balances(message.get('data'))
        

    elif message.get('channel')=='executions' and message.get('type')=='snapshot':
        kraken_executions.handle_executions_snapshot(message.get('data'))
        #TODO look for snapshot balances and snapshot executions before prompt_event.set()

    elif message.get('channel')=='executions' and message.get('type')=='update':
        kraken_executions.handle_executions_update(message.get('data'))

    elif message.get('method')=='add_order':
        print("Order req_id: {}; Success: {}".format(message.get('result').get('order_id'), message.get('success')))
        prompt_event.set()

def on_error(ws, error):
    print("Error:", error)

def on_close(ws, status_code, status_message):
    print(f"Connection closed with code {status_code}, message: {status_message}")


def on_open(ws):
    def run(*args):
        global api_token
        global myKrakenBalances
        api_token = kraken_token.get_websocket_token()
        subscription_message_balances = create_subscription_message_balances(api_token)
        ws.send(json.dumps(subscription_message_balances))

        subscription_message_executions = create_subscription_message_executions(api_token)
        ws.send(json.dumps(subscription_message_executions))

        while True:
            prompt_event.wait()

            action = input("Type 'order', 'balances' or 'exit':\n")
            if action == 'order':
                prompt_event.clear()
                handle_manual_order()
            elif action == 'balances':
                prompt_event.clear()
                myKrakenBalances.print_balances()
                prompt_event.set()

            elif action == 'test':
                pass
            elif action == 'exit':
                break
        ws.close()
    thread = threading.Thread(target=run)
    thread.start()

def main():
    #websocket.enableTrace(True)
    global ws
    signal.signal(signal.SIGINT, signal_handler)
    ws_thread_l2 = threading.Thread(target=kraken_l2.start_websocket, daemon=True)
    ws_thread_l2.start()
    ws = websocket.WebSocketApp(ws_url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    ws.run_forever(ping_interval = 30) #maybe consider ping_interval = 30

if __name__ == "__main__":
    main()