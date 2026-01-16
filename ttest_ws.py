#Import Modules 
import os
import websocket
import threading
from datetime import datetime, timedelta

WS_API_URL = "wss://advanced-trade-ws.coinbase.com"

def start_websocket():
    ws = websocket.WebSocketApp(WS_API_URL , 
                                on_open = on_open , 
                                on_message = on_message
                                on_error = on_error
    )
    ws.run_forever



def main():
    #Statrting with a seperate Thread 
    ws_thread = threading.Thread(target=start_websocket)
    ws_thread.start()










if __name__ == "__main__":
    main()