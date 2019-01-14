# import websocket
# import time
#
#
# class OCPPWebsocketReceiver:
#     def __init__(self):
#         self.counter = 0
#         self.ws = websocket.WebSocketApp("ws://192.168.10.1:8000/data/",
#                                          on_message=self.on_message,
#                                          on_error=self.on_error,
#                                          on_close=self.on_close)
#         self.ws.on_open = self.on_open
#         print('Initialized OCPP ws')
#         self.ws.run_forever()
#
#     def on_message(self, ws, message):
#         print('We got a message!', message)
#         time.sleep(0.5)
#         self.ws.send(str(self.counter))
#         self.counter += 1
#
#     def on_open(self, ws):
#         print('OCPP Websocket Receiver open')
#         # self.ws.send("hello from surface")
#         # time.sleep(1)
#
#     def on_close(self, ws):
#         print('OCPP Websocket Receiver closed')
#
#     def on_error(self, ws, error):
#         """ Errors here will trigger the stopped event which will prompt update_firebase to try to reconnect to WS """
#         print('Got an error in ws!!', error)
#
#
# def main():
#     test = OCPPWebsocketReceiver()
#
#
# if __name__ == '__main__':
#     main()
import psutil

addrs = psutil.net_if_addrs()
print(addrs.keys())
