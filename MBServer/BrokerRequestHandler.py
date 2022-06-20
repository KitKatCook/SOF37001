import json
import socketserver

class BrokerRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(128000).strip()
        try:
            data = str(self.data.decode("utf-8"))
            jsonData = json.loads(data)
            self.server.Broker.AddMessage(jsonData)
                       
            jsonResponse = json.dumps(jsonData)

            self.request.sendall(jsonResponse.encode())
        except Exception as exception:
            jsonResponse = json.dumps({ "code": 500, "message": ("BrokerRequestHandler error") })
            self.request.sendall(jsonResponse.encode())