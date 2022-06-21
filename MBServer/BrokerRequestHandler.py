import json
import socketserver

class BrokerRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(128000).strip()
        try:
            data = str(self.data.decode("utf-8"))
            jsonData = json.loads(data)

            if jsonData["command"] == "pull":
                self.messages = self.server.Broker.GetMessages(jsonData["topicId"], jsonData["groupId"] )
                jsonResponse = json.dumps({ "messages": self.messages })       
                self.request.sendall(jsonResponse.encode())
            else:
                self.server.Broker.AddMessage(jsonData)
                jsonResponse = json.dumps({ "code": 200, "message": ("Message Added") })       
                self.request.sendall(jsonResponse.encode())
            
        except Exception as exception:
            jsonResponse = json.dumps({ "code": 500, "message": ("BrokerRequestHandler error") })
            self.request.sendall(jsonResponse.encode())