import json
import socketserver

class MBRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        print(self.request)
        self.data = self.request.recv(128000).strip()
        try:
            data = str(self.data.decode("utf-8"))
            jsonData = json.loads(data)
                       
            jsonResponse = json.dumps(jsonData)

            self.request.sendall(jsonResponse.encode())
        except Exception as exception:
            jsonResponse = json.dumps({ "code": 500, "data": ("MBRequestHandler error") })
            self.request.sendall(jsonResponse.encode())