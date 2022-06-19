
from socketserver import TCPServer
from threading import Thread
import asyncio
from email.policy import default
from http.client import UnknownProtocol
import json
import socketserver

class MBRequestHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(1024).strip()
        try:
            data = str(self.data.decode("utf-8"))
            
            jsonData = json.loads(data)
           
            #response = URIIdentifier.invoke_function(self.server.reflection_class, message["message_type"], message["body"])
            
            jsonResponse = self.FormatRespose(self, 200, jsonData)

            self.request.sendall(jsonResponse.encode())
        except Exception as exception:
            jsonResponse = self.FormatErrorRespose(self,500,"MBRequestHandler error")
            self.request.sendall(jsonResponse.encode())


    def FormatRespose(self, code, data):
        return json.dumps({"status_code": code,"data": data})

    def FormatErrorRespose(self, code, data):
        return json.dumps({"status_code": code,"error": data})