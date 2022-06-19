
from socketserver import TCPServer
from threading import Thread
import asyncio
from email.policy import default
from http.client import UnknownProtocol
import json
import socketserver

from MBCommon.MBMessage import Message


class TCPServe():
    __port: int = 2700
    __alive_thread: Thread
    Server: TCPServer
    def __init__(self, port):
        self.__port = port
        self.__alive_thread = Thread(target=asyncio.run, args=(self.__create(),))
        self.__alive_thread.start()
    
    async def __create(self):
        with TCPServer(("", self.__port), TcpRequestHandler) as self.Server:
            try:
                await self.Server.serve_forever()
            except:
                print('Server shutting down....')

    def close(self):
        self.Server.shutdown()


class TcpRequestHandler(socketserver.BaseRequestHandler):
    __BUFFER_SIZE=1024
    def handle(self):
        self.data = self.request.recv(self.__BUFFER_SIZE).strip()
        try:
            json_data = str(self.data.decode("utf-8"))
            message : Message = json.loads(json_data)
            uri_response = URIIdentifier.invoke_function(self.server.reflection_class, message["message_type"], message["body"])
            json_response = json.dumps({
                "status_code": 200,
                "data": uri_response
            })
            self.request.sendall(json_response.encode())
        except ServiceException as ex:
            json_response = json.dumps({
                "status_code": 500,
                "error_message": ex.message
            })
            self.request.sendall(json_response.encode())
        except Exception as ex:
            json_response = json.dumps({
                "status_code": 500,
                "error_message": ""
            })
            self.request.sendall(json_response.encode())