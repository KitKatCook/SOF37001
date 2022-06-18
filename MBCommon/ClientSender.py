import json
import socket
import string

class ClientSender():
    Address: string
    Port: int
    
    def __init__(self, address, port):
        self.Address = address
        self.Port = port

    def send(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.address, self.port))
                json_data = self.ToJSON(self.ToJSON(message))
                s.sendall(json_data)
                data = s.recv(1024)
                json_data = str(data.decode('UTF-8'))
                response_object = json.loads(json_data)
                if response_object['status_code'] == 200:
                    return response_object['data']
                else:
                    raise Exception(response_object['error_message'])
    
    async def send_async(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.address, self.port))
                json_data = self.ToJSON(self.ToJSON(message))
                s.sendall(json_data)
                data = s.recv(1024)
                json_data = str(data.decode('UTF-8'))
                response_object = json.loads(json_data)

    def ToJSON(message):
        return message.toJSON().encode()
