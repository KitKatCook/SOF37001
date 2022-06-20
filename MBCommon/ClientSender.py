import json
import socket
import string

class ClientSender():
    Address: string
    Port: int
    
    def __init__(self, address, port):
        self.Address = address
        self.Port = port

    def Send(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mbsocket:
                mbsocket.connect((self.Address, self.Port))
                jsonData = json.dumps(message, default=lambda o: o.__dict__, sort_keys=True, indent=4).encode()
                print(jsonData)
                mbsocket.sendall(jsonData)
                data = mbsocket.recv(1024)
                print(data)
                responseString = str(data.decode('UTF-8'))
                response = json.loads(responseString)
                if response['status'] == 200:
                    return response['data']
                else:
                    raise Exception(response['error'])

