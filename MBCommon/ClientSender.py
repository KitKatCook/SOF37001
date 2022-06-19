import json
import string

class ClientSender():
    Address: string
    Port: int
    
    def __init__(self, address, port):
        self.Address = address
        self.Port = port

    def Send(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as socket:
                socket.connect((self.address, self.port))
                jsonData = self.ToJSON(self.ToJSON(message))
                socket.sendall(jsonData)
                data = socket.recv(1024)
                responseString = str(data.decode('UTF-8'))
                response = json.loads(responseString)
                if response['status'] == 200:
                    return response['data']
                else:
                    raise Exception(response['error'])

    def ToJSON(message):
        return message.toJSON().encode()
