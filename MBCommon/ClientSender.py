import json
import socket
import string

## Sender class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class creates socket connections.
class ClientSender():
    Address: string
    Port: int
    
    ## __init__ method.
    #  @param self The class pointer.
    #  @param address The address to connect to.
    #  @param port The port to connect to.
    def __init__(self, address, port):
        self.Address = address
        self.Port = port

    ## Send message method.
    #  @param self The class pointer.
    #  @param message The message to send.
    #  @return The response from the server {code: "", data: ""}.
    #  @details This method will send a message to the address and port of a server and return the response.
    def Send(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mbsocket:
                mbsocket.connect((self.Address, self.Port))
                jsonData = json.dumps(message, default=lambda o: o.__dict__, sort_keys=True, indent=4).encode()
                print(jsonData)
                mbsocket.sendall(jsonData)
                data = mbsocket.recv(2048)
                #print(data)
                responseString = str(data.decode('UTF-8'))
                if len(responseString) > 0: 
                    response = json.loads(responseString)
                    if response['code'] == 200:
                        return response
                    else:
                        raise Exception(response['message'])
                else:
                    print("No message received.")
                mbsocket.close()

    ## Send message method.
    #  @param self The class pointer.
    #  @param message The message to send.
    #  @return The response from the server {code: "", data: ""}.
    #  @details This method will send a message to the address and port of a server and return the response.
    async def SendAsync(self, message):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as mbsocket:
            mbsocket.connect((self.Address, self.Port))
            jsonData = json.dumps(message, default=lambda o: o.__dict__, sort_keys=True, indent=4).encode()
            print(jsonData)
            mbsocket.sendall(jsonData)
            data = mbsocket.recv(1024)
            #print(data)
            mbsocket.close()

