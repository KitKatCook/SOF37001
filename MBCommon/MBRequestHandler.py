import json
import socketserver

## MBRequestHandler class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class handles requests made.
class MBRequestHandler(socketserver.BaseRequestHandler):
    
    ## handle method.
    #  @param self The class pointer.
    #  @details decodes json data and sends back a response.
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