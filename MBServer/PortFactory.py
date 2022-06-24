from ServerSetupConfig import *

from http.server import BaseHTTPRequestHandler
from socketserver import TCPServer

## PortCheckerFactory class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class getting available ports for clients.
class PortCheckerFactory():
    PortsInUse = []

    ## GetNextPort method.
    #  @param self The class pointer.
    #  @details gets the next unused port.
    def GetNextPort(self):
        for p in range(port+1, (port + 1000)):
            try:
                with TCPServer(('', p), BaseHTTPRequestHandler) as s:
                    s.server_close()
                if p in PortCheckerFactory.PortsInUse:
                    raise OSError()
                else:
                    self.PortsInUse.append(port)
                return p
            except OSError: 
                pass
            
            raise OSError("Port {} is already in use.".format(p))
        