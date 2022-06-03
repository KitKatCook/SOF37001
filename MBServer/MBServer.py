import ctypes
from HTTPServer import HTTPServer

class MBServer:
    def __init__(self, args = None):
        self.args = args
        
        self.SetConsoleTitle()
        print("Welcome!")

        HTTPServer()

    def SetConsoleTitle(self):
        ctypes.windll.kernel32.SetConsoleTitleA("Message Broker Server")

