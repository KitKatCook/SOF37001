import ctypes

class MRClient:
    def __init__(self, args = None):
        self.args = args
        
        self.SetConsoleTitle()
        print("Welcome Client!")
        self.SetClientType()

    def SetConsoleTitle(self):
        ctypes.windll.kernel32.SetConsoleTitleA("Message Broker Client")

    def SetClientType(self):
        return "consumer"

