import ctypes
from MBClient.Client import Client
from MBClient.ClientFactory import ClientFactory
from MBCommon.Client.ClientType import ClientType

class MBClient:
    client: Client
    
    def __init__(self, args=None):
        self.args = args

        self.setConsoleTitle()
        print("Welcome Client!")
        self.inputCLientType()

    def setConsoleTitle(self):
        ctypes.windll.kernel32.SetConsoleTitleA("Message Broker Client")

    def inputCLientType(self):
        print("Are you a producer or consumer?")
        print("1: producer")
        print("2: consumer")

        clientTypeInput = input()
        vaildate = self.validateClientTypeInput(self, clientTypeInput)

        if vaildate:
            self.createClientType(self)
        else:
            self.inputCLientType(self)

    def validateClientTypeInput(self, clientTypeInput):
        if clientTypeInput != 1 or clientTypeInput != 2:
            print("Incorrect choice, please try again.")
            return False
        else:
            return True

    def createClientType(self, clientType):
        if clientType == 1:
            self.client = ClientFactory(ClientType.Producer)
        elif clientType == 1:
            self.client = ClientFactory(ClientType.Consumer)