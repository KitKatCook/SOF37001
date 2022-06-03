from MBCommon.Client.Consumer import Consumer
from MBCommon.Client.Producer import Producer
from MBCommon.Error.Error import Error
import Client
from MBCommon.Error.ErrorType import ErrorType
from MBCommon.Client.ClientType import ClientType

class ClientFactory:
    client: Client

    def __init__(self, clientType):

        if self.clientType == ClientType.Producer:
            return Producer(self.client)
        elif self.clientType == ClientType.Consumer:
            return Consumer(self.client)
        else:
            Error(ErrorType.Error.name, "Cannot create client type")
