from MBClient.Consumer import Consumer
from MBClient.Producer import Producer
from MBCommon.Error.Error import Error
import Client
from MBCommon.Error.ErrorType import ErrorType
from MBCommon.Client.ClientType import ClientType

class ClientFactory:
    client: Client

    def __init__(self):

        if self.clientType == ClientType.Producer:
            producer = Producer(self.client)
            producer.Create()
            return producer
        elif self.clientType == ClientType.Consumer:
            consumer = Consumer(self.client)
            consumer.Create()
            return consumer
        else:
            Error(ErrorType.Error.name, "Cannot create client type")
