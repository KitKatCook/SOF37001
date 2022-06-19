import http.server
import socketserver
from uuid import uuid4

from Consumer import Consumer
import ServerSetupConfig
from Broker import Broker
from ServerSetupConfig import *
from threading import Thread
import asyncio
from socketserver import TCPServer


class Zookeeper:
    Port: int = ServerSetupConfig.port
    ServerThread: Thread
    Server: TCPServer
    Brokers: list[Broker]
    Consumers: list[Consumer]

    def __init__(self, args = None):
        self.args = args
        self.Brokers = []
        self.Consumers = []
        thread = Thread(target=asyncio.run, args=(self.StartServer(),))
        thread.start()
        self.PrintMenu()

    async def StartServer(self):
        print("Server starting...")
        with socketserver.TCPServer(("", self.Port), http.server.SimpleHTTPRequestHandler) as self.Server:
            print("serving at port", self.Port)
            await self.Server.serve_forever()

    def StopServer(self):
        print("Server stopping...")
        self.Server.shutdown
        print("Server stopped.")
        

    def PrintMenu(self):
        print("\nPlease select one of the following options:")
        print("1. Start the broker")
        print("2. Add topic")
        print("3. Start Consumer")
        print("4. List Topics")
        print("9. Stop")
        user_selection = input()
        self.MenuSelection(user_selection)

    def MenuSelection(self, userInput):
        match userInput:
            case "1":
                self.StartBroker()
            case "2":
                self.AddTopic()
            case "3":
                self.AddConsumer(self)
            case "4":
                self.ListTopics(self)
            case "9":
                self.__stop
            case _:
                print("Incorrect choice please try again...")
                self.PrintMenu()

        self.PrintMenu()

    def StartBroker(self):
        print("Broker starting\n")
        id = uuid4()
        broker = Broker(id)
        self.Brokers.append(broker)
        print("Broker registerd.\n")

    def Stop(self):
        self.Brokers = None
        self.StopServer()

    def AddTopic(self):
        print("Adding Topic.")
        brokers = self.Brokers()
        if len(brokers) < 1:
            print("No brokers available, please add one from the menu.")
            return
        print("Please enter the topic name: ")
        topicNameInput = input()
    
        if len(topicNameInput) < 1:
            print("You must enter a name for the topic, please try again.")
            return 

        topic = self.Brokers[0].AddTopic(topicNameInput)
        if topic is None:
            print("Error addoing topic to brokers")
        else:
            print("Topic " + topicNameInput + " added")

    def ListBrokers(self):
        for broker in self.Brokers:
            print(broker)

    def ListTopics(self):
        for broker in self.Brokers:
            for topic in broker.Topics:
                print(topic)

    def AddConsumer(self):
        print("Adding Consumer.\n")
        groupNameInput = input("Please enter consumer group name:")
        newConsumer = Consumer(ServerSetupConfig.localAddress, self.Port, groupNameInput)
        self.Consumers.append(newConsumer)
        topics = newConsumer.GetTopics()
        
        topicIndex = 1

        for topic in topics:
            print(topicIndex + ": " + topic)
            topicIndex += 1

        print("\nPlease select a topic.")
        topicSelectionInput = self.GetMenuInput(self) - 1;
        
        selectedTopic = topics[topicSelectionInput]

        consumer_thread = Thread(target=asyncio.run, args=(newConsumer.ListenOnTopic(selectedTopic["Id"]),))
        consumer_thread.start()
        print("Consumer created.\n")

    def GetMenuInput(self):
        user_input = input()
        try:
            return int(user_input)
        except ValueError:
            print("Incorrect input! Please try again \n")
            return self.GetMenuInput()


