from concurrent.futures import thread
import http.server
from operator import indexOf
import os
import socketserver
import time
from uuid import uuid4

from Consumer import Consumer
from MBRepository import MBRepository
from MBRequestHandler import MBRequestHandler
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
    Repository: MBRepository

    def __init__(self, startMenu = None):
        self.Brokers = []
        self.Consumers = []
        thread = Thread(target=asyncio.run, args=(self.StartServer(),))
        thread.start()
        self.CreateDBTables()
        if startMenu is not None:
            self.PrintMenu()

    async def StartServer(self):
        print("Server starting...")
        with socketserver.TCPServer(("", self.Port), MBRequestHandler) as self.Server:
            print("serving at port", self.Port)
            await self.Server.serve_forever()

    def CreateDBTables(self):
        self.Repository = MBRepository()
        self.Repository.CreateTables()

    def StopServer(self):
        print("Server stopping...")
        self.Server.shutdown
        print("Server stopped.")

    def PrintMenu(self):
        print("\nPlease select one of the following options:")
        print("1. Start the broker")
        print("2. Add topic")
        print("4. List Topics")
        print("5. List Messages")
        print("9. Stop")
        user_selection = input()
        self.MenuSelection(user_selection)

    def MenuSelection(self, userInput):
        match userInput:
            case "1":
                self.StartBroker()
                self.PrintMenu()
            case "2":
                self.AddTopic()
                self.PrintMenu()
            case "4":
                self.ListTopics()
                self.PrintMenu()
            case "5":
                self.ListMessages()
                self.PrintMenu()
            case "9":
                self.Stop()
            case _:
                print("Incorrect choice please try again...")
                self.PrintMenu()
    
        

    def StartBroker(self):
        print("Broker starting\n")
        id = uuid4()
        broker = Broker(id)
        self.Brokers.append(broker)

        self.Repository.AddBroker(broker.Id, broker.Port)

        print("Broker registerd.\n")

    def Stop(self):
        self.Brokers = None
        self.StopServer()

    def AddTopic(self):
        print("Adding Topic.")
        if len(self.Brokers) < 1:
            print("No brokers available, please add one from the menu.")
            return
        print("Please enter the topic name: ")
        topicNameInput = input()
    
        if len(topicNameInput) < 1:
            print("You must enter a name for the topic, please try again.")
            return 

        topic = self.Brokers[0].AddTopic(topicNameInput)
        if topic is None:
            print("Error adding topic to brokers")
        else:
            print("Topic " + topicNameInput + " added")
            self.Repository.AddTopic(topic.Id, topic.Name)

            for partition in topic.Partitions:

                partitionId:uuid4 = partition.Id

                self.Repository.AddPartition(partitionId, topic.Id)

    def ListBrokers(self):
        for broker in self.Brokers:
            print(broker)

    def ListTopics(self):
        for broker in self.Brokers:
            print("\nTopics:")
            for topic in broker.Topics:
                print(topic.Name)

    def ListMessages(self):
        for broker in self.Brokers:
            for topic in broker.Topics:
                for partition in topic.Partitions:
                    for message in partition.Messages:
                        print("Topic: " + topic.Name + "- Partition " + str(topic.Partitions.index(partition) + 1) +": Message - " + message)

    def GetMenuInput(self):
        user_input = input()
        try:
            return int(user_input)
        except ValueError:
            print("Incorrect input! Please try again \n")
            return self.GetMenuInput()


