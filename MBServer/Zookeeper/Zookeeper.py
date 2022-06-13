import http.server
import socketserver
from uuid import uuid3
from Broker.Broker import Broker
from ServerSetupConfig import *
from threading import Thread
import asyncio
from socketserver import TCPServer


class Zookeeper:
    Port: int = port
    ServerThread: Thread
    Server: TCPServer
    Brokers: list[Broker]

    def __init__(self, args = None):
        self.args = args
        
        thread = Thread(target=asyncio.run, args=(self.StartServer(),))
        thread.start()
        self.PrintMenu()

    async def StartServer(self):
        print("Server starting...")
        with socketserver.TCPServer(("", self.Port), http.server.SimpleHTTPRequestHandler) as self.Server:
            print("serving at port", port)
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
        print("9. Stop")
        user_selection = input()
        self.__command_factory(user_selection)

    def MenuSelection(self, userInput):
        match userInput:
            case "1":
                self.StartBroker
            case "2":
                self.__add_topic
            case "3":
                self.__start_consumer
            case "4":
                self.ListTopics
            case "9":
                self.__stop
            case _:
                print("Incorrect choice please try again...")
                self.PrintMenu()

        self.PrintMenu()

    def StartBroker(self):
        print("Broker starting\n")
        id = uuid3()
        broker = Broker(id)
        self.Brokers.append(broker)
        print("Broker registerd.\n")

    def Stop(self):
        self.__service.delete_all_brokers()
        self.StopServer()

    def AddTopic(self):
        print("Adding Topic.")
        brokers = self.Brokers()
        if len(brokers) < 1:
            print("No brokers available, please add one from the menu.")
            return
        print("Please enter the topic name... \n")
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

    def __start_consumer(self):
        print("--------- Starting Consumer ---------")
        print("-----------------------------------")
        consumer_group_name = input("Please enter consumer group name - ")
        consumer = Consumer(BROKER_LOCAL_IP, DEFAULT_PORT, consumer_group_name)
        self.__consumers.append(consumer)
        topics = consumer.get_topics()
        index = 1
        for info in topics:
           # print(f"{index}. {info["topic"]}")
            index += 1
        selection = int(input("Please select a topic..."))
        topic_broker = topics[selection - 1]
        consumer_thread = Thread(target=asyncio.run, args=(consumer.listen_to_cluster(topic_broker["topic_id"]),))
        consumer_thread.start()
        print("------- Consumer registerd  ------- \n")
        print("-----------------------------------")

    def __get_number_input(self):
        user_input = input()
        try:
            return int(user_input)
        except ValueError:
            print("Wrong input \n")
            return self.__get_number_input()
