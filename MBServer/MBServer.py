import http.server
import socketserver
from uuid import uuid3
from MBServer.Broker.Broker import Broker
from ServerSetupConfig import *
from threading import Thread
import asyncio
from socketserver import TCPServer


class MBServer:
    Port: int = port
    ServerThread: Thread
    Server: TCPServer
     
    def __init__(self, args = None):
        self.args = args
        
        self.StartServer()

    def StartServer(self):
        print("Server starting...")
        with socketserver.TCPServer(("", self.Port), http.server.SimpleHTTPRequestHandler) as self.Server:
            print("serving at port", port)
            self.Server.serve_forever()
            self.PrintMenu()

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
        broker = Broker(uuid3())
        broker_service = BrokerService(broker)
        controller = BrokerController(broker_service)
        broker.assign_handler(controller)
        self.__service.add_broker(broker.id, BROKER_LOCAL_IP, broker.port)
        print("Broker registerd.\n")


    def Stop(self):
        self.__service.delete_all_brokers()
        self.StopServer()

    def __add_topic(self):
        print("---------- Adding topic -----------")
        print("-----------------------------------")
        brokers = self.__service.list_brokers()
        if len(brokers) == 0:
            print("Broker list empty")
            return
        print("Please enter topic name... \n")
        topic_name = input()
        print("Please enter number of partitions... \n")
        number_of_partitions = self.__get_number_input()
        print("Please enter number of replication (-1 for default replication)... \n")
        replication_factor = 0 #self.__get_number_input()
        #while(number_of_partitions >= len(brokers)):
        #    print("Please enter a replication factor which less than the amount of brokers... \n")
        #    replication_factor = self.__get_number_input()
    
        is_done = self.__service.add_topic(topic_name, number_of_partitions, replication_factor)
        if is_done:
            print(f"Topic added to brokers")
        else:
            print(f"Error addoing topic to brokers")
        print("-----------------------------------")

    def __list_brokers(self):
        for row in self.__service.list_brokers():
            print(row)

    def __start_consumer(self):
        print("--------- Starting Consumer ---------")
        print("-----------------------------------")
        consumer_group_name = input("Please enter consumer group name - ")
        consumer = Consumer(BROKER_LOCAL_IP, DEFAULT_PORT, consumer_group_name)
        self.__consumers.append(consumer)
        topics = consumer.get_topics()
        index = 1
        for info in topics:
            print(f"{index}. {info["topic"]}")
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
