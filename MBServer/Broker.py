import asyncio
import json
import socketserver
from uuid import UUID, uuid3
import MBRequestHandler
from Partition import Partition
from PortFactory import PortCheckerFactory
from Topic import Topic
from socketserver import TCPServer


from ServerSetupConfig import *
from threading import Thread
from socketserver import TCPServer
import http.server

class Broker:
    Id: UUID
    Topics: list[Topic]
    TCPServer: TCPServer 
    Port:int
    WorkerThread: Thread
    PortCheckerFactory: PortCheckerFactory

    def __init__(self, id):
        self.Id = id
        self.PortCheckerFactory = PortCheckerFactory()

        self.Port = self.PortCheckerFactory.GetNextPort()
        WorkerThread = Thread(target=asyncio.run, args=(self.StartServer(),))
        WorkerThread.start()
        
    async def StartServer(self):
        print("Broker starting...")
        with socketserver.TCPServer(("", self.Port), MBRequestHandler) as self.Server:
            print("serving at port", self.Port)
            await self.Server.serve_forever()

    def AddMessage(self, messageData):
        topicId = UUID(messageData['topicId'])
        partitionId = UUID(messageData['partitionId'])
        body = messageData['message']
        topic: Topic = self.GetTopicById(topicId)
        partition: Partition = topic.GetPartitionById(partitionId)
        partition.AddMessage(body)

        return True
   
    def GetMessages(self, topicId: UUID, groupId: UUID):
        aggregate_messages = []
        topic: Topic = self.GetTopicById(topicId)
        for partition in topic.Partitions:
            offset = self.__get_consumer_group_offsets(partition.Id, groupId)
            partition_size = partition.size()
            messages = partition.get_messages(offset, partition_size)
            aggregate_messages = aggregate_messages + messages
            self.__set_consumer_group_offset(partition.id, groupId, partition_size)
        return aggregate_messages


    def AddTopic(self, topicName):
        topicId = uuid3()
        topic: Topic = Topic(topicId, topicName) 
        self.Topics.append(topic)
        return self.GetTopicById(topicId)

    def AddPartition(self, topicId):
        topic: Topic = self.GetTopicById(topicId)
        if topic is None:
            raise("Topic not found")
        topic.add_partition(topicId)
        return True

    def GetTopicById(self, id):
        topic = [topic for topic in self.Topics if topic.Id == id] 
        if(topic.count > 0):
            return topic[0]
        return None

        
    def __get_consumer_group_offsets(self, partition_id, consumer_group_id):
        sender: Sender = Sender(WARDEN_ADDRESS, WARDEN_PORT)
        response = sender.send(Message(GET_CONSUMER_GROUP_OFFSET, {
                "partition_id": str(partition_id),
                "broker_id": str(self.__broker.id),
                "consumer_group_id": str(consumer_group_id),
            }))
        return int(response['offset'])
        
    def __set_consumer_group_offset(self, partition_id, consumer_group_id, offset):
        sender: Sender = Sender(WARDEN_ADDRESS, WARDEN_PORT)
        return sender.send(Message(SET_CONSUMER_GROUP_OFFSET, {
                "partition_id": str(partition_id),
                "broker_id": str(self.__broker.id),
                "consumer_group_id": str(consumer_group_id),
                "offset": offset,
            }))