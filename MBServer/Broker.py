import asyncio
import json
from uuid import UUID, uuid3, uuid4
import uuid
from BrokerRequestHandler import BrokerRequestHandler
from MBRepository import MBRepository
from Partition import Partition
from PortFactory import PortCheckerFactory
from Topic import Topic
from ServerSetupConfig import *
from threading import Thread
from socketserver import TCPServer

class Broker:
    Id: UUID
    Topics: list[Topic]
    Port:int
    WorkerThread: Thread
    PortCheckerFactory: PortCheckerFactory
    TCPserver: TCPServer
    Repositity: MBRepository
    
    def __init__(self, id):
        self.Id = id
        self.PortCheckerFactory = PortCheckerFactory()
        self.Topics = []
        self.Repositity = MBRepository()
        self.Port = 8001 #self.PortCheckerFactory.GetNextPort()
        WorkerThread = Thread(target=asyncio.run, args=(self.StartServer(),))
        WorkerThread.start()
        
    async def StartServer(self):
        print("Broker starting...")
        with TCPServer(("", self.Port), BrokerRequestHandler) as self.TCPserver:
            self.TCPserver.Broker = self
            try:
                print("serving at port", self.Port)
                await self.TCPserver.serve_forever()
            except Exception as e:
                print(e)

    def AddMessage(self, messageData):
        topicId = str(UUID(messageData['topicId']))
        body = messageData['message']
        topic = [x for x in self.Topics if str(x.Id) == topicId][0]
        for partition in topic.Partitions:
            partition.AddMessage(body)
        
        return True
   
    def GetMessages(self, topicId: UUID, groupId: UUID):
        messages = []
        topic = [x for x in self.Topics if str(x.Id) == topicId][0]
        
        groupOffsets = self.Repositity.GetGroupOffset(groupId)

        for partition in topic.Partitions:
            #messages = partition.GetMessages(groupId)

            topicOffsets = [x for x in groupOffsets if x[2] == groupId]
            for offset in topicOffsets:
                partition.Offset[offset[2]] = offset[3]

            messages = self.GetGroupOffset(topicId, groupId)
        return messages


    def AddTopic(self, topicName):
        topicId = uuid4()
        topic: Topic = Topic(topicId, topicName) 
        self.Topics.append(topic)
        return topic

    def GetTopics(self):
        topics: list[Topic] = []
        topicsData = self.Repositity.GetAllTopics()
        partitions = self.Repositity.GetAllPartitions()

        for topicData in topicsData:
                topic = Topic(topicData[0], topicData[1])
                
                topic.Partitions = []
                topicPartitions = [x for x in partitions if x[1] == topic.Id]
                for partition in topicPartitions:
                    topic.Partitions.append(Partition(partition[0],partition[1]))
                topics.append(topic)

                for partition in topic.Partitions:
                    groupOffsets = self.Repositity.GetGroupOffset(partition.Id)
                    for offset in groupOffsets:
                            partition.Offset[offset[2]] = offset[3]

        return topics
    
    def SetGroupOffset(self, topicId, groupId, offset):
        topic = [x for x in self.Topics if x.Id == topicId][0]
        partition = topic.Partitions[0]
        offset = partition.SetOffset(groupId, offset)
        return offset
        
    def GetGroupOffset(self, topicId, groupId):
        topic = [x for x in self.Topics if str(x.Id) == topicId][0]
        partition = topic.Partitions[0]
        messages = partition.GetMessages(groupId)
        return messages


        