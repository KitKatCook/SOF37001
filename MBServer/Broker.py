import asyncio
from uuid import UUID, uuid4
from BrokerRequestHandler import BrokerRequestHandler
from MBRepository import MBRepository
from Partition import Partition
from PortFactory import PortCheckerFactory
from Topic import Topic
from ServerSetupConfig import *
from threading import Thread
from socketserver import TCPServer

## Broker class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class contains data and functionality for a Broker.
class Broker:
    Id: UUID
    Topics: list[Topic]
    Port:int
    WorkerThread: Thread
    PortCheckerFactory: PortCheckerFactory
    TCPserver: TCPServer
    Repositity: MBRepository
    
    ## __init__ method.
    #  @param self The class pointer.
    #  @param id Identifier of the broker.
    def __init__(self, id):
        self.Id = id
        self.PortCheckerFactory = PortCheckerFactory()
        self.Topics = []
        self.Repositity = MBRepository()
        self.Port = 8001 
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

    ## AddTopic method.
    #  @param self The class pointer.
    #  @param messageData date tuple containing topicId and message string.
    #  @details Adds a message to the topics partition.
    #  @return True.
    def AddMessage(self, messageData):
        topicId = str(UUID(messageData['topicId']))
        body = messageData['message']
        topic = [x for x in self.Topics if str(x.Id) == topicId][0]
        for partition in topic.Partitions:
            partition.AddMessage(body)
        
        return True
   
    ## AddTopic method.
    #  @param self The class pointer.
    #  @param topicId The id of the topic.
    #  @param groupId The id of the user group.
    #  @details Initialises a new topic .
    #  @return A collection of messages.
    def GetMessages(self, topicId: UUID, groupId: UUID):
        messages = []
        
        topic = [x for x in self.Topics if str(x.Id) == topicId][0]
        groupOffsets = self.Repositity.GetGroupOffset(groupId)

        for partition in topic.Partitions:
            topicOffsets = [x for x in groupOffsets if x[2] == groupId]
            for offset in topicOffsets:
                partition.Offset[offset[2]] = offset[3]

            messages = self.GetGroupOffset(topicId, groupId)
        return messages

    ## AddTopic method.
    #  @param self The class pointer.
    #  @param topicName The name of the topic.
    #  @details Initialises a new topic .
    #  @return A topics.
    def AddTopic(self, topicName):
        topicId = uuid4()
        topic: Topic = Topic(topicId, topicName) 
        self.Topics.append(topic)
        return topic

    ## GetTopics method.
    #  @param self The class pointer.
    #  @details Gets all the persisted topics and their partitions from the repository and builds the data structure.
    #  @return A collection of topics.
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
    
    
    ## SetGroupOffset method.
    #  @param self The class pointer.
    #  @param topicId the topic Id the consumer is subscribed to.
    #  @param groupId the groupId to set the offset of.
    #  @param offset the offset value to set.
    #  @details sets the offset for a topic user group.
    #  @return the offset value.
    def SetGroupOffset(self, topicId, groupId, offset):
        topic = [x for x in self.Topics if x.Id == topicId][0]
        partition = topic.Partitions[0]
        offset = partition.SetOffset(groupId, offset)
        return offset
        
    ## GetGroupOffset method.
    #  @param self The class pointer.
    #  @param topicId the topic Id the consumer is subscribed to.
    #  @param groupId the groupId to set the offset of.
    #  @details gets the offset for a topic user group.
    #  @return messages from an offset.
    def GetGroupOffset(self, topicId, groupId):
        topic = [x for x in self.Topics if str(x.Id) == topicId][0]
        partition = topic.Partitions[0]
        messages = partition.GetMessages(groupId)
        return messages


        