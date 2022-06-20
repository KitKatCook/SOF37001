from Broker import Broker
from ClientSender import *
from ClientSetupConfig import *
import asyncio
import sys, os
from threading import Thread

from MBMessage import MBMessage
from MBRepository import *
from Topic import *

class Producer():
      Repositity: MBRepository

      def __init__(self):
            self.Repositity = MBRepository()
            self.Create()

      def Create(self):
            brokerPort = self.GetBrokerPort()
            topics = self.GetTopics()
            index = 1
            for info in topics:
                  print(str(index) +": " + info.Name)
                  index += 1
            
            topicInput = int(input("Please select a topic. \n")) -1

            topicIndex = (topicInput)
            topic = topics[topicIndex]
            topicId = topic.Id

            messageInput = input("Please enter a message. \n")
            
            self.SendMessage(topicId, messageInput,brokerPort)

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

            return topics

      def GetBrokerPort(self):
            brokerData = self.Repositity.GetAllBroker()
            return brokerData[0][0]
            

      def SendMessage(self, topicId, message, brokerPort):
            Thread(target=asyncio.run, args=(self.CreateMessage(topicId, message, brokerPort),)).start()

      async def CreateMessage(self, topicId, message, brokerPort):
            try:
                  clientSender = ClientSender(localAddress, 2700)
                  response = await clientSender.Send({
                        "topicId": topicId,
                        "message": message
                        })  
                  return response
            except Exception as e:
                  print(e)