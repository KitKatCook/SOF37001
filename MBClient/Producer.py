from Broker import Broker
from ClientSender import *
from ClientSetupConfig import *
import asyncio
from threading import Thread

from MBRepository import *
from Topic import *

## Producer class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class contains data and functionality for a Producer.
class Producer():
      Repositity: MBRepository

      ## __init__ method.
      #  @param self The class pointer.
      #  @param test A Test flag .
      def __init__(self, test = None):
            self.Repositity = MBRepository()
            self.Create(test)

      ## Create producer method.
      #  @param self The class pointer.
      #  @param test A Test flag .
      #  @details Runs the setup steps needed to create a producer, including console commands and sending of a message.
      def Create(self, test):
            brokerPort = self.GetBrokerPort()
            topics = self.GetTopics()
            index = 1
            
            if test is None:
                  for t in topics:
                        print(str(index) +": " + t.Name)
                        index += 1
                  topicInput = int(input("Please select a topic. \n")) -1
                  topicIndex = (topicInput)
                  topic = topics[topicIndex]
            else:
                  topic = topics[0]

            topicId = topic.Id

            if test is None:
                  messageInput = input("Please enter a message. \n")
                  
                  self.SendMessage(topicId, messageInput,brokerPort)

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

      ## GetBrokerPort method.
      #  @param self The class pointer.
      #  @details Gets the broker from the repository.
      #  @return A broker entity.
      def GetBrokerPort(self):
            brokerData = self.Repositity.GetAllBroker()
            return brokerData[0][0]
            
      ## SendMessage method.
      #  @ListenOnTopic self The class pointer.
      #  @param topicId the topicId the consumer has subscribed too.
      #  @param callback an optional parameter for if a callback is needed after the consumer recieves a message.
      #  @details Sends a message on a new thread.
      def SendMessage(self, topicId, message, brokerPort):
            Thread(target=asyncio.run, args=(self.CreateMessage(topicId, message, brokerPort),)).start()

      ## CreateMessage method.
      #  @ListenOnTopic self The class pointer.
      #  @param topicId the topicId the producer has subscribed too.
      #  @param message the message to send.
      #  @param brokerPort the port of the broker.
      #  @details creates a ClientSender and sends the message.
      async def CreateMessage(self, topicId, message, brokerPort):
            try:
                  clientSender =  ClientSender(localAddress, 8001)
                  response = await clientSender.SendAsync({
                        "topicId": str(topicId),
                        "message": message
                        })  
                  return response
            except Exception as e:
                  print(e)