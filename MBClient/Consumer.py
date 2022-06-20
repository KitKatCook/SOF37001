from uuid import UUID
from ClientSetupConfig import *
from ClientSender import ClientSender
from MBRepository import MBRepository
from Partition import Partition
from Topic import Topic
import asyncio

class Consumer():
      Repositity: MBRepository
      GroupId: UUID

      def __init__(self):
            self.Repositity = MBRepository()
            self.Create(self)

      def Create(self):
            groupNameInput = input('Please enter a consumer group name...\n')
            
            topics = self.getTopics()
            i = 1
            for topic in topics:
                  topic.Print()
                  i += 1
            selection = int(input("Please select a topic..."))
            topic_broker = topics[selection - 1]
            self.ListenOnTopic(topic_broker['topic_id'])

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

      async def ListenOnTopic(self, topicId):
        topic_brokers = [x for x in self.__cluster_info if x["Id"] == topicId]
        while(True):
            for topic_broker in topic_brokers:
                clientSender = ClientSender(localAddress, self.BrokerPort)
                response = clientSender.send({
                    "topicId": topicId,
                    "groupId": self.GroupId
                })
                if (len(response['messages']) > 0):
                    print(self.__consumer_group_name)
                    print(len(response['messages']))
            await asyncio.sleep(1)