from threading import Thread
from uuid import UUID, uuid4
from ClientSetupConfig import *
from ClientSender import ClientSender
from MBRepository import MBRepository
from Partition import Partition
from Topic import Topic
import asyncio

class Consumer():
      Repositity: MBRepository
      GroupId: UUID
      WorkerThread: Thread
      
      def __init__(self):
            self.Repositity = MBRepository()
            self.Create()
           

      def Create(self):
            groupNameInput = input('Please enter a consumer group name...\n')
            
            groups = self.Repositity.GetAllGroups()
            exists = True
            for group in groups:
                  if group[1] == groupNameInput:
                        exists = False 
                        self.GroupId = group[0]  

            if exists:
                  groupId = uuid4()
                  self.Repositity.AddGroup(groupId, groupNameInput)
                  self.GroupId = groupId
            
            topics = self.GetTopics()
            
            i = 1
            for t in topics:
                  print(str(i) +": " + t.Name)
                  i += 1

            selection = int(input("Please select a topic..."))
            topic = topics[selection - 1]

            self.WorkerThread = Thread(target=asyncio.run, args=(self.ListenOnTopic(topic.Id),))
            self.WorkerThread.start()

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
        topic = [x for x in self.GetTopics() if x.Id == topicId][0]
        while(True):
            for partition in topic.Partitions:
                clientSender = ClientSender(localAddress, self.BrokerPort)
                response = clientSender.Send({
                    "command": "pull",
                    "topicId": topicId,
                    "groupId": self.GroupId
                })
                if (len(response['messages']) > 0):
                    print(len(response['messages']))
            await asyncio.sleep(1)