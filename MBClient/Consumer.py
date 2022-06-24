import string
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
      BrokerPort: int
      GroupName : string

      def __init__(self, groupName = None, test = None):
            self.Repositity = MBRepository()
            self.BrokerPort = self.GetBrokerPort()
            self.GroupName = groupName
            self.Create(test)
           

      def Create(self, test = None):
            if self.GroupName is None:
                  groupNameInput = input('Please enter a consumer group name...\n')
            else:
                 groupNameInput = self.GroupName

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
            
            if test is None:
                  selection = int(input("Please select a topic..."))
                  topic = topics[selection - 1]
            else:
                  topic = topics[0]


            self.Repositity.AddGroupOffset(uuid4(), topic.Partitions[0].Id ,self.GroupId, 0)      
            topic.Partitions[0].SetOffset(self.GroupId, 0)

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

                  for partition in topic.Partitions:
                        groupOffsets = self.Repositity.GetGroupOffset(partition.Id)
                        for offset in groupOffsets:
                              partition.Offset[offset[2]] = offset[3]

            return topics

      def GetBrokerPort(self):
            brokerData = self.Repositity.GetAllBroker()
            return brokerData[0][1]

      async def ListenOnTopic(self, topicId):
        topic = [x for x in self.GetTopics() if x.Id == topicId][0]
        while(True):
            for partition in topic.Partitions:
                clientSender = ClientSender(localAddress, self.BrokerPort)
                response = clientSender.Send({
                    "command": "pull",
                    "topicId": topicId,
                    "groupId": str(self.GroupId)
                })
                if response is not None and len(response) > 0:
                  if (len(response['message']) > 0):
                        print(response['message'])
            await asyncio.sleep(0.1)