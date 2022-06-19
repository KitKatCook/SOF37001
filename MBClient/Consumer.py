from ClientSetupConfig import *
from ClientSender import ClientSender
from MBMessage import MBMessage
from Topic import Topic
import asyncio

class Consumer():
      Topics: Topic
      BrokerPort: int

      def __init__(self, brokerPort):
            print("I am a Consumer")
            self.Create(self)

      def sendMessage(self, message):
            return message

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
        return self.Topics

      async def ListenOnTopic(self, topicId):
        topic_brokers = [x for x in self.__cluster_info if x["Id"] == topicId]
        while(True):
            for topic_broker in topic_brokers:
                clientSender = ClientSender(localAddress, self.BrokerPort)
                response = clientSender.send(MBMessage(GET_MEESAGES, {
                    "id": topic_id,
                    "consumer_group_id": self.__consumer_group_id
                }))
                if (len(response['messages']) > 0):
                    print(self.__consumer_group_name)
                    print(len(response['messages']))
            await asyncio.sleep(1)