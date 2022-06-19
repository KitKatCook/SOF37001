from ClientSetupConfig import *
from Client import Client
from MBCommon.ClientSender import ClientSender
from MBServer.Topic import Topic
import asyncio
import sys, os

class Consumer(Client):
      Topics: Topic
      
      def __init__(self, args):
            Client.__init__(self, args)
            print("I am a Consumer")

      def sendMessage(self, message):
            return message

      def Create(self):
            groupNameInput = input('Please enter a consumer group name...\n')
            consumer = Consumer(address, port, groupNameInput)
            topics = consumer.getTopics()
            i = 1
            for topic in topics:
                  topic.Print()
                  i += 1
            selection = int(input("Please select a topic..."))
            topic_broker = topics[selection - 1]
            consumer.listen_to_cluster(topic_broker['topic_id'])

      def GetTopics(self):
        return self.Topics

      async def ListenOnTopic(self, topicId):
        topic_brokers = [x for x in self.__cluster_info if x["topic_id"] == topicId]
        while(True):
            for topic_broker in topic_brokers:
                clientSender = ClientSender(topic_broker['broker_address'], int(topic_broker['broker_port']), 1024)
                response = clientSender.send(Message(GET_MEESAGES, {
                    "id": topic_id,
                    "consumer_group_id": self.__consumer_group_id
                }))
                if (len(response['messages']) > 0):
                    print(self.__consumer_group_name)
                    print(len(response['messages']))
            await asyncio.sleep(1)