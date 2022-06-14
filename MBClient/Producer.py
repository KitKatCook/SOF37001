from Client import Client
from ClientSetupConfig import *
import asyncio
import sys, os
from threading import Thread

class Producer():
      def __init__(self):
            self.__cluster_info = self.__get_cluster_info()


      def Create(self):
            topics = self.GetTopics()
            index = 1
            for info in topics:
                  print(f'{index}. {info["topic"]}')
                  index += 1
            
            topicInput = int(input("Please select a topic. \n"))

            topicIndex = (topicInput - 1)
            topic = topics[topicIndex]
            topicId = topic.Id

            messageInput = input("Please enter a message. \n")
            
            self.publish(topicId, messageInput)

      def GetTopics(self):
            topics = [x["topicId"] for x in self.__cluster_info]
            unique_topics = []
            for topic in list(set(topics)):
                  unique_topics.append([x for x in self.__cluster_info if x["topic_id"] == topic][0])
            return unique_topics

      def SendMessage():
            pass


      def __get_cluster_info(self):
            sender: Sender = Sender(CLUSTER_ADDRESS, CLUSTER_WARDEN_PORT, BUFFER_SIZE)
            return sender.send(Message(GET_CLUSTER_INFO, {}))



      def publish(self, topic_id, topic_message, n):
            cluster_info = self.__get_cluster_info()
            topic_brokers = [x for x in cluster_info if x["topic_id"] == topic_id]
            #TODO: implement weighted round robin by requesting partition sizes
            if len(topic_brokers) > n:
                  pass
            else:
                  batch_size = n // len(topic_brokers)
                  batch_size_r = n % len(topic_brokers)
                  i = 0
                  for topic_broker in topic_brokers:
                        size_n = batch_size
                  if i == (len(topic_brokers) - 1):
                        size_n = batch_size + batch_size_r
                        i += 1
                        print(size_n)
                        for x in range(0, size_n):
                              Thread(target=asyncio.run, args=(self.__add_message(topic_broker['topic_id'], topic_broker['partition_id'], topic_broker, topic_message),)).start()

      async def __add_message(self, topic_id, partition_id, broker, topic_message):
            try:
                  sender: Sender = Sender(broker['broker_address'], int(broker['broker_port']), BUFFER_SIZE)
                  response = await sender.send_async( Message(ADD_MEESAGE, {
                        "topic_id": topic_id,
                        "partition_id": partition_id,
                        "message": topic_message
                  }))
                  return response
            except Exception as e:
                  print(e)