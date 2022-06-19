import ClientSender
from ClientSetupConfig import *
import asyncio
import sys, os
from threading import Thread

from MBCommon.MBMessage import MBMessage
from MBRepository import *

class Producer():
      Repositity: MBRepository

      def __init__(self):
            self.Repositity = MBRepository()
            self.Create()

      def Create(self):
            topics = self.GetTopics()
            index = 1
            for info in topics:
                  print(f'{index}. {info["topic"]}')
                  index += 1
            
            topicInput = int(input("Please select a topic. \n")) -1

            topicIndex = (topicInput)
            topic = topics[topicIndex]
            topicId = topic.Id

            messageInput = input("Please enter a message. \n")
            
            self.SendMessage(topicId, messageInput)

      def GetTopics(self):
            return self.Repositity.GetAllTopics()
            

      def SendMessage(self, topicId, message):
            Thread(target=asyncio.run, args=(self.CreateMessage(topicId, message),)).start()

      async def CreateMessage(self, topicId, message):
            try:
                  clientSender = ClientSender(localAddress, port)
                  response = await clientSender.send(message)
                  
                  return response
            except Exception as e:
                  print(e)