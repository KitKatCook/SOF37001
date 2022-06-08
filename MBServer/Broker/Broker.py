import json
from uuid import UUID

class Broker:
    def __init__(self):
        pass

    def AddMessage(self, messageData):
        topicId = UUID(messageData['topicId'])
        partitionId = UUID(messageData['partitionId'])
        body = messageData['message']
        return {
            'isDone': self.__service.add_message(topicId, partitionId, body)
        }

    def GetMessages(self, messageData):
        messageId = UUID(messageData['Id'])
        groupId = UUID(messageData['groupId'])
        return {
            'messages': self.__service.get_messages(messageId, groupId)
        }

    def AddTopic(self, messageData):
        topicId = UUID(messageData['Id'])
        topicName = messageData['name']
        return {
            'isDone': self.__service.add_topic(topicId, topicName)
        }
    
    def AddPartition(self, messageData):
        partitionId = UUID(messageData['Id'])
        topicId = UUID(messageData['topicId'])
        leader = bool(messageData['leader'])
        return {
            'isDone': self.__service.add_partition(topicId, partitionId, leader)
        }