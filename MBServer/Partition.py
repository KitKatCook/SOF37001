import array
import uuid

class Partition():
    Id: uuid
    TopicId: uuid
    Messages : array

    def __init__(self, partitionId, topicId):
        self.TopicId = topicId
        self.Id = partitionId

    def AddMessage(self, body):
        self.__queue.append(body)

    def GetMessages(self, offset, amount):
        return self.Messages[offset:amount]

    def AmountOfMessages(self):
        return len(self.Messages)