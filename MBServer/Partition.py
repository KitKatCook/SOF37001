import array
import string
from uuid import UUID, uuid3, uuid4

class Partition():
    Id: UUID
    TopicId: UUID
    Messages : list

    def __init__(self, partitionId, topicId):
        self.TopicId = topicId
        self.Id = partitionId
        self.Messages = []

    def AddMessage(self, body):
        self.Messages.append(body)

    def GetMessages(self, offset, amount):
        return self.Messages[offset:amount]

    def AmountOfMessages(self):
        return len(self.Messages)