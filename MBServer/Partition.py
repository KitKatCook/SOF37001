import array
import string
from uuid import UUID, uuid3, uuid4

class Partition():
    Id: UUID
    TopicId: UUID
    Messages : list
    Offset: dict

    def __init__(self, partitionId, topicId):
        self.TopicId = topicId
        self.Id = partitionId
        self.Messages = []
        self.Offset = {}

    def AddMessage(self, body):
        self.Messages.append(body)

    def GetMessages(self, groupId):
        messages = None
        offset = 0
        size = len(self.Messages)
        if groupId in self.Offset.keys:
            offset = self.Offset[groupId]
        messages = self.Messages[offset:size]
        self.Offset[groupId] = size
        return messages

    def AmountOfMessages(self):
        return len(self.Messages)