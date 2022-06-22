import array
import string
from uuid import UUID, uuid3, uuid4

from MBRepository import MBRepository

class Partition():
    Id: UUID
    TopicId: UUID
    Messages : list
    Offset: dict
    Repository: MBRepository

    def __init__(self, partitionId, topicId):
        self.TopicId = topicId
        self.Id = partitionId
        self.Messages = []
        self.Offset = {}
        self.Repository = MBRepository()

    def AddMessage(self, body):
        self.Messages.append(body)

    def GetMessages(self, groupId):
        messages = None
        offset = 0
        size = len(self.Messages)
        if groupId in self.Offset:
            offset = self.Offset[groupId]
        messages = self.Messages[offset:size]
        self.Offset[groupId] = size
        self.Repository.SetGroupOffset(groupId, size)
        return messages

    def SetOffset(self, groupId, offset):
            self.Offset[str(groupId)] = offset