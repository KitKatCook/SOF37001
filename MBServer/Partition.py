import array
import string
from uuid import UUID, uuid3, uuid4

from MBRepository import MBRepository

## Partition class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class contains data and functionality for a Partition.
class Partition():
    Id: UUID
    TopicId: UUID
    Messages : list
    Offset: dict
    Repository: MBRepository

    ## __init__ method.
    #  @param self The class pointer.
    #  @param partitionId The id of this partition.
    #  @param topicId The id of the topic this partition is part of.
    def __init__(self, partitionId, topicId):
        self.TopicId = topicId
        self.Id = partitionId
        self.Messages = []
        self.Offset = {}
        self.Repository = MBRepository()

    ## AddMessage method.
    #  @param self The class pointer.
    #  @param body the message.
    #  @details add a message to this partitions message list.
    def AddMessage(self, body):
        self.Messages.append(body)

    ## GetMessages method.
    #  @param self The class pointer.
    #  @param groupId to get the offset and messages for.
    #  @details sets the offset value for a user group.
    #  @return the messages from the group offset.
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

    ## AddMessage method.
    #  @param self The class pointer.
    #  @param groupId for the offsets user group.
    #  @param offset the value of the offset.
    #  @details sets the offset value for a user group.
    def SetOffset(self, groupId, offset):
            self.Offset[str(groupId)] = offset