import json
from uuid import UUID, uuid3
from MBServer.Partition.Partition import Partition
from MBServer.PortFactory import PortCheckerFactory

from MBServer.Topic.Topic import Topic
from socketserver import TCPServer

class Broker:
    Id: UUID
    Topics: list[Topic]
    TCPServer: TCPServer 
    Port:int

    def __init__(self):
        self.port = PortCheckerFactory.GetNextPort()

    def AddMessage(self, messageData):
        topicId = UUID(messageData['topicId'])
        partitionId = UUID(messageData['partitionId'])
        body = messageData['message']
        topic: Topic = self.GetTopicById(topicId)
        partition: Partition = topic.GetPartitionById(partitionId)
        partition.AddMessage(body)

        return True

    def GetMessages(self, messageData):
        messageId = UUID(messageData['Id'])
        groupId = UUID(messageData['groupId'])
        return {
            'messages': self.__service.get_messages(messageId, groupId)
        }

    def get_messages(self, id, consumer_group_id):
        return self.__get_topic_messages(id, consumer_group_id)
    
    def AddTopic(self, topicName):
        topicId = uuid3()
        topic: Topic = Topic(topicId, topicName) 
        self.Topics.append(topic)
        return self.GetTopicById(topicId)

    def AddPartition(self, topicId):
        topic: Topic = self.GetTopicById(topicId)
        if topic is None:
            raise("Topic not found")
        topic.add_partition(topicId)
        return True

    def GetTopicById(self, id):
        topic = [topic for topic in self.Topics if topic.Id == id] 
        if(topic.count > 0):
            return topic[0]
        return None

        
    def __get_topic_messages(self, id: UUID, consumer_group_id):
        aggregate_messages = []
        topic: Topic = self.__broker.get_topic(id)
        for partition in topic.partitions:
            offset = self.__get_consumer_group_offsets(partition.id, consumer_group_id)
            partition_size = partition.size()
            messages = partition.get_messages(offset, partition_size)
            aggregate_messages = aggregate_messages + messages
            self.__set_consumer_group_offset(partition.id, consumer_group_id, partition_size)
        return aggregate_messages

    def __get_consumer_group_offsets(self, partition_id, consumer_group_id):
        sender: Sender = Sender(WARDEN_ADDRESS, WARDEN_PORT)
        response = sender.send(Message(GET_CONSUMER_GROUP_OFFSET, {
                "partition_id": str(partition_id),
                "broker_id": str(self.__broker.id),
                "consumer_group_id": str(consumer_group_id),
            }))
        return int(response['offset'])
        
    def __set_consumer_group_offset(self, partition_id, consumer_group_id, offset):
        sender: Sender = Sender(WARDEN_ADDRESS, WARDEN_PORT)
        return sender.send(Message(SET_CONSUMER_GROUP_OFFSET, {
                "partition_id": str(partition_id),
                "broker_id": str(self.__broker.id),
                "consumer_group_id": str(consumer_group_id),
                "offset": offset,
            }))