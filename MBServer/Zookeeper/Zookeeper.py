import uuid

from MBServer.Topic.Topic import Topic


class Zookeeper:
    Id: uuid
    Topics: list[Topic]
    RRTopicIndex: int

    def __init__(self):
        self.setDefaultRRTopicIndex(self)

    def addTopic(self, topic: Topic):
        self.Topics.append(topic)
        
        if self.RRTopicIndex == 0:
            self.RRTopicIndex = 1

    def getNextTopic(self):
        if (self.RRTopicIndex + 1) <= self.Topics.count:
            self.RRTopicIndex += 1
        else:
            self.RRTopicIndex = 1

        return self.Topics[self.RRTopicIndex]

    def setDefaultRRTopicIndex(self):
        self.RRTopicIndex = 0

    def addTopic(self, noPartitions = None):
        self.Topics.append(Topic(noPartitions))