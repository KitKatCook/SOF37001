import uuid

from MBServer.Partition.Partition import Partition

class Topic:
    Id: uuid
    Partitions: list

    def __init__(self, noPartitions = 1):
        self.Id = uuid.uuid4()

    def getId(self):
        return self.Id

    def createPartitions(self):
        for parition in range(self.noPartitions):
            self.Partitions.append(Partition(self.Id))
