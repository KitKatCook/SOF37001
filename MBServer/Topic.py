import string
from uuid import UUID, uuid3, uuid4

from Partition import Partition

class Topic:
    Id: UUID
    Name: string
    Partitions: list[Partition]

    def __init__(self, id, name, partitions = None):
        self.Id = id
        self.Name = name

        if partitions == None:
            self.CreatePartitions()
        else: 
            self.Partitions = partitions

    def getId(self):
        return self.Id

    def CreatePartitions(self):
        self.Partitions = []
        for parition in range(1):
            self.Partitions.append(Partition(uuid4(), self.Id))

    def PrintSelf(self, log = False):
        if log:
            print("[Topic] Id: {self.Id} - Name: {self.name}")
        else:
            print("[Topic] Name: {self.name}")

    def GetPartitionById(self, id):
        partition = [partition for partition in self.Partitions if partition.Id == id] 
        if(partition.count > 0):
            return partition[0]
        return None

    def AddPartition(self, topicId):
        partitionId = uuid4()
        partition: Partition = Partition(partitionId, topicId) 
        self.Partitions.append(partition)
        return self.GetPartitionById(topicId)

    def GetPartitionById(self, id):
        partition = [partition for partition in self.Partition if partition.Id == id] 
        if(partition.count > 0):
            return partition[0]
        return None