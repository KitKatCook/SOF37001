import string
import uuid

from MBServer.Partition.Partition import Partition

class Topic:
    Id: uuid
    Name: string
    Partitions: list

    def __init__(self, name, noPartitions = 1):
        self.Id = uuid.uuid4()
        self.Name = name

    def getId(self):
        return self.Id

    def createPartitions(self):
        for parition in range(self.noPartitions):
            self.Partitions.append(Partition(self.Id))


    def PrintSelf(self, log = False):
        if log:
            print("[Topic] Id: {self.Id} - Name: {self.name}")
        else:
            print("[Topic] Name: {self.name}")
