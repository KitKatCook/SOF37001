import uuid

class Partition():
    Id: uuid
    TopicId: uuid
    Offset: int

    def __init__(self, topicId):
        self.TopicId = topicId
        self.create()

        def create():
            Id = uuid.uuid4()

        def getOffset(self):
            return self.Offset

        def incrementOffset(self):
            self.Offset += 1