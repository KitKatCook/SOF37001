import json
import string

class MBMessage:
    body: string

    def __init__(self, body):
        self.body = body

    def toJSON(self):
        return json.dumps(self.body)