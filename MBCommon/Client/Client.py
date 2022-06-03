from MBCommon.Error.Error import Error
from MBCommon.Error.ErrorType import ErrorType


class Client:
    def __init__(self, args):
        self.args = args[0]
        self.validateArguments(self)

    def validateArguments(self):
        validate = True

        if self.args[0] is None:
            "error"
        

        return validate