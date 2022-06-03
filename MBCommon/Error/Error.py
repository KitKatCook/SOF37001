class Error:
    def __init__(self, type, message):
        self.type = type
        self.message = message
        self.raiseError(self)

    def raiseError(self):
        print(self.type + ": " + self.message)
    

            
    
