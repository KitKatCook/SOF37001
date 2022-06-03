from Client import Client

class Consumer(Client):
      def __init__(self, args):
            Client.__init__(self, args)
            print("I am a Consumer")