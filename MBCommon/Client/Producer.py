from Client import Client

class Producer(Client):
      def __init__(self, args):
            Client.__init__(self, args)
            print("I am a Producer")