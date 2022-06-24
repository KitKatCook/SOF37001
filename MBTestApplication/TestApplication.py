import asyncio
import os
import sys
from threading import Thread
import time
import tkinter as tk
from tkinter import *
from tkinter.ttk import Notebook
from uuid import uuid4

sys.path.append(f"{os.getcwd()}/MBServer")
sys.path.append(f"{os.getcwd()}/MBClient")
sys.path.append(f"{os.getcwd()}/MBCommon")

from Broker import Broker
from Consumer import Consumer
from MBRepository import MBRepository
from Topic import Topic
from Producer import Producer
from Zookeeper import Zookeeper

StartTime = time.time()

window = tk.Tk()
window.minsize(720,480)
window.title("Message Broker Test Application")

def GetSeconds(sec):
    sec = sec % 60
    mins = mins % 60
    hours = mins // 60
    return ("{0}".format(sec))

def RunTest():
    repository = MBRepository()
    repository.CreateTables()
    zooKeeper = Zookeeper()
    zooKeeper.CreateDBTables()

    brokerPort = 8000
    brokerId = uuid4()
    broker = Broker(brokerId)
    repository.AddBroker(broker.Id, broker.Port)

    topic = broker.AddTopic("topic1")
    repository.AddTopic(topic.Id, topic.Name)
        
    for partition in topic.Partitions:
        partitionId:uuid4 = partition.Id
        repository.AddPartition(partitionId, topic.Id)

    consumer1 = Consumer("group1",True)
    consumer1Thread = Thread(target=asyncio.run, args=(consumer1.ListenOnTopic(str(topic.Id), PrintConsumerMessage),))
    consumer1Thread.start()
    consumer2 = Consumer("group1",True)
    consumer2Thread = Thread(target=asyncio.run, args=(consumer2.ListenOnTopic(str(topic.Id), PrintConsumerMessage),))
    consumer2Thread.start()
    consumer3 = Consumer("group2",True)
    consumer3Thread = Thread(target=asyncio.run, args=(consumer3.ListenOnTopic(str(topic.Id), PrintConsumerMessage),))
    consumer3Thread.start()
    consumer4 = Consumer("group2",True)
    consumer4Thread = Thread(target=asyncio.run, args=(consumer4.ListenOnTopic(str(topic.Id), PrintConsumerMessage),))
    consumer4Thread.start()
    

    for i in range(0,100):
        message = f"Message: {i}, TopicId: {topic.Id}"
        producer = Producer(True)
        producer.SendMessage(topic.Id, message, broker.Port)

    end = time.time()
    result.config(text = "Result (s): " + GetSeconds(end - StartTime))

def PrintConsumerMessage(text):

    if isinstance(text, list):
        for message in text:
            logTabOutput.insert(1.0, message + "\n" )
    else:
        logTabOutput.insert(1.0, text + "\n" )


def addErrorLogText(text):
    errorTabOutput.insert(1.0, text + "\n" )

greeting = tk.Label(text="Welcome")
greeting.pack()

runButton = tk.Button(window, text = 'Run Test!', command=lambda:RunTest()).pack()

result = tk.Label(text="Result (s): ")
result.pack()

tabparent = Notebook(window)

logTab = tk.Frame(tabparent)
tabparent.add(logTab, text="Meesage Log")

errorTab = tk.Frame(tabparent)
tabparent.add(errorTab, text="Errors")

tabparent.pack(expand=1, fill='both')


logTabOutputScrollBar = tk.Scrollbar(logTab, orient='vertical')
errorTabOutputScrollBar = tk.Scrollbar(errorTab, orient='vertical')


logTabOutput = Text(logTab, height = 30, width = 25, yscrollcommand=logTabOutputScrollBar.set)
logTabOutput.pack(fill='both', expand=True, side=tk.LEFT)
logTabOutput.insert(1.0, "No Messages.")


errorTabOutput = Text(errorTab, height = 30, width = 25, yscrollcommand=errorTabOutputScrollBar.set)
errorTabOutput.pack(fill='both', expand=True, side=tk.LEFT)
errorTabOutput.insert(1.0, "No Errors.")


logTabOutputScrollBar.pack(side=tk.RIGHT, fill='y')
logTabOutputScrollBar.config(command=logTabOutput.yview)


errorTabOutputScrollBar.pack(side=tk.RIGHT, fill='y')
errorTabOutputScrollBar.config(command=errorTabOutput.yview)


timeResults = tk.Entry(window, )


window.mainloop()