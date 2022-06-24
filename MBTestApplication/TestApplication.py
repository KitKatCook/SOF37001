import asyncio
import os
import sys
from threading import Thread
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

window = tk.Tk()
window.minsize(720,480)
window.title("Message Broker Test Application")



def RunTest():
    repository = MBRepository()
    repository.CreateTables()
    zooKeeper = Zookeeper()
    zooKeeper.CreateDBTables()

    brokerPort = 8000
    brokerId = uuid4()
    broker = Broker(brokerId)
    repository.AddBroker(broker.Id, broker.Port)

    topic: Topic = broker.AddTopic()

    consumer1 = Consumer("group1")
    consumer1Thread = Thread(target=asyncio.run, args=(consumer1.ListenOnTopic([topic.Id], addMessageCg1),))
    consumer2 = Consumer("group1")
    consumer2Thread = Thread(target=asyncio.run, args=(consumer2.ListenOnTopic([topic.Id], addMessageCg1),))
    consumer3 = Consumer("group2")
    consumer3Thread = Thread(target=asyncio.run, args=(consumer3.ListenOnTopic([topic.Id], addMessageCg1),))
    consumer4 = Consumer("group2")
    consumer4Thread = Thread(target=asyncio.run, args=(consumer4.ListenOnTopic([topic.Id], addMessageCg1),))
    topic = broker.AddTopic("topic1")
    repository.AddTopic(topic.Id, topic.Name)
    
    for partition in topic.Partitions:
        partitionId:uuid4 = partition.Id
        repository.AddPartition(partitionId, topic.Id)

    consumer1 = Consumer("Group1",True)
    consumer2 = Consumer("Group1",True)
    consumer3 = Consumer("Group2",True)
    consumer4 = Consumer("Group2",True)

    for i in range(0,100):
        message = f"Message: {i}, TopicId: {topic.Id}"
        producer = Producer(True)
        producer.SendMessage(topic.Id, message, broker.Port)
        logTabOutput.insert(1.0, message + "\n" )

def addMessageLogText(text):
    logTabOutput.insert(1.0, text + "\n" )

def addErrorLogText(text):
    errorTabOutput.insert(1.0, text + "\n" )

def setResultsTime(text):
    errorTabOutput.insert(1.0, text + "\n" )

def setResultsMessages(text):
    errorTabOutput.insert(1.0, text + "\n" )

greeting = tk.Label(text="Welcome")
greeting.pack()

runButton = tk.Button(window, text = 'Run Test!', command=lambda:RunTest()).pack()

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


