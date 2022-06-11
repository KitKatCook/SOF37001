import tkinter as tk
from tkinter import *
from tkinter.ttk import Notebook

window = tk.Tk()
window.minsize(720,480)
window.title("Message Broker Test Application")

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

runButton = tk.Button(window, text = 'Run Test!', command=lambda:addMessageLogText("new content")).pack()

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


