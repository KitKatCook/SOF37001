import tkinter as tk
from tkinter import *
from tkinter.ttk import Notebook

window = tk.Tk()
window.minsize(720,480)
window.title("Message Broker Test Application")

def setTextInput(text):
    logTabOutput.insert(1.0, text + "\n" )

greeting = tk.Label(text="Welcome")
greeting.pack()

runButton = tk.Button(window, text = 'Run Test!', command=lambda:setTextInput("new content")).pack()

tabparent = Notebook(window)

logTab = tk.Frame(tabparent)
tabparent.add(logTab, text="Meesage Log")

errorTab = tk.Frame(tabparent)
tabparent.add(errorTab, text="Errors")

tabparent.pack(expand=1, fill='both')

logTabOutput = Text(logTab, height = 30,
              width = 25)
logTabOutput.pack(fill='both', expand=True, side=tk.LEFT)
logTabOutput.insert(1.0, "No Messages.")

errorTabOutput = Text(errorTab, height = 30,
              width = 25)
errorTabOutput.pack(fill='both', expand=True, side=tk.LEFT)
errorTabOutput.insert(1.0, "No Errors.")

logTabOutputScrollBar = tk.Scrollbar(logTab, orient='vertical')
logTabOutputScrollBar.pack(side=tk.RIGHT, fill='y')
logTabOutputScrollBar.config(command=logTabOutput.yview)

errorTabOutputScrollBar = tk.Scrollbar(errorTab, orient='vertical')
errorTabOutputScrollBar.pack(side=tk.RIGHT, fill='y')
errorTabOutputScrollBar.config(command=errorTabOutput.yview)


window.mainloop()


