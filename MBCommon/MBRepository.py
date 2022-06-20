from sqlite3 import Connection
import sqlite3

class MBRepository:
    conn: Connection
    
    def __init__(self):
        self.conn = sqlite3.connect('MBData.sqlite') 
        
    def CreateTables(self):
        conn = sqlite3.connect('MBData.sqlite')
        
        cur = conn.cursor()

        cur.execute(F"DROP TABLE Topic")
        cur.execute(F"DROP TABLE Partition")
        cur.execute(F"DROP TABLE Broker")

        cur.execute(f"CREATE TABLE Topic (Id UNIQUEIDENTIFIER, Name VARCHAR)")
        conn.commit()

        cur.execute(f"CREATE TABLE Partition (Id UNIQUEIDENTIFIER, TopicId UNIQUEIDENTIFIER)")
        conn.commit()

        cur.execute(f"CREATE TABLE Broker (Id UNIQUEIDENTIFIER, Port INT)")
        conn.commit()

    def AddTopic(self, id, name):
        self.conn.execute(f"INSERT INTO Topic (Id,Name) VALUES ('{id}', '{name}')")
        self.conn.commit()

    def GetAllTopics(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM Topic")
        return cur.fetchall()

    def AddPartition(self, id, topicId):
        self.conn.execute(f"INSERT INTO Partition (Id, TopicId) VALUES ('{id}', '{topicId}')")
        self.conn.commit()

    def GetAllPartitions(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM Partition")
        return cur.fetchall()


    def AddBroker(self, id, port):
        self.conn.execute(f"INSERT INTO Broker (Id, Port) VALUES ('{id}', '{port}')")
        self.conn.commit()

    def GetAllBroker(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM Broker")
        return cur.fetchall()
