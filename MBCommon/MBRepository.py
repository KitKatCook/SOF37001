from sqlite3 import Connection
import sqlite3

## MBRepository class.
#  @author  Kit Cook
#  @version 1.0
#  @date    22/06/2022
#  @bug     No known bugs.
#  
#  @details This class contains methods for saving and retrieving data from the database.
class MBRepository:
    conn: Connection
    
    ## __init__ method.
    #  @param self The class pointer.
    def __init__(self):
        self.conn = sqlite3.connect('MBData.sqlite', check_same_thread = False) 
        
    ## CreateTables method.
    #  @param self The class pointer.
    #  @details Create the table structure needed for the system to persist data.
    def CreateTables(self):
        conn = sqlite3.connect('MBData.sqlite', check_same_thread = False)
        
        cur = conn.cursor()
        
        try:
            cur.execute(F"DROP TABLE Topic")
            cur.execute(F"DROP TABLE Partition")
            cur.execute(F"DROP TABLE Broker")
            cur.execute(F"DROP TABLE UserGroup")
            cur.execute(F"DROP TABLE GroupOffset")
        except:
            pass

        cur.execute(f"CREATE TABLE Topic (Id UNIQUEIDENTIFIER, Name VARCHAR)")
        conn.commit()

        cur.execute(f"CREATE TABLE Partition (Id UNIQUEIDENTIFIER, TopicId UNIQUEIDENTIFIER)")
        conn.commit()

        cur.execute(f"CREATE TABLE Broker (Id UNIQUEIDENTIFIER, Port INT)")
        conn.commit()

        cur.execute(f"CREATE TABLE UserGroup (Id UNIQUEIDENTIFIER, Name VARCHAR)")
        conn.commit()

        cur.execute(f"CREATE TABLE GroupOffset (Id UNIQUEIDENTIFIER, PartitionId UNIQUEIDENTIFIER, GroupId UNIQUEIDENTIFIER, Offset INT)")
        conn.commit()

    ## AddTopic method.
    #  @param self The class pointer.
    #  @param id Identifier of the topic.
    #  @param name Name of the topic.
    #  @details Persist a Topic entity.
    def AddTopic(self, id, name):
        self.conn.execute(f"INSERT INTO Topic (Id,Name) VALUES ('{id}', '{name}')")
        self.conn.commit()

    ## GetAllTopics method.
    #  @param self The class pointer.
    #  @return The all Topic entities that are persisted.
    #  @details This method return a collection of Topics.
    def GetAllTopics(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM Topic")
        return cur.fetchall()

    ## AddPartition method.
    #  @param self The class pointer.
    #  @param id Identifier of the partition.
    #  @param topicId Identifier of the topic.
    #  @details Persist a Partition entity.
    def AddPartition(self, id, topicId):
        self.conn.execute(f"INSERT INTO Partition (Id, TopicId) VALUES ('{id}', '{topicId}')")
        self.conn.commit()

    ## GetAllTopics method.
    #  @param GetAllPartitions The class pointer.
    #  @return The all Partition entities that are persisted.
    #  @details This method return a collection of Partitions.
    def GetAllPartitions(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM Partition")
        return cur.fetchall()

    ## AddTopic method.
    #  @param self The class pointer.
    #  @param id Identifier of the Broker.
    #  @param port port of the Broker.
    #  @details Persist a Broker entity.
    def AddBroker(self, id, port):
        self.conn.execute(f"INSERT INTO Broker (Id, Port) VALUES ('{id}', '{port}')")
        self.conn.commit()

    ## GetAllBroker method.
    #  @param GetAllPartitions The class pointer.
    #  @return The all Broker entities that are persisted.
    #  @details This method return a collection of Brokers.
    def GetAllBroker(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM Broker")
        return cur.fetchall()

    ## AddTopic method.
    #  @param self The class pointer.
    #  @param id Identifier of the UserGroup.
    #  @param name Name of the UserGroup.
    #  @details Persist a UserGroup entity.
    def AddGroup(self, id, name):
        self.conn.execute(f"INSERT INTO UserGroup (Id, Name) VALUES ('{id}', '{name}')")
        self.conn.commit()

    ## GetAllGroups method.
    #  @param GetAllPartitions The class pointer.
    #  @return The all UserGroup entities that are persisted.
    #  @details This method return a collection of UserGroups.
    def GetAllGroups(self):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM UserGroup")
        return cur.fetchall()

    ## AddTopic method.
    #  @param self The class pointer.
    #  @param id Identifier of the topic.
    #  @param partitionId Identifier of the partition.
    #  @param groupId Identifier of the usergroup.
    #  @param offset Offset Value.
    #  @details Persist a GroupOffset entity.
    def AddGroupOffset(self, id, partitionId, groupId, offset):
        self.conn.execute(f"INSERT INTO GroupOffset (Id, PartitionId, GroupId, Offset) VALUES ('{id}', '{partitionId}', '{groupId}', '{offset}')")
        self.conn.commit()

    ## GetGroupOffset method.
    #  @param GetAllPartitions The class pointer.
    #  @return The all GroupOffset entities that are persisted.
    #  @details This method return a collection of GroupOffsets.
    def GetGroupOffset(self, groupId):
        cur = self.conn.cursor()
        cur.execute(f"SELECT * FROM GroupOffset WHERE GroupId = '{groupId}'")
        return cur.fetchall()

    ## SetGroupOffset method.
    #  @param self The class pointer.
    #  @param groupId Identifier of the usergroup.
    #  @param offset Offset Value.
    #  @details Persist a topic entity.
    def SetGroupOffset(self, groupId, offset):
        cur = self.conn.cursor()
        cur.execute(f"UPDATE GroupOffset SET Offset = '{offset}' WHERE GroupId = '{groupId}'")
        self.conn.commit()

