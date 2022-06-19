import sqlite3

class TopicRepository:
    def __init__():
        pass

    def CreateTable(self):
        conn = sqlite3.connect('MBData.sqlite')
        
        cur = conn.cursor()
        cur.execute('CREATE TABLE Topic (Id UNIQUEIDENTIFIER, name VARCHAR)')
        conn.commit()

        conn.close()

