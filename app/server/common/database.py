import os
import motor.motor_asyncio
import asyncio

class MongoManager:
    __instance = None

    def __init__(self):
        print("Connect __init__.")

        if MongoManager.__instance != None:
            print("This class is a singleton")
            raise Exception("This class is a singleton!")
        else:
            print("call  getConnection")
            MongoManager.__instance = self.getConnection()

    @staticmethod
    def getInstance():
        print("Connect MongoDB.")
        if MongoManager.__instance == None:
            MongoManager()
        return MongoManager.__instance

    def getConnection(self):
        print("Connect getConnection")
        connURL = os.environ["DB_URL"]
        DB_NAME = os.environ["DB_NAME"]
        try:
            client = motor.motor_asyncio.AsyncIOMotorClient(connURL, serverSelectionTimeoutMS=5000)
            client.get_io_loop = asyncio.get_running_loop
            database = client[DB_NAME]
            print("Connect SUCCESS.")
            return database
        except Exception:
            print("Unable to connect to the server.")
            return None
