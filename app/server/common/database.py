import os
import motor.motor_asyncio
import asyncio


class MongoManager:
    __instance = None

    def __init__(self):
        if MongoManager.__instance != None:
            print("This class is a singleton")
            raise Exception("This class is a singleton!")
        else:
            MongoManager.__instance = self.getConnection()

    @staticmethod
    def getInstance():
        if MongoManager.__instance is None:
            MongoManager()
        return MongoManager.__instance

    def getConnection(self):
        connURL = os.environ["DB_URL"]
        DB_NAME = os.environ["DB_NAME"]
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            client = motor.motor_asyncio.AsyncIOMotorClient(connURL, serverSelectionTimeoutMS=5000, io_loop=loop)
            client.get_io_loop = asyncio.get_running_loop
            database = client[DB_NAME]
            return database
        except Exception:
            print("Unable to connect to the server.")
            return None

