from abc import ABC
from motor.core import AgnosticCollection
import asyncio
from .operation_dao import OperationDAO
from bson.objectid import ObjectId
from ..common.database import MongoManager


class OperationImplDAO(OperationDAO, ABC):
    instance_collection = None

    def __init__(self, collection):
        self.collection = collection
        self.instance_collection = self.get_collection()

    def get_collection(self) -> AgnosticCollection:
        database = MongoManager.getInstance()
        return database.get_collection(self.collection)

    async def save(self, data: dict) -> dict:
        try:
            loop = self.instance_collection.get_io_loop()
            loop.run_until_complete(self.instance_collection.insert_one(data))
            # await self.instance_collection.instance_collection.insert_one(data)
            #collectionObj = await self.instance_collection.insert_one(data)
            # new_collectionObj = await self.instance_collection.find_one({"_id": collectionObj.inserted_id})
            return None # collectionObj
        except RuntimeError as e:
            print("[Error :: DAO] - Save > ", e)
            return None

    async def find_condition(self, filter) -> list[dict]:
        jsonList = []
        try:
            if filter is not None:
                async for objectFind in self.instance_collection.find(filter):
                    jsonList.append(objectFind)
            else:
                async for objectFind in self.instance_collection.find():
                    jsonList.append(objectFind)
            return jsonList
        except Exception as e:
            print("[Error :: DAO] - Find > ", e)
            return None

    async def find_one(self, id: str) -> dict:
        try:
            collectionObj = await self.instance_collection.find_one({"_id": ObjectId(id)})
            if collectionObj:
                return collectionObj
        except Exception as e:
            print('[Error :: DAO] - Find_One > ', e)

    async def update_many(self, filter, data):
        try:
            await self.instance_collection.update_many(filter, {"$set": data})
            return True
        except Exception as e:
            print("[Error :: DAO] - Update_Many > ", e)
            return False

    async def update_one(self, id, data):
        try:
            await self.instance_collection.update_one({"_id": ObjectId(id)}, {"$set": data})
            return True
        except Exception as e:
            print("[Error :: DAO] - Update_One > ", e)
            return False

    async def delete_condition(self, filter):
        try:
            await self.instance_collection.delete_many(filter)
            return True
        except Exception as e:
            print('[Error :: DAO] - Delete_Condition > ', e)
            return False

    async def delete_many(self):
        try:
            await self.instance_collection.delete_many({})
            return True
        except Exception as e:
            print('[Error :: DAO] - Delete_Condition > ', e)
            return False

