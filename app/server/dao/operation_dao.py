from abc import ABCMeta, abstractmethod

from motor.core import AgnosticCollection


class OperationDAO(metaclass=ABCMeta):


    @abstractmethod
    def get_collection(self) -> AgnosticCollection:
        pass


    @abstractmethod
    async def find_condition(self, filter):
        pass

    @abstractmethod
    async def find_one(self, id: str) -> dict:
        pass

    @abstractmethod
    async def save(self, object) -> object:
        pass

    @abstractmethod
    async def update_many(self, filter, data):
        pass

    @abstractmethod
    async def update_one(self, id, data):
        pass

    @abstractmethod
    async def delete_condition(self, filter):
        pass
