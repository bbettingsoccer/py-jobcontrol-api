from app.server.common.match_constants import MatchConstants
from app.server.dao.operationimpl_dao import OperationImplDAO
from fastapi.encoders import jsonable_encoder
from _datetime import datetime
from app.server.model.control_execution_model import ControlExecutionModel


class ControlExecutionService:

    def __init__(self):
        self.collection = OperationImplDAO("control_execution")

    async def getJobsForAll(self):
        objectL = []
        try:
            objects = await self.collection.find_condition(None)
            print(" getJobsForAll ", objects)
            if objects:
                for objected in objects:
                    objectL.append(ControlExecutionModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: Service] - FindAll")
        return None

    async def getJobForCondition(self, search: str, values: [str]):
        objectL = []
        filter = []
        match search:
            case MatchConstants.GET_JOB_CHAMPIONSHIP_DATE_STATUS:
                filter = {"$and": [{ControlExecutionModel.config.championship: {"$eq": values[0]}},
                                   {ControlExecutionModel.config.date_event: {"$eq": values[1]}},
                                   {ControlExecutionModel.config.status: {"$eq": values[2]}}]}
            case MatchConstants.GET_JOB_STATUS_DATE:
                filter = {"$and": [{ControlExecutionModel.config.status: {"$eq": values[0]}},
                                   {ControlExecutionModel.config.date_event: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_CHAMPIONSHIP:
                filter = {ControlExecutionModel.config.championship: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_NAME:
                filter = {ControlExecutionModel.config.job_name: {"$eq": values[0]}}

        try:
            objects = await self.collection.find_condition(filter)
            if objects:
                for objected in objects:
                    objectL.append(ControlExecutionModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: Service] - FindCondition > Filter :", filter)
        return None

    async def deleteJobForCondition(self, type: str, values: [str]):
        filter = []
        match type:
            case MatchConstants.DELETE_JOB_NAME:
                filter = {ControlExecutionModel.config.job_name: {"$eq": values[0]}}
            case MatchConstants.DELETE_STATUS:
                filter = {ControlExecutionModel.config.status: {"$eq": values[0]}}
            case MatchConstants.DELETE_DATE_EVENT:
                filter = {ControlExecutionModel.config.date_event: {"$eq": values[0]}}

        try:
            return await self.collection.delete_condition(filter)
        except Exception as e:
            print('Error :: Service] - DeleteCondition > Filter ', filter)
            return False

    async def save(self, data: ControlExecutionModel):
        print("jobcontrol_execute_save")
        try:
            jsonObj = jsonable_encoder(data)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print('Error :: Service] - Save >', e)
            raise

    async def save_job_no_finished(self, job_name: str, championship: str):
        print("jobcontrol_execute_save")
        try:
            execution_model = ControlExecutionModel(job_name=job_name,
                                                    job_event=MatchConstants.EVENT_JOB_FINISHED_AG,
                                                    date_event=datetime.now(),
                                                    championship=championship,
                                                    msg_info=MatchConstants.MSG_JOB_INF_AGENT,
                                                    status=MatchConstants.STATUS_SUCCESS)
            jsonObj = jsonable_encoder(execution_model)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print('Error :: Service] - save_job_no_finished >', e)
            raise
