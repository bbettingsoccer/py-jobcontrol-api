from _datetime import datetime
from bson import ObjectId
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dao.operationimpl_dao import OperationImplDAO
from fastapi.encoders import jsonable_encoder

from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.model.control_schedule_model import ControlScheduleModel
from app.server.schedule.factory.schedule_job_functional_factory import ScheduleJobFunctionalFactory
from app.server.service.control_execution_service import ControlExecutionService


class ControlScheduleService:

    def __init__(self):
        self.collection = OperationImplDAO("control_scheduled")

    async def getJobAll(self):
        objectL = []
        try:
            objects = await self.collection.find_condition(None)
            print(" getJobsForAll ", objects)
            if objects:
                for objected in objects:
                    objectL.append(ControlScheduleModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: Service] - FindAll ", e)
        return None

    async def getJobForCondition(self, search: str, values: [str]) -> []:
        objectL = []
        filter = []
        match search:
            case MatchConstants.GET_JOB_NAME:
                filter = {ControlScheduleModel.config.job_name: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_NAME_STATUS:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_CHAMPIONSHIP_EVENT_DATE:
                filter = {"$and": [{ControlScheduleModel.config.championship: {"$eq": values[0]}},
                                   {"$or": [
                                       {ControlScheduleModel.config.job_event: {"$eq": values[1]}},
                                       {ControlScheduleModel.config.job_event: {"$eq": values[2]}}]
                                   },
                                   {ControlScheduleModel.config.start_date_execution: {"$eq": values[3]}}]
                          }
            case MatchConstants.GET_JOB_CHAMPIONSHIP:
                filter = {ControlScheduleModel.config.championship: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_EVENT:
                filter = {ControlScheduleModel.config.job_event: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_EVENT_EVENT:
                filter = {"$or": [{ControlScheduleModel.config.job_event: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_DATE_STATUS:
                filter = {"$and": [{ControlScheduleModel.config.start_date_execution: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_NAME_EVENT_STATUS:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[2]}}]}
            case MatchConstants.GET_JOB_NAME_EVENT_STATUS_DATE:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[2]}},
                                   {ControlScheduleModel.config.start_date_execution: {"$eq": values[3]}}]}
        try:
            objects = await self.collection.find_condition(filter)
            if objects:
                for objected in objects:
                    objectL.append(ControlScheduleModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: Service] - FindCondition > Filter :", filter)
            return None

    async def deleteJobForCondition(self, type: str, values: [str]):
        filter = []
        match type:
            case MatchConstants.DELETE_JOB_NAME:
                filter = {ControlScheduleModel.config.job_name: {"$eq": values[0]}}
        try:
            return await self.collection.delete_condition(filter)
        except Exception as e:
            print('Error :: Service] - DeleteCondition > Filter ', filter)
            return False

    async def save(self, data: ControlScheduleModel):
        filter = []
        try:
            jsonObj = jsonable_encoder(data)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print('Error :: Service] - Save >', e)
            raise

    async def update_for_schedule_event_init(self, job_name: str, schedule_event_find: str, schedule_event_save: str,
                                             schedule_status: str, dateTimeObj: datetime):
        filter = []
        try:
            value_filter = [job_name, schedule_event_find, MatchConstants.STATUS_SUCCESS, dateTimeObj]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_NAME_EVENT_STATUS_DATE, value_filter)
            if jbc_schedule_l:
                objectFind = jbc_schedule_l[0]
                filter = {"job_name": job_name}
                del objectFind['_id']
                objectFind[ControlScheduleModel.config.job_event] = schedule_event_save
                objectFind[ControlScheduleModel.config.status] = schedule_status
                await self.collection.update_many(filter, objectFind)
            return True
        except Exception as e:
            print('Error :: Service] - Update >', e)
            raise

    async def update_for_jobs_no_finished(self):
        print("-- update_for_schedule_no_finished --")
        jbc_execution = ControlExecutionService()
        date_now = TimezoneUtil.dateNowUTC()
        dateTime_Mongo = TimezoneUtil.getDateTimeForSearchMongo(date_now)
        try:
            value_filter = [MatchConstants.EVENT_SCHEDULED_START]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_EVENT, value_filter)
            if jbc_schedule_l:
                for jbc_sche_obj in jbc_schedule_l:
                    objectFind_up = jbc_sche_obj
                    if dateTime_Mongo > objectFind_up[ControlScheduleModel.config.end_date_execution]:
                        id = (jbc_sche_obj['_id'])
                        del objectFind_up['_id']
                        filter = {"_id": ObjectId(id)}
                        objectFind_up[
                            ControlScheduleModel.config.job_event] = MatchConstants.EVENT_SCHEDULED_FINISHED_AG
                        await self.collection.update_many(filter, objectFind_up)
                        await jbc_execution.save_job_no_finished(objectFind_up[ControlScheduleModel.config.job_name],
                                                                 objectFind_up[
                                                                     ControlScheduleModel.config.championship])
        except Exception as e:
            print('Error :: Service] - Update with schedule_event no  finished >', e)
            raise


    async def relaunch_schedule_jobs_with_fail(self):
        print("-- relaunch_jobs_with_fail --")
        jbc_execution = ControlExecutionService()
        date_now = TimezoneUtil.dateNowUTC()
        dateTime_Mongo = TimezoneUtil.getDateTimeForSearchMongo(date_now)
        try:
            value_filter = [MatchConstants.EVENT_SCHEDULED_START, MatchConstants.EVENT_SCHEDULED_INIT]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_EVENT_EVENT, value_filter)
            if jbc_schedule_l:
                for jbc_sche_obj in jbc_schedule_l:
                    objectFind_up = jbc_sche_obj
                    if objectFind_up[ControlScheduleModel.config.end_date_execution] > dateTime_Mongo:
                        print(" === Open Process Schedule type _planned === ")
                        job_schedule_dto = JobScheduleDto(objectFind_up.job_name,
                                                          objectFind_up.job_type,
                                                          objectFind_up.championship,
                                                          objectFind_up[ControlScheduleModel.config.start_date_execution],
                                                          objectFind_up[ControlScheduleModel.config.end_date_execution],
                                                          None,
                                                          objectFind_up.collection_name,
                                                          objectFind_up.interval)
                        job_instance = ScheduleJobFunctionalFactory(job_schedule_dto)
                        job_instance.job_instance()
                        id = (jbc_sche_obj['_id'])
                        del objectFind_up['_id']
                        filter = {"_id": ObjectId(id)}
                        objectFind_up[ControlScheduleModel.config.job_event] = MatchConstants.EVENT_SCHEDULED_FAIL
                        await self.collection.update_many(filter, objectFind_up)
                        await jbc_execution.save_job_no_finished(objectFind_up[ControlScheduleModel.config.job_name],
                                                                 objectFind_up[ControlScheduleModel.config.championship])
        except Exception as e:
            print('Error :: Service] - relaunch_schedule_jobs_with_fail  >', e)
            raise

    async def load_jobs_schedule_init(self):
        print("instance_job_support :")
        try:
            values = [MatchConstants.EVENT_SCHEDULED_INIT]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_EVENT, values)
            print("instance_job_support :", jbc_schedule_l)
            if jbc_schedule_l:
                for jbc_schedule_i in jbc_schedule_l:
                    jbc_support_obj = ControlScheduleModel.parse_obj(jbc_schedule_i)

        except Exception as e:
            print("[Error :: Service] - Update")
            return False
