from bson import ObjectId
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dao.operationimpl_dao import OperationImplDAO
from fastapi.encoders import jsonable_encoder
from app.server.model.control_schedule_model import ControlScheduleModel
from app.server.service.control_execution_service import ControlExecutionService


class ControlScheduleFunctionalService:

    def __init__(self) -> object:
        self.collection = OperationImplDAO("control_scheduled_functional")

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
            case MatchConstants.GET_JOB_BY_NAME:
                filter = {ControlScheduleModel.config.job_name: {"$eq": values[0]}}

            case MatchConstants.GET_JOB_BY_NAME_INSTANCE:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_instance: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_BY_NAME_INSTANCE_EVENT_STATUS:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_instance: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[2]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[3]}}]
                          }

            case MatchConstants.GET_JOB_CHAMPIONSHIP_EVENT_DATE:
                filter = {"$and": [{ControlScheduleModel.config.championship: {"$eq": values[0]}},
                                   {"$or": [
                                       {ControlScheduleModel.config.job_event: {"$eq": values[1]}},
                                       {ControlScheduleModel.config.job_event: {"$eq": values[2]}}]
                                   },
                                   {ControlScheduleModel.config.start_date_execution: {"$eq": values[3]}}]
                          }
            case MatchConstants.GET_JOB_BY_CHAMPIONSHIP:
                filter = {ControlScheduleModel.config.championship: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_EVENT:
                filter = {ControlScheduleModel.config.job_event: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_EVENT_EVENT:
                filter = {"$or": [{ControlScheduleModel.config.job_event: {"$eq": values[0]}},
                                  {ControlScheduleModel.config.job_event: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_DATE_STATUS:
                filter = {"$and": [{ControlScheduleModel.config.start_date_execution: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_BY_NAME_EVENTS_DATE:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {"$or":
                                        [{ControlScheduleModel.config.job_event: {"$eq": values[1]}},
                                         {ControlScheduleModel.config.job_event: {"$eq": values[2]}}
                                         ]
                                    },
                                   {ControlScheduleModel.config.start_date_execution: {"$eq": values[3]}}]}
            case MatchConstants.GET_JOB_BY_NAME_INSTANCE_EVENTS_DATE:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_instance: {"$eq": values[1]}},
                                   {"$or":
                                        [{ControlScheduleModel.config.job_event: {"$eq": values[2]}},
                                         {ControlScheduleModel.config.job_event: {"$eq": values[3]}}
                                         ]
                                    },
                                   {ControlScheduleModel.config.start_date_execution: {"$eq": values[3]}}]
                          }

            case MatchConstants.GET_JOB_TYPE_CLASSIFICATION_STATUS_EVENT:
                filter = {"$and": [{ControlScheduleModel.config.job_type: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[2]}},
                                   {ControlScheduleModel.config.job_classification: {"$eq": values[3]}}]}
            case MatchConstants.GET_JOB_TYPE_STATUS_EVENT:
                filter = {"$and": [{ControlScheduleModel.config.job_type: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[2]}}]}
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
            case MatchConstants.DELETE_JOB_BY_TYPE_CLASSIFICATION_EVENT:
                filter = {"$and": [{ControlScheduleModel.config.job_type: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.job_classification: {"$eq": values[2]}}]}
        try:
            return await self.collection.delete_condition(filter)
        except Exception as e:
            print('Error :: Service] - DeleteCondition > Filter ', filter)
            return False

    async def save(self, data: ControlScheduleModel):
        print("SERVICE > save ", data)

        try:
            jsonObj = jsonable_encoder(data)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print('Error :: Service] - Save >', e)
            raise

    async def update_schedule_for_event_and_status(self, job_name: str, job_instance: str, schedule_event_save: str,
                                                   status_schedule: str):
        print("update_for_schedule_event_init()")
        try:
            value_filter = [job_name, job_instance]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_BY_NAME_INSTANCE, value_filter)

            if jbc_schedule_l:
                for job_schedule in jbc_schedule_l:
                    print("[update_schedule_for_event_and_status] - UPDATE EVENT - STATUS ")
                    id = (job_schedule['_id'])
                    del job_schedule['_id']
                    filter = {"_id": ObjectId(id)}
                    job_schedule[ControlScheduleModel.config.job_event] = schedule_event_save
                    job_schedule[ControlScheduleModel.config.status] = status_schedule
                    await self.collection.update_many(filter, job_schedule)
                return True
        except Exception as e:
            print('Error :: Service] - update_for_schedule_event_init() >', e)
            raise

    async def update_for_jobs_functional_no_finished(self):
        print("[ControlScheduleFunctionalService]-[update_for_jobs_functional_no_finished] - START ")

        jbc_execution = ControlExecutionService()
        date_now = TimezoneUtil.dateNowUTC()
        dateTime_Mongo = TimezoneUtil.getDateTimeForSearchMongo(date_now)
        try:
            value_filter = [MatchConstants.JOB_TYPE_FUNCTIONAL, MatchConstants.STATUS_SUCCESS,
                            MatchConstants.EVENT_SCHEDULED_START]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_TYPE_STATUS_EVENT, value_filter)
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
            print("[ControlScheduleFunctionalService]-[update_for_jobs_functional_no_finished] - END ")
            return True
        except Exception as e:
            print('Error :: Service] - Update with schedule_event no  finished >', e)
            return False

    """
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
                                                          None,
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
    """

    async def checkScheduleIsStopped(self, job_name: str, job_instance: str):
        print("[ControlScheduleFunctionalService]-[checkScheduleIsStopped] - START ")
        try:
            values = [job_name, job_instance, MatchConstants.EVENT_SCHEDULED_STOPPED,
                      MatchConstants.STATUS_STOPPER_MANUAL]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_BY_NAME_INSTANCE_EVENT_STATUS, values)
            if jbc_schedule_l:
                return True
            else:
                return False
        except Exception as e:
            print("[ControlScheduleFunctionalService]-[checkExistJobScheduleEventStopped] - ERROR ", e)
            return False

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
