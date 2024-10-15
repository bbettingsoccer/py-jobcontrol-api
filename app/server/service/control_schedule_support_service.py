from bson import ObjectId
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dao.operationimpl_dao import OperationImplDAO
from fastapi.encoders import jsonable_encoder
from app.server.model.control_schedule_model import ControlScheduleModel


class ControlScheduleSupportService:

    def __init__(self) -> object:
        self.collection = OperationImplDAO("control_scheduled_support")

    #################################################################################################################
    #################################### METHOD EXECUTE PROCESS ONLINE - ROUTER() ###################################
    #################################################################################################################

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

    async def getJobForCondition(self, search: str, values: list[str]) -> []:
        print("[ControlScheduleSupportService]-[getJobForCondition] - START")
        objectL = []
        filter = []
        match search:
            case MatchConstants.GET_JOB_BY_NAME:
                filter = {ControlScheduleModel.config.job_name: {"$eq": values[0]}}

            case MatchConstants.GET_JOB_BY_NAME_INSTANCE_EVENT_STATUS:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_instance: {"$eq": values[1]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[2]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[3]}}]
                          }

            case MatchConstants.GET_JOB_BY_NAME_INSTANCE:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_instance: {"$eq": values[1]}}]}

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
            case MatchConstants.GET_JOB_NAME_EVENT:
                filter = {"$and": [{ControlScheduleModel.config.job_name: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.job_event: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_TYPE_CLASSIFICATION_STATUS_EVENT:
                filter = {"$and": [{ControlScheduleModel.config.job_type: {"$eq": values[0]}},
                                   {ControlScheduleModel.config.status: {"$eq": values[1]}},
                                   {"$or": [
                                       {ControlScheduleModel.config.job_event: {"$eq": values[2]}},
                                       {ControlScheduleModel.config.job_event: {"$eq": values[3]}},
                                       {ControlScheduleModel.config.job_event: {"$eq": values[4]}}]
                                   },
                                   {ControlScheduleModel.config.job_classification: {"$eq": values[5]}}]
                          }
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

    async def delete_for_all(self):
        try:
            return await self.collection.delete_many()
        except Exception as e:
            print('[ControlScheduleSupportService]-[delete_for_all] - ERROR ', e)
            return False

    async def save(self, data: ControlScheduleModel):
        print("[ControlScheduleSupportService][save] - START")
        try:
            jsonObj = jsonable_encoder(data)
            await self.collection.save(jsonObj)
        except Exception as e:
            print("[ControlScheduleSupportService][save] - ERROR :: ", e)
            raise
        finally:
            print("[ControlScheduleSupportService][save] - End")
            return True

    async def cancelInstanceSchedule(self, job_name: str, job_instance: str):
        try:
            filter = [job_name, job_instance]
            objectL = await self.getJobForCondition(MatchConstants.GET_JOB_BY_NAME_INSTANCE, filter)
            if objectL:
                for job_dict in objectL:
                    id = (job_dict['_id'])
                    del job_dict['_id']
                    job_dict[ControlScheduleModel.config.job_event] = MatchConstants.EVENT_SCHEDULED_STOPPED
                    job_dict[ControlScheduleModel.config.status] = MatchConstants.STATUS_STOPPER_MANUAL
                    await self.collection.update_one(id, job_dict)
                    return True
        except Exception as e:
            print('Error :: Service] - cancelJobSupport ', e)
            return False

    #################################################################################################################
    #################################### METHOD EXECUTE PROCESS BACKGROUND ##########################################
    #################################################################################################################

    async def update_schedule_for_event_and_status(self, job_name: str, schedule_event_find: str,
                                                   schedule_event_save: str,
                                                   schedule_status: str):
        print("[ControlScheduleSupportService][update_for_schedule_event_init] - START")
        try:
            value_filter = [job_name, schedule_event_find]
            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_NAME_EVENT, value_filter)
            if jbc_schedule_l:
                objectFind = jbc_schedule_l[0]
                filter = {"job_name": job_name}
                del objectFind['_id']
                objectFind[ControlScheduleModel.config.job_event] = schedule_event_save
                objectFind[ControlScheduleModel.config.status] = schedule_status
                await self.collection.update_many(filter, objectFind)
        except Exception as e:
            print("[ControlScheduleSupportService][update_for_schedule_event_init] - ERROR ::", e)
            raise
        finally:
            print("[ControlScheduleSupportService][update_for_schedule_event_init] - END")
            return True

    async def update_for_jobs_support_no_finished(self):
        print("[ControlScheduleSupportService]-[update_for_jobs_support_no_finished] - START ")
        date_now = TimezoneUtil.dateNowUTC()
        dateTime_Mongo = TimezoneUtil.getDateTimeForSearchMongo(date_now)
        try:
            value_filter = [MatchConstants.JOB_TYPE_SUPPORT, MatchConstants.EVENT_SCHEDULED_INIT,
                            MatchConstants.EVENT_SCHEDULED_START, None, MatchConstants.JOB_SUPPORT_ONLINE_WITH_DATETIME]

            jbc_schedule_l = await self.getJobForCondition(MatchConstants.GET_JOB_TYPE_CLASSIFICATION_STATUS_EVENT,
                                                           value_filter)
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
            print("[ControlScheduleService]-[update_for_jobs_support_no_finished] - END ")
            return True
        except Exception as e:
            print('Error :: Service] - Update >', e)
            return False

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
