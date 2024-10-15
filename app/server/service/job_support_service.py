from fastapi.encoders import jsonable_encoder

from app.server.common.general_function import GeneralFunctions
from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dao.operationimpl_dao import OperationImplDAO
from app.server.schedule.factory.schedule_job_support_factory import ScheduleJobSupportFactory
from app.server.model.job_support_model import JobSupportModel
from app.server.service.control_schedule_support_service import ControlScheduleSupportService


class JobSupportService:

    def __init__(self):
        self.collection = OperationImplDAO("job_support")

    #################################################################################################################
    #################################### METHOD ROUTER - ONLINE ########## ##########################################
    #################################################################################################################

    async def getJobAll(self):
        objectL = []
        try:
            objects = await self.collection.find_condition(None)
            print(" getJobsForAll ", objects)
            if objects:
                for objected in objects:
                    objectL.append(JobSupportModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: JobSupportService] - FindAll ", e)
        return None

    async def save(self, data: JobSupportModel):
        filter = []
        try:
            jsonObj = jsonable_encoder(data)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print('Error :: JobSupportService] - Save >', e)
            raise

    async def update(self, id: str, data: JobSupportModel):
        try:
            jsonObj = jsonable_encoder(data)
            dbObj = await self.collection.find_one(id)
            if dbObj:
                await self.collection.update_one(id, jsonObj)
                return True
            else:
                return False
        except Exception as e:
            print("[Error :: JobSupportService] - Update")
            return False

    async def getJobForCondition(self, search: str, values: []):
        currentsL = []
        filter = []
        match search:
            case MatchConstants.GET_JOB_BY_NAME:
                filter = {"$and": [{JobSupportModel.config.job_name: {"$eq": values[0]}}]}

            case MatchConstants.GET_JOB_BY_CLASSIFICATION:
                filter = {"$or": [{JobSupportModel.config.job_classification: {"$eq": values[1]}},
                                  {JobSupportModel.config.job_classification: {"$eq": values[2]}}]
                          }
            case MatchConstants.GET_JOB_BY_CLASSIFICATION_STATUS:
                filter = {"$and": [{JobSupportModel.config.status: {"$eq": values[0]}},
                                   {"$or":
                                        [{JobSupportModel.config.job_classification: {"$eq": values[2]}},
                                         {JobSupportModel.config.job_classification: {"$eq": values[3]}}]
                                    }]
                          }
        try:
            currents = await self.collection.find_condition(filter)
            if currents:
                for currented in currents:
                    currentsL.append(JobSupportModel.data_helper(currented))
                return currentsL
        except Exception as e:
            print("[Error :: JobSupportService ] - Find_Condition > Filter :", e)
            print("[Error :: JobSupportService ] - Find_Condition > Filter :", filter)
        return None

    async def deleteJobForCondition(self, type: str, values: [str]):
        filter = []
        match type:
            case MatchConstants.DELETE_JOB_NAME:
                filter = {JobSupportModel.config.job_name: {"$eq": values[0]}}
        try:
            return await self.collection.delete_condition(filter)
        except Exception as e:
            print('Error :: Service] - DeleteCondition > Filter ', filter)
            return False

    async def cancelJobSupport(self, job_name: str):
        try:
            filter = [job_name]
            objectL = await self.getJobForCondition(MatchConstants.GET_JOB_BY_NAME, filter)
            if objectL:
                for job_dict in objectL:
                    id = (job_dict['_id'])
                    del job_dict['_id']
                    job_dict[JobSupportModel.config.status] = MatchConstants.JOB_STATUS_DISABLED
                    await self.collection.update_one(id, job_dict)
        except Exception as e:
            print('Error :: Service] - cancelJobSupport ', e)
            return False

    async def planning_schedule_jobs_support_by_online(self, job_support_data: JobSupportModel):
        print("instance_job_support :")
        timeUtil = TimezoneUtil()
        dateTimeObj = None
        try:
            value_filter = [job_support_data.job_name, MatchConstants.JOB_STATUS_ACTIVE]
            jbc_support = await self.getJobForCondition(MatchConstants.GET_JOB_BY_NAME, value_filter)
            if jbc_support is None:
                await self.save(job_support_data)
                if job_support_data.timezones_execute == MatchConstants.TIMEZONE_SYSTEM:
                    dateTimeObj = timeUtil.getDateTimeTogether(job_support_data.date_start,
                                                               job_support_data.date_end,
                                                               job_support_data.time_start,
                                                               job_support_data.date_end)
                elif job_support_data.timezones_execute != MatchConstants.TIMEZONE_SYSTEM:
                    dateTimeObj = timeUtil.convertDateTimeOrigenToDateTimeLocal(job_support_data.date_start,
                                                                                job_support_data.date_end,
                                                                                job_support_data.time_start,
                                                                                job_support_data.date_end,
                                                                                job_support_data.timezones_execute)

                print(" === Open Process Schedule type job_support === datetime ")
                job_dto = JobScheduleDto(job_support_data.job_name,
                                         job_support_data.job_type,
                                         job_support_data.job_classification,
                                         None,
                                         job_support_data.function,
                                         dateTimeObj['datetime_start'],
                                         dateTimeObj['datetime_end'],
                                         job_support_data.class_external,
                                         job_support_data.method_external,
                                         job_support_data.interval)
                job_instance = ScheduleJobSupportFactory(job_dto)
                job_instance.job_instance()
                return True
            else:
                return False
        except Exception as e:
            print("[Error :: JobSupportService] - Update")
            return False

    #################################################################################################################
    #################################### METHOD EXECUTE PROCESS BACKGROUND FROM MAIN() ##########################################
    #################################################################################################################

    async def planning_schedule_jobs_support_by_runtime(self):
        print("[job_support_service]-[planning_schedule_jobs_support_by_runtime] - START ")
        timeUtil = TimezoneUtil()
        time_tz = None
        dateToday = timeUtil.dateToday()
        generalFunc = GeneralFunctions()
        try:
            # (1) # Get LIST ALL JOBs-SUPPORT BASED IN CONDICTION FILTER
            values = [MatchConstants.JOB_STATUS_ACTIVE, MatchConstants.JOB_SUPPORT_RUNTIME_WITHOUT_TIME,
                      MatchConstants.JOB_SUPPORT_RUNTIME_WITH_TIME]
            objectL = await self.getJobForCondition(MatchConstants.GET_JOB_BY_CLASSIFICATION_STATUS, values)
            if objectL:
                for job_dict in objectL:
                    jbc_support = JobSupportModel.parse_obj(job_dict)

                    time_tz = timeUtil.getTimeConvertFromTimezoneLocal(
                        jbc_support.datetime_use[JobSupportModel.config.time_start],
                        jbc_support.datetime_use[JobSupportModel.config.timezones_execute])
                    dateTime_schedule = timeUtil.getDateTimeTogether(dateToday, dateToday, time_tz, time_tz)
                    job_instance = generalFunc.random_number_characters()

                    job_dto = JobScheduleDto(jbc_support.job_name,
                                             jbc_support.job_type,
                                             jbc_support.job_classification,
                                             job_instance,
                                             jbc_support.function,
                                             dateTime_schedule['datetime_start'],
                                             dateTime_schedule['datetime_end'],
                                             jbc_support.class_external,
                                             jbc_support.method_external,
                                             jbc_support.datetime_use[JobSupportModel.config.interval])
                    job_instance = ScheduleJobSupportFactory(job_dto)
                    job_instance.job_instance()
        except Exception as e:
            print("[job_support_service]-[planning_schedule_jobs_support_by_datetime] - ERROR ", e)
            raise
        finally:
            print("[job_support_service]-[planning_schedule_jobs_support_by_datetime] - END ")

    async def planning_schedule_jobs_support_by_datetime(self):
        print("[job_support_service]-[planning_schedule_jobs_support_by_datetime] - Start ")
        timeUtil = TimezoneUtil()
        generalFunc = GeneralFunctions()

        try:
            # (1) # Get LIST ALL JOB-Support with JOB_TYPE = JOB_SUPPORT and Classification = JBSUP_DATETIME
            values = [MatchConstants.JOB_STATUS_ACTIVE, MatchConstants.JOB_SUPPORT_ONLINE_WITH_DATETIME,
                      MatchConstants.JOB_SUPPORT_ONLINE_WITH_DATETIME]
            objectL = await self.getJobForCondition(MatchConstants.GET_JOB_BY_CLASSIFICATION_STATUS, values)
            if objectL:
                for job_dict in objectL:
                    jbc_support = JobSupportModel.parse_obj(job_dict)

                    dateTime_schedule = timeUtil.convertDateTimeOrigenToDateTimeLocal(
                        jbc_support.datetime_use[JobSupportModel.config.date_start],
                        jbc_support.datetime_use[JobSupportModel.config.date_end],
                        jbc_support.datetime_use[JobSupportModel.config.time_start],
                        jbc_support.datetime_use[JobSupportModel.config.time_end],
                        jbc_support.datetime_use[JobSupportModel.config.timezones_execute])

                    job_instance = generalFunc.random_number_characters()

                    job_dto = JobScheduleDto(jbc_support.job_name,
                                             jbc_support.job_type,
                                             jbc_support.job_classification,
                                             job_instance,
                                             jbc_support.function,
                                             dateTime_schedule['datetime_start'],
                                             dateTime_schedule['datetime_end'],
                                             jbc_support.class_external,
                                             jbc_support.method_external,
                                             jbc_support.datetime_use[JobSupportModel.config.interval])
                    job_instance = ScheduleJobSupportFactory(job_dto)
                    job_instance.job_instance()
        except Exception as e:
            print("[job_support_service]-[planning_schedule_jobs_support_by_datetime] - ERROR > ", e)
            raise
        finally:
            print("[job_support_service]-[planning_schedule_jobs_support_by_datetime] - END  ")

    @staticmethod
    async def delete_all_schedule_job_support():
        print("[job_support_service]-[delete_all_schedule_job_support] - START ")
        controlScheduleObj = ControlScheduleSupportService()
        try:
            await controlScheduleObj.delete_for_all()
            return True
        except Exception as e:
            print("[job_support_service]-[delete_all_schedule_job_support] - ERROR ", e)
            raise
