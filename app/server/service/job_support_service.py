from fastapi.encoders import jsonable_encoder
from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dao.operationimpl_dao import OperationImplDAO
from app.server.schedule.factory.schedule_job_support_factory import ScheduleJobSupportFactory
from app.server.model.job_support_model import JobSupportModel


class JobSupportService:

    def __init__(self):
        self.collection = OperationImplDAO("job_support")

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
            print("[Error :: Service] - FindAll ", e)
        return None

    async def save(self, data: JobSupportModel):
        filter = []
        try:
            jsonObj = jsonable_encoder(data)
            await self.collection.save(jsonObj)
            return True
        except Exception as e:
            print('Error :: Service] - Save >', e)
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
            print("[Error :: Service] - Update")
            return False

    async def getJobForCondition(self, search: str, values: []):
        currentsL = []
        filter = []
        match search:
            case MatchConstants.GET_JOB_NAME_STATUS:
                filter = {"$and": [{JobSupportModel.config.job_name: {"$eq": values[0]}},
                                   {JobSupportModel.config.status: {"$eq": values[1]}}]}
            case MatchConstants.GET_JOB_TYPE_STATUS:
                filter = {"$and": [{JobSupportModel.config.job_type: {"$eq": values[0]}},
                                   {JobSupportModel.config.status: {"$eq": values[1]}}]}

        try:
            currents = await self.collection.find_condition(filter)
            if currents:
                for currented in currents:
                    currentsL.append(JobSupportModel.data_helper(currented))
                return currentsL
        except Exception as e:
            print("[Error :: Service] - Find_Condition > Filter :", filter)
        return None

    async def job_support_schedule_for_datetime(self, job_support_data: JobSupportModel):
        print("instance_job_support :")
        timeUtil = TimezoneUtil()
        dateTimeObj = None
        try:
            value_filter = [job_support_data.job_name, MatchConstants.JOB_STATUS_ACTIVE]
            jbc_support = await self.getJobForCondition(MatchConstants.GET_JOB_NAME_STATUS, value_filter)
            if jbc_support is None:
                await self.save(job_support_data)
                if job_support_data.timezones_execute == MatchConstants.TIMEZONE_SYSTEM:
                    dateTimeObj = timeUtil.getDateTimeInTimezoneOrigen(job_support_data.date_start,
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
                                         job_support_data.description,
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
            print("[Error :: Service] - Update")
            return False

    async def job_support_schedule_for_runtime(self):
        value_filter = []
        dataFormat = TimezoneUtil.getDateFormat(MatchConstants.DAY_TODAY)
        try:
            values = [MatchConstants.JOB_TYPE_SUPPORT_RUNTIME, MatchConstants.JOB_STATUS_ACTIVE]
            # (1) # Get LIST ALL JOBs-SUPPORT BASED IN CONDICTION FILTER
            objectL = await self.getJobForCondition(MatchConstants.GET_JOB_TYPE_STATUS, values)

            if objectL:
                for job_dict in objectL:
                    jbc_support = JobSupportModel.parse_obj(job_dict)
                    print(" === Open Process Schedule type _planned === jOB_SUPPORT EVER_DAY / EVER_TIME ")
                    job_dto = JobScheduleDto(jbc_support.job_name,
                                             jbc_support.job_type,
                                             jbc_support.description,
                                             None,
                                             None,
                                             jbc_support.class_external,
                                             jbc_support.method_external,
                                             jbc_support.interval)
                    job_instance = ScheduleJobSupportFactory(job_dto)
                    job_instance.job_instance()
        except Exception as e:
            print('Error :: Service] - checkJobForPlain >', e)
            raise
