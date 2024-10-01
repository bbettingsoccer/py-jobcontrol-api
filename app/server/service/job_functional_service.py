from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.schedule.factory.schedule_job_functional_factory import ScheduleJobFunctionalFactory
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dao.operationimpl_dao import OperationImplDAO
from fastapi.encoders import jsonable_encoder
from app.server.model.job_functional_model import JobFunctionalModel
from app.server.service.control_job_schedule_service import ControlScheduleService


class JobFunctionalService:

    def __init__(self):
        self.collection = OperationImplDAO("job_functional")

    async def getJobAll(self):
        objectL = []
        try:
            objects = await self.collection.find_condition(None)
            if objects:
                for objected in objects:
                    objectL.append(JobFunctionalModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: Service] - FindAll ", e)
        return None

    async def getJobForCondition(self, search: str, values: [str]) -> [JobFunctionalModel]:
        objectL = []
        filter = []
        match search:
            case MatchConstants.GET_JOB_NAME:
                filter = {JobFunctionalModel.config.job_name: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_CHAMPIONSHIP:
                filter = {JobFunctionalModel.config.championship: {"$eq": values[0]}}
            case MatchConstants.GET_JOB_DAY:
                filter = {JobFunctionalModel.config.week_days: {"$eq": values}}
            case MatchConstants.GET_JOB_DATE_STATUS:
                filter = {"$and": [{JobFunctionalModel.config.date_start: {"$lte": values[0]}},
                                   {JobFunctionalModel.config.date_end: {"$gte": values[0]}},
                                   {JobFunctionalModel.config.status: {"$eq": values[1]}}]}
        try:
            objects = await self.collection.find_condition(filter)
            if objects:
                for objected in objects:
                    objectL.append(JobFunctionalModel.data_helper(objected))
                return objectL
        except Exception as e:
            print("[Error :: Service] - FindCondition > Filter :", filter)
            print("[Error :: Service] - FindCondition > Error :", e)

        return None

    async def deleteJobForCondition(self, type: str, values: [str]):
        filter = []
        match type:
            case MatchConstants.DELETE_JOB_NAME:
                filter = {JobFunctionalModel.config.job_name: {"$eq": values[0]}}
        try:
            return await self.collection.delete_condition(filter)
        except Exception as e:
            print('Error :: Service] - DeleteCondition > Filter ', filter)
            return False

    async def save(self, data: JobFunctionalModel):
        filter = []
        result = False
        try:
            values = [data.job_name]
            objUpdateOrSave = await self.getJobForCondition(MatchConstants.GET_JOB_NAME, values)
            if objUpdateOrSave is None:  # SAVE - Transaction
                jsonObj = jsonable_encoder(data)
                await self.collection.save(jsonObj)
                result = True
            return result
        except Exception as e:
            print('Error :: Service] - Save >', e)
            raise

    @staticmethod
    async def job_schedule_for_datetime(jbc_plan: JobFunctionalModel):
        scheduleService = ControlScheduleService()
        timeUtil = TimezoneUtil()
        try:
            dateTimeExecution = timeUtil.convertDateTimeOrigenToDateTimeLocal(jbc_plan.date_start, jbc_plan.date_end,
                                                                              jbc_plan.time_start, jbc_plan.time_end,
                                                                              jbc_plan.timezones_execute)
            value_filter = [jbc_plan.championship, MatchConstants.EVENT_SCHEDULED_INIT,
                            MatchConstants.EVENT_SCHEDULED_START,
                            dateTimeExecution['datetime_start']]

            jbc_schedule = await scheduleService.getJobForCondition(MatchConstants.GET_JOB_CHAMPIONSHIP_EVENT_DATE,
                                                                    value_filter)
            if jbc_schedule is None:
                print(" === Open Process Schedule type _datetime === ")
                job_schedule_dto = JobScheduleDto(jbc_plan.job_name,
                                                  jbc_plan.job_type,
                                                  jbc_plan.championship,
                                                  dateTimeExecution['datetime_start'],
                                                  dateTimeExecution['datetime_end'],
                                                  None,
                                                  jbc_plan.collection_name,
                                                  jbc_plan.interval)
                job_instance = ScheduleJobFunctionalFactory(job_schedule_dto)
                job_instance.job_instance()
        except Exception as e:
            print('Error :: Service] - job_schedule_for_datetime >', e)
            raise

    async def job_schedule_for_planned(self):
        scheduleService = ControlScheduleService()
        timeUtil = TimezoneUtil()
        try:
            dateTomorrow = TimezoneUtil.getDateFormat(MatchConstants.DAY_TODAY)
            print("dateD+1 ", dateTomorrow)

            values = [dateTomorrow, MatchConstants.JOB_STATUS_PLAN]
            print("values ", values)

            # (1) # Get LIST ALL JOBs with date between data_start and date_end and status = PLAN
            objectL = await self.getJobForCondition(MatchConstants.GET_JOB_DATE_STATUS, values)
            if objectL:
                for job_dict in objectL:
                    jbc_plan = JobFunctionalModel.parse_obj(job_dict)
                    # (2) Check if day for schedule is weekday
                    for weekday_schedule in jbc_plan.week_days:
                        if timeUtil.confirmDayWeek(dateTomorrow, weekday_schedule):
                            print("  jbc_schedule NOW ", weekday_schedule)
                            dateTimeExecution = timeUtil.convertDateTimeOrigenToDateTimeLocal(dateTomorrow,
                                                                                              dateTomorrow,
                                                                                              jbc_plan.time_start,
                                                                                              jbc_plan.time_end,
                                                                                              jbc_plan.timezones_execute)

                            value_filter = [jbc_plan.championship, MatchConstants.EVENT_SCHEDULED_INIT,
                                            MatchConstants.EVENT_SCHEDULED_START,
                                            dateTimeExecution['datetime_start']]

                            jbc_schedule = await scheduleService.getJobForCondition(
                                MatchConstants.GET_JOB_CHAMPIONSHIP_EVENT_DATE, value_filter)
                            if jbc_schedule is None:
                                print(" === Open Process Schedule type _planned === ")
                                job_schedule_dto = JobScheduleDto(jbc_plan.job_name,
                                                                  jbc_plan.job_type,
                                                                  jbc_plan.championship,
                                                                  dateTimeExecution['datetime_start'],
                                                                  dateTimeExecution['datetime_end'],
                                                                  None,
                                                                  jbc_plan.collection_name,
                                                                  jbc_plan.interval)
                                job_instance = ScheduleJobFunctionalFactory(job_schedule_dto)
                                job_instance.job_instance()
        except Exception as e:
            print('Error :: Service] - job_schedule_for_planned >', e)
            raise
