from abc import ABC

from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.schedule.schedule_factory import ScheduleFactory
from apscheduler.events import EVENT_SCHEDULER_STARTED, EVENT_JOB_ERROR, EVENT_SCHEDULER_SHUTDOWN
from apscheduler.schedulers.background import BackgroundScheduler
import asyncio
from app.server.model.control_schedule_model import ControlScheduleModel
from app.server.service.control_schedule_functional_service import ControlScheduleFunctionalService
from app.server.service.control_schedule_support_service import ControlScheduleSupportService
from app.server.service.job_functional_service import JobFunctionalService


class ScheduleJobSupportFactory(ScheduleFactory, ABC):
    scheduler = None
    data = None
    api_url = None
    collection_name = None
    job = None
    method = None
    times_schedule_job = None
    reasonShutDown = False
    resultOperation = True

    def __init__(self, job_schedule_dto: JobScheduleDto):
        super().__init__()
        self.data = job_schedule_dto
        self.scheduler = BackgroundScheduler()
        class_name = self.getClassSupport(self.data.class_external)
        class_instance = class_name
        method_name = self.data.method_external
        self.method = getattr(class_instance, method_name)

    def job_instance(self):
        print("[ScheduleJobSupportFactory]-[job_instance] - START ")
        timeUtil = TimezoneUtil()
        try:
            # (1) Schedule Job Process
            self.scheduler.add_listener(self.handle_schedule_notification, EVENT_SCHEDULER_STARTED)
            self.scheduler.add_listener(self.handle_finish_notification, EVENT_SCHEDULER_SHUTDOWN)
            self.scheduler.add_listener(self.handle_error_notification, EVENT_JOB_ERROR)

            if self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITH_TIME:
                times_schedule = timeUtil.getTimeFromDateTime(self.data.datetime_start)

                trigger = CronTrigger(
                    year="*", month="*", day="*", hour=times_schedule[0], minute=times_schedule[1],
                    second=times_schedule[2]
                )
                self.scheduler.add_job(self.job_runtime, trigger, id=self.data.job_name)
                time_str = times_schedule[0] + ":" + times_schedule[1] + ":" + times_schedule[2]
                self.data.datetime_start = timeUtil.getDateTimeTogetherOne(time_str)
                self.data.datetime_end = timeUtil.dateNowUTC()
                self.times_schedule_job = time_str

            elif self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITHOUT_TIME:

                interval = IntervalTrigger(
                    minutes=self.data.interval
                )
                self.scheduler.add_job(self.job_runtime, interval, id=self.data.job_name)
                self.times_schedule_job = timeUtil.getTimeCurrentMoreInterval(self.data.interval)
                self.data.datetime_start = timeUtil.getDateTimeTogetherOne(self.times_schedule_job)
                self.data.datetime_end = timeUtil.dateNowUTC()

            elif self.data.job_classification == MatchConstants.JOB_SUPPORT_ONLINE_WITH_DATETIME:
                self.scheduler.add_job(self.job_runtime,
                                       "interval",
                                       days=self.data.interval,
                                       start_date=self.data.datetime_start,
                                       end_date=self.data.datetime_end,
                                       id=self.data.job_name)
            self.scheduler.start()
        except Exception as e:
            print("[ScheduleJobSupportFactory]-[job_instance] - ERROR:: ", e)

    @staticmethod
    def getClassSupport(name: str):
        factory_dict = {
            "ControlFunctionalSchedule": ControlScheduleFunctionalService(),
            "controlSupportSchedule": ControlScheduleSupportService(),
            "JobFunctionalService": JobFunctionalService()
        }
        return factory_dict[name]

    def job_runtime(self):
        print("[ScheduleJobSupportFactory]-[job_runtime] - START ")
        try:
            self.handle_start_notification()
            self.resultOperation = asyncio.run(self.method())
            print("... RUNNNNNNN .... ", self.data.job_name)
            self.check_job_runtime()

        except Exception as e:
            print("[ScheduleJobSupportFactory]-[job_runtime] - ERROR:: ", e)

        finally:
            print("[ScheduleJobSupportFactory]-[job_runtime] - END")

    def check_job_runtime(self):
        schedule_service = ControlScheduleSupportService()
        resultStopper = asyncio.run(schedule_service.checkScheduleIsStopped(self.data.job_name, self.data.job_instance))

        if self.resultOperation == False or resultStopper == False:
            self.scheduler.shutdown(wait=False)

    def handle_schedule_notification(self, event):
        print("[ScheduleJobSupportFactory]-[handle_schedule_notification] - START ")

        scheduler_model = ControlScheduleModel(job_name=self.data.job_name,
                                               job_instance=self.data.job_instance,
                                               job_event=MatchConstants.EVENT_SCHEDULED_INIT,
                                               job_type=self.data.job_type,
                                               job_classification=self.data.job_classification,
                                               championship=self.data.championship,
                                               start_date_execution=self.data.datetime_start,
                                               end_date_execution=self.data.datetime_end,
                                               collection_name=self.data.method_external,
                                               status=MatchConstants.STATUS_SUCCESS)

        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            scheduler_service = ControlScheduleSupportService()
            tsk = loop.create_task(scheduler_service.save(scheduler_model))
        print("[ScheduleJobSupportFactory]-[handle_schedule_notification] - END ")

    def handle_start_notification(self):
        print("[ScheduleJobSupportFactory]-[handle_start_notification] - START ")
        schedule_service = ControlScheduleSupportService()
        datetime_now = TimezoneUtil.dateNowUTC()
        time_current = TimezoneUtil.getTimeCurrent()
        register_start = False
        schedule_status = None

        if self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITH_TIME or self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITHOUT_TIME:

            if time_current == self.times_schedule_job:
                if self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITH_TIME:
                    schedule_status = MatchConstants.STATUS_SCHEDULED_DAY_ONLY_TIME
                elif self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITHOUT_TIME:
                    schedule_status = MatchConstants.STATUS_SCHEDULED_DAY_EVERY_MINUTES

                register_start = True
        elif self.data.job_classification == MatchConstants.JOB_SUPPORT_ONLINE_WITH_DATETIME:
            if datetime_now == self.data.datetime_start:
                register_start = True
                schedule_status = MatchConstants.STATUS_SCHEDULED_ONLY_DATE_TIME

        if register_start:
            try:
                loop_jb = asyncio.get_running_loop()
            except RuntimeError:
                async def funcCall():
                    await schedule_service.update_schedule_for_event_and_status(self.data.job_name,
                                                                                MatchConstants.EVENT_SCHEDULED_INIT,
                                                                                MatchConstants.EVENT_SCHEDULED_START,
                                                                                schedule_status)

                asyncio.run(funcCall())
        print("[ScheduleJobSupportFactory]-[handle_start_notification] - END ")

    def handle_error_notification(self, event):
        print("[ScheduleJobSupportFactory]-[handle_error_notification] - START ")
        schedule_service = ControlScheduleSupportService()

        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await schedule_service.update_schedule_for_event_and_status(self.data.job_name,
                                                                            MatchConstants.EVENT_SCHEDULED_START,
                                                                            MatchConstants.EVENT_SCHEDULED_FAIL,
                                                                            MatchConstants.STATUS_ERROR)
                asyncio.run(funcCall())
        print("[ScheduleJobSupportFactory]-[handle_error_notification] - END ")

    def handle_finish_notification(self, event):
        print("[ScheduleJobSupportFactory]-[handle_finish_notification] - START ")
        schedule_service = ControlScheduleSupportService()
        event_status = None
        status = None

        if self.reasonShutDown:
            event_status = MatchConstants.EVENT_SCHEDULED_STOPPED
            status = MatchConstants.STATUS_SCHEDULED_STOPPED_FAIL_FUNCTIONAL
        elif self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITH_TIME or self.data.job_classification == MatchConstants.JOB_SUPPORT_RUNTIME_WITHOUT_TIME:
            event_status = MatchConstants.EVENT_SCHEDULED_STOPPED
            status = MatchConstants.STATUS_STOPPED_MANUAL
        elif self.data.job_classification == MatchConstants.JOB_SUPPORT_ONLINE_WITH_DATETIME:
            event_status = MatchConstants.EVENT_SCHEDULED_COMPLETED
            status = MatchConstants.STATUS_EXECUTED_ON_TIME

        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await schedule_service.update_schedule_for_event_and_status(self.data.job_name,
                                                                            MatchConstants.EVENT_SCHEDULED_START,
                                                                            event_status,
                                                                            status)

            asyncio.run(funcCall())
        print("[ScheduleJobSupportFactory]-[handle_finish_notification] - END ")
