from abc import ABC
from apscheduler.events import EVENT_SCHEDULER_STARTED, EVENT_SCHEDULER_SHUTDOWN, EVENT_JOB_ERROR
from apscheduler.schedulers.background import BackgroundScheduler

from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.schedule.schedule_factory import ScheduleFactory
import asyncio
from app.server.service.control_execution_service import *
from app.server.service.control_schedule_functional_service import *


class ScheduleJobFunctionalFactory(ScheduleFactory, ABC):
    scheduler = None
    data = None
    # api_url = None
    collection_name = None
    response = None
    reasonShutDown = False

    def __init__(self, job_schedule_dto: JobScheduleDto):
        super().__init__()
        self.data = job_schedule_dto
        self.scheduler = BackgroundScheduler()
        self.api_url = self.data.class_external + self.data.method_external

    def job_instance(self):
        print("[ScheduleJobFunctionalFactory]-[job_instance] - START ")
        try:
            # (1) Schedule Job Process
            self.scheduler.add_listener(self.handle_schedule_notification, EVENT_SCHEDULER_STARTED)
            self.scheduler.add_listener(self.handle_finish_notification, EVENT_SCHEDULER_SHUTDOWN)
            self.scheduler.add_listener(self.handle_error_notification, EVENT_JOB_ERROR)
            self.scheduler.add_job(self.job_runtime,
                                   "interval",
                                   minutes=self.data.interval,
                                   start_date=self.data.datetime_start,
                                   end_date=self.data.datetime_end,
                                   id=self.data.job_name)
            self.scheduler.start()
            return True
        except Exception as e:
            print("[ScheduleJobFunctionalFactory]-[job_instance] - ERROR :: ", e)
            return False

    def job_runtime(self):
        print("[ScheduleJobFunctionalFactory]-[job_runtime] - START ")

        try:
            # Save JOB for first execution
            self.handle_start_notification()
            self.check_job_runtime()

            print(" URL CALL :)", self.api_url)

            # self.response = requests.get(url)
            print(" RUNNING NOW :)", self.data.job_name)

            """
            if self.response.status_code == 200:
                self.check_job_runtime()
            else:
                self.scheduler.e
            """
            print("[ScheduleJobFunctionalFactory]-[job_runtime] - END ")

        except Exception as e:
            print("[ScheduleJobFunctionalFactory]-[job_runtime] - ERROR :: ", e)

    def check_job_runtime(self):
        print("[ScheduleJobFunctionalFactory]-[check_job_runtime] - START ")
        scheduler_service = ControlScheduleFunctionalService()
        dateNow = TimezoneUtil.dateNowUTC()

        """ 
        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await scheduler_service.checkExistJobScheduleEventStopped(self.data.job_name,
                                                                    self.data.job_instance,
                                                                    self.data.datetime_start,
                                                                MatchConstants.EVENT_SCHEDULED_STOPPED)
        """
        result = asyncio.run(scheduler_service.checkScheduleIsStopped(self.data.job_name, self.data.job_instance))

        print(" _______result________ ", result)

        if dateNow >= self.data.datetime_end or result:
            self.reasonShutDown = True
            self.scheduler.shutdown(wait=False)

        print("[ScheduleJobFunctionalFactory]-[check_job_runtime] - END ")

    def handle_schedule_notification(self, event):
        print("[ScheduleJobFunctionalFactory]-[handle_schedule_notification] - START ")
        scheduler_service = ControlScheduleFunctionalService()
        scheduler_model = ControlScheduleModel(job_name=self.data.job_name,
                                               job_instance=self.data.job_instance,
                                               job_type=self.data.job_type,
                                               job_event=MatchConstants.EVENT_SCHEDULED_INIT,
                                               job_classification=self.data.job_classification,
                                               championship=self.data.championship,
                                               start_date_execution=self.data.datetime_start,
                                               end_date_execution=self.data.datetime_end,
                                               collection_name=self.data.method_external,
                                               status=MatchConstants.STATUS_SUCCESS)

        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            print('Async event loop already running. Adding coroutine to the event loop.')
            tsk = loop.create_task(scheduler_service.save(scheduler_model))
            tsk.add_done_callback(lambda fut: print(f"Task {fut.result()}"))
        else:
            asyncio.run(scheduler_service.save(scheduler_model))
        print("[ScheduleJobFunctionalFactory]-[handle_schedule_notification] - END ")

    def handle_start_notification(self):
        print("[ScheduleJobFunctionalFactory]-[handle_start_notification] - START ")

        date_now = TimezoneUtil.dateNowUTC()

        if date_now == self.data.datetime_start:
            execution_service = ControlExecutionService()
            schedule_service = ControlScheduleFunctionalService()
            execution_model = ControlExecutionModel(job_name=self.data.job_name,
                                                    job_event=MatchConstants.EVENT_JOB_START,
                                                    date_event=date_now,
                                                    championship=self.data.championship,
                                                    msg_info=MatchConstants.MSG_JOB_INF_START,
                                                    status=MatchConstants.STATUS_SUCCESS)
            try:
                loop_jb = asyncio.get_running_loop()
            except RuntimeError:
                async def funcCall():
                    await execution_service.save(execution_model)
                    await schedule_service.update_schedule_for_event_and_status(self.data.job_name,
                                                                                self.data.job_instance,
                                                                                MatchConstants.EVENT_SCHEDULED_START,
                                                                                MatchConstants.STATUS_SUCCESS)

                asyncio.run(funcCall())
            else:
                if loop_jb and loop_jb.is_running():
                    print('Async event loop already running. Adding coroutine to the event loop.')
                    tsk = loop_jb.create_task(execution_service.save(execution_model))
                    tsk.add_done_callback(lambda job_end: print(f"Task {job_end.result()}"))

            print("[ScheduleJobFunctionalFactory]-[handle_start_notification] - END ")

    def handle_error_notification(self, event):
        print("[ScheduleJobFunctionalFactory]-[handle_error_notification] - START ")
        date_time = TimezoneUtil.getDateTimeForSearchMongo(self.data.datetime_start)
        date_now = TimezoneUtil.dateNowUTC()
        execution_service = ControlExecutionService()
        schedule_service = ControlScheduleFunctionalService()
        execution_model = ControlExecutionModel(job_name=self.data.job_name,
                                                job_event=MatchConstants.EVENT_JOB_ERROR_END,
                                                date_event=date_now,
                                                championship=self.data.championship,
                                                msg_info=MatchConstants.MSG_JOB_INF_ERROR,
                                                status=MatchConstants.STATUS_ERROR)
        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await execution_service.save(execution_model)
                await schedule_service.update_schedule_for_event_and_status(self.data.job_name,
                                                                            self.data.job_instance,
                                                                            MatchConstants.EVENT_SCHEDULED_FAIL,
                                                                            MatchConstants.STATUS_ERROR)

            asyncio.run(funcCall())
        print("[ScheduleJobFunctionalFactory]-[handle_error_notification] - END ")

    def handle_finish_notification(self, event):
        print("[ScheduleJobFunctionalFactory]-[handle_finish_notification] - START ")
        date_time = TimezoneUtil.getDateTimeForSearchMongo(self.data.datetime_start)
        date_now = TimezoneUtil.dateNowUTC()
        reasonFinish = None
        if self.reasonShutDown:
            reasonFinish = MatchConstants.STATUS_STOPPER_INSTANCE_OR_DATETIME
        else:
            reasonFinish = MatchConstants.EVENT_JOB_END

        schedule_service = ControlScheduleFunctionalService()
        execution_service = ControlExecutionService()
        execution_model = ControlExecutionModel(job_name=self.data.job_name,
                                                job_event=MatchConstants.EVENT_JOB_END,
                                                date_event=date_now,
                                                championship=self.data.championship,
                                                msg_info=MatchConstants.MSG_JOB_INF_SUCCESS,
                                                status=MatchConstants.STATUS_SUCCESS)
        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await execution_service.save(execution_model)
                await schedule_service.update_schedule_for_event_and_status(self.data.job_name,
                                                                            self.data.job_instance,
                                                                            MatchConstants.EVENT_SCHEDULED_END,
                                                                            reasonFinish)
            asyncio.run(funcCall())
        print("[ScheduleJobFunctionalFactory]-[handle_finish_notification] - END ")
