import os
from apscheduler.events import EVENT_SCHEDULER_STARTED, EVENT_SCHEDULER_SHUTDOWN, EVENT_JOB_ERROR
from apscheduler.schedulers.background import BackgroundScheduler
from app.server.common.match_constants import MatchConstants
from app.server.common.timezone_util import TimezoneUtil
from app.server.dto.job_schedule_dto import JobScheduleDto
from app.server.model.control_schedule_model import ControlScheduleModel
import app.server.service.control_job_schedule_service
from app.server.schedule.schedule_factory import ScheduleFactory
import asyncio


class ScheduleJobFunctionalFactory(ScheduleFactory):
    scheduler = None
    data = None
    api_url = None
    collection_name = None
    response = None

    def __init__(self, job_schedule_dto: JobScheduleDto):
        super().__init__()
        self.data = job_schedule_dto
        self.scheduler = BackgroundScheduler()
        self.api_url = os.getenv('SOCCER_SCRAPY_API')

    def job_instance(self):
        print("...job_instance...")
        try:
            # (1) Schedule Job Process
            self.scheduler.add_listener(self.handle_schedule_notification, EVENT_SCHEDULER_STARTED)
            self.scheduler.add_listener(self.handle_finish_notification, EVENT_SCHEDULER_SHUTDOWN)
            self.scheduler.add_listener(self.handle_error_notification, EVENT_JOB_ERROR)
            self.scheduler.add_job(self.job_runtime,
                                   "interval",
                                   minutes=self.data.interval,
                                   start_date=self.data.date_start,
                                   end_date=self.data.date_end,
                                   id=self.data.job_name)
            self.scheduler.start()
        except Exception as e:
            print("[Error :: ] - job_instance", e)

    def job_runtime(self):
        print("...job_runtime.... ")
        try:
            # Save JOB for first execution
            self.handle_start_notification()
            url = self.api_url + self.collection_name
            # self.response = requests.get(url)
            print(" RUNNING NOW :)")

            self.check_job_runtime()
            """
            if self.response.status_code == 200:
                self.check_job_runtime()
            else:
                self.scheduler.e
            """
        except Exception as e:
            print("[Error] - job_runtime", e)

    def check_job_runtime(self):
        print("check_job_runtime ")
        dateNow = TimezoneUtil.dateNowUTC()

        if self.scheduler.running:
            if dateNow >= self.data.date_end:
                self.scheduler.shutdown(wait=False)
        else:
            self.scheduler.shutdown(wait=False)

    def handle_schedule_notification(self, event):
        print("Execute :: handle_job_schedule ")
        scheduler_service = app.server.service.control_job_schedule_service.ControlScheduleService()
        scheduler_model = ControlScheduleModel(job_name=self.data.job_name,
                                               job_event=MatchConstants.EVENT_SCHEDULED_INIT,
                                               championship=self.data.championship,
                                               start_date_execution=self.data.date_start,
                                               end_date_execution=self.data.date_end,
                                               collection_name=self.data.external_method,
                                               status=MatchConstants.STATUS_SUCCESS)

        loop = asyncio.get_running_loop()
        if loop and loop.is_running():
            print('Async event loop already running. Adding coroutine to the event loop.')
            tsk = loop.create_task(scheduler_service.save(scheduler_model))
            tsk.add_done_callback(lambda fut: print(f"Task {fut.result()}"))
        else:
            asyncio.run(scheduler_service.save(scheduler_model))

    def handle_start_notification(self):
        print("Execute :: handle_job_start")
        """
        date_now = TimezoneUtil.dateNowUTC()
        date_time = TimezoneUtil.getDateTimeForSearchMongo(self.data.date_start)

        if date_now == self.data.date_start:
            execution_service = ControlExecutionService()
            schedule_service = ControlScheduleService()
            execution_model = ControlExecutionModel(job_name=self.data.job_name,
                                                    job_event=MatchConstants.EVENT_JOB_START,
                                                    date_event=datetime.datetime.now(),
                                                    championship=self.data.championship,
                                                    msg_info=MatchConstants.MSG_JOB_INF_START,
                                                    status=MatchConstants.STATUS_SUCCESS)
            try:
                loop_jb = asyncio.get_running_loop()
            except RuntimeError:
                async def funcCall():
                    await execution_service.save(execution_model)
                    await schedule_service.update_for_schedule_event_init(self.data.job_name,
                                                                          MatchConstants.EVENT_SCHEDULED_INIT,
                                                                          MatchConstants.EVENT_SCHEDULED_START,
                                                                          MatchConstants.STATUS_SUCCESS,
                                                                          date_time)

                asyncio.run(funcCall())
            else:
                if loop_jb and loop_jb.is_running():
                    print('Async event loop already running. Adding coroutine to the event loop.')
                    tsk = loop_jb.create_task(execution_service.save(execution_model))
                    tsk.add_done_callback(lambda job_end: print(f"Task {job_end.result()}"))
        """

    def handle_error_notification(self, event):
        print("Execute :: handle_job_end")
        """
        date_time = TimezoneUtil.getDateTimeForSearchMongo(self.data.date_start)
        execution_service = ControlExecutionService()
        schedule_service = ControlScheduleService()
        execution_model = ControlExecutionModel(job_name=self.data.job_name,
                                                job_event=MatchConstants.EVENT_JOB_ERROR_END,
                                                date_event=datetime.datetime.now(),
                                                championship=self.data.championship,
                                                msg_info=MatchConstants.MSG_JOB_INF_ERROR,
                                                status=MatchConstants.STATUS_ERROR)
        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await execution_service.save(execution_model)
                await schedule_service.update_for_schedule_event_init(self.data.job_name,
                                                                      MatchConstants.EVENT_SCHEDULED_START,
                                                                      MatchConstants.EVENT_SCHEDULED_FAIL,
                                                                      MatchConstants.STATUS_ERROR,
                                                                      date_time)

            asyncio.run(funcCall())
        """

    def handle_finish_notification(self, event):
        print("Execute :: handle_finish_notification")
        """
        date_time = TimezoneUtil.getDateTimeForSearchMongo(self.data.date_start)
        schedule_service = ControlScheduleService()
        execution_service = ControlExecutionService()
        execution_model = ControlExecutionModel(job_name=self.data.job_name,
                                                job_event=MatchConstants.EVENT_JOB_END,
                                                date_event=datetime.datetime.now(),
                                                championship=self.data.championship,
                                                msg_info=MatchConstants.MSG_JOB_INF_SUCCESS,
                                                status=MatchConstants.STATUS_SUCCESS)
        try:
            loop_jb = asyncio.get_running_loop()
        except RuntimeError:
            async def funcCall():
                await execution_service.save(execution_model)
                await schedule_service.update_for_schedule_event_init(self.data.job_name,
                                                                      MatchConstants.EVENT_SCHEDULED_START,
                                                                      MatchConstants.EVENT_SCHEDULED_END,
                                                                      MatchConstants.STATUS_SUCCESS,
                                                                      date_time)

            asyncio.run(funcCall())
        """
