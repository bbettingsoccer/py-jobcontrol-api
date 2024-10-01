import uvicorn
from app.server.common.enviroment_conf import env_check
from app.server.service.control_job_schedule_service import ControlScheduleService
from app.server.service.job_support_service import JobSupportService
import asyncio


async def initialization_process():
    jbc_support = JobSupportService()
    ctl_schedule = ControlScheduleService()
    # JOB-SUPPORT :: Run all JOB-SUPPORT type RUNTIME
    await jbc_support.job_support_schedule_for_runtime()
    # PROCESS :: Run process relaunch JOB-FAIL
    await ctl_schedule.relaunch_schedule_jobs_with_fail()


if __name__ == '__main__':
    env_check()
    try:
        loop_jb = asyncio.get_running_loop()
    except RuntimeError:
        async def funcCall():
            await initialization_process()
        asyncio.run(funcCall())

    uvicorn.run("server.app:app", host="0.0.0.0", port=8001, reload=True)
