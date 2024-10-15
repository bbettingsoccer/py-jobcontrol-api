import uvicorn
from app.server.common.enviroment_conf import env_check
from app.server.service.job_support_service import JobSupportService
import asyncio


async def initialization_process():
    jbc_support = JobSupportService()
    await jbc_support.delete_all_schedule_job_support()
    await jbc_support.planning_schedule_jobs_support_by_runtime()
    await jbc_support.planning_schedule_jobs_support_by_datetime()


if __name__ == '__main__':
    env_check()

    try:
        loop_jb = asyncio.get_running_loop()
        tsk = loop_jb.create_task(initialization_process())
        tsk.add_done_callback(lambda job_end: print(f"Task {job_end.result()}"))
    except RuntimeError as e:
        async def funcCall():
            await initialization_process()


        asyncio.run(funcCall())
    """
    try:
        loop_jb = asyncio.get_running_loop()
    except RuntimeError as e:
        print(" Except", e)
        async def funcCall():
            await initialization_process()
        asyncio.run(funcCall())
    """

    uvicorn.run("server.app:app", host="0.0.0.0", port=8001, reload=True)
