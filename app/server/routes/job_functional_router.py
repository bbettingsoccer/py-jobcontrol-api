from fastapi import APIRouter, Body, Query
from app.server.common.match_constants import MatchConstants
from app.server.model.control_schedule_model import ControlScheduleModel
from app.server.model.job_functional_model import JobFunctionalModel
from app.server.service.control_schedule_functional_service import ControlScheduleFunctionalService
from app.server.service.job_functional_service import JobFunctionalService
from typing import List

router = APIRouter()


@router.get("/functional/all", response_description="Match retrieved")
async def getJobFunctionalByAll():
    service = JobFunctionalService()
    objectL = await service.getJobAll()
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/schedule/all", response_description="Match retrieved")
async def getControlScheduleFunctionalByAll():
    service = ControlScheduleFunctionalService()
    objectL = await service.getJobAll()
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/functional/days/")
async def getJobByStatusAndDate(days: List[str] = Query(None)):
    service = JobFunctionalService()
    values = days
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_DAY_EXECUTION, values)
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Championship doesn't exist.")


@router.get("/schedule/{job_name}/{job_instance}/{event}")
async def getScheduleByNameAndInstanceAndEvent(name_job: str, job_instance: str, event1: str, event2: str):
    service = ControlScheduleFunctionalService()
    values = [name_job, event1, event2, job_instance]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_NAME_INSTANCE_EVENT, values)
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Championship doesn't exist.")


@router.get("/functional/championship/{championship}", response_description="Data retrieved")
async def getJobByChampionship(championship: str):
    service = JobFunctionalService()
    values = [championship]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_CHAMPIONSHIP, values)
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/functional/job_name/{job_name}", response_description="Data retrieved")
async def getJobByName(job_name: str):
    service = JobFunctionalService()
    values = [job_name]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_NAME, values)
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.post("/functional", response_description="Data saved successfully")
async def postJobFunctional(data: JobFunctionalModel = Body(...)):
    service = JobFunctionalService()
    try:
        jsonObj = await service.save(data)
        return JobFunctionalModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return JobFunctionalModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.post("/functional/plan", response_description="Data saved successfully")
async def postPlanningJobFunctional_Now(jbc_plan: JobFunctionalModel = Body(...)):
    service = JobFunctionalService()
    try:
        jsonObj = await service.planning_schedule_jobs_functional_by_online(jbc_plan)
        return JobFunctionalModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return JobFunctionalModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.delete("/job_name/{job_name}", response_description="Data deleted from the database")
async def deleteJobForId(job_name: str):
    service = JobFunctionalService()
    values = [job_name]
    data = await service.deleteJobForCondition(MatchConstants.DELETE_JOB_NAME, values)
    if data:
        return JobFunctionalModel.ResponseModel("DELETE", "delete for date successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 500, "Job doesn't exist.")


@router.put("/{id}", response_description="Data update the database")
async def put(id: str, req: JobFunctionalModel.as_optional() = Body(...)):
    # req = {k: v for k, v in req.dict().items() if v is not None}
    service = JobFunctionalService()
    up_currentMatch = await service.update(id, req)
    if up_currentMatch:
        return JobFunctionalModel.ResponseModel("Update Transaction".format(id), "Success")
    return JobFunctionalModel.ErrorResponseModel("Error", 404, "Error update transaction")
