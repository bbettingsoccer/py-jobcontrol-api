from fastapi import APIRouter, Body, Query
from app.server.common.match_constants import MatchConstants
from app.server.model.job_functional_model import JobFunctionalModel
from app.server.service.job_functional_service import JobFunctionalService
from typing import List

router = APIRouter()


@router.get("/", response_description="Match retrieved")
async def getJobByAll():
    service = JobFunctionalService()
    objectL = await service.getJobAll()
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


# async def read_items(tags: List[str] = Query(None)):

@router.get("/days/")
async def getJobByStatusAndDate(days: List[str] = Query(None)):
    service = JobFunctionalService()
    values = days
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_DAY, values)
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Championship doesn't exist.")


@router.get("/championship/{championship}", response_description="Data retrieved")
async def getJobByChampionsAndStatus(championship: str):
    service = JobFunctionalService()
    values = [championship]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_CHAMPIONSHIP, values)
    print(" ROUTER ", objectL)
    if objectL:
        print(" ROUTER  2 ")
        return JobFunctionalModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/job_name/{job_name}", response_description="Data retrieved")
async def getJobByChampionsAndStatus(job_name: str):
    service = JobFunctionalService()
    values = [job_name]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_NAME, values)
    if objectL:
        return JobFunctionalModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobFunctionalModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.post("/", response_description="Data saved successfully")
async def post(data: JobFunctionalModel = Body(...)):
    service = JobFunctionalService()
    try:
        jsonObj = await service.save(data)
        return JobFunctionalModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return JobFunctionalModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.get("/schedule/planned", response_description="Data saved successfully")
async def getJobPlanning_Schedule():
    service = JobFunctionalService()
    try:
        jsonObj = await service.job_schedule_for_planned()
        return JobFunctionalModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return JobFunctionalModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.post("/schedule/datetime", response_description="Data saved successfully")
async def postJobPlanning_Date(jbc_plan: JobFunctionalModel = Body(...)):
    service = JobFunctionalService()
    try:
        jsonObj = await service.job_schedule_for_datetime(jbc_plan)
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
