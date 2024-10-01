from fastapi import APIRouter, Body
from app.server.common.match_constants import MatchConstants
from app.server.model.control_execution_model import ControlExecutionModel
from app.server.service.control_execution_service import ControlExecutionService

router = APIRouter()


@router.get("/", response_description="Match retrieved")
async def getJobControlExecuteByAll():
    service = ControlExecutionService()
    objectL = await service.getJobsForAll()
    if objectL:
        return ControlExecutionModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/championship/{championship}/date_event/{date_event}/status/{status}")
async def getJobByChampionshipAndDateAndStatu(championship: str, date_event: str, status: str):
    service = ControlExecutionService()
    values = [championship, date_event, status]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_CHAMPIONSHIP_DATE_STATUS, values)
    if objectL:
        return ControlExecutionModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 404, "Jobs doesn't exist.")


@router.get("/status/{status}/date_event/{date_event}/")
async def getJobByStatusAndDate(status: str, date_event: str):
    service = ControlExecutionService()
    values = [status, date_event]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_STATUS_DATE, values)
    if objectL:
        return ControlExecutionModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 404, "Championship doesn't exist.")


@router.get("/championship/{championship}", response_description="Data retrieved")
async def getJobByChampionsAndStatus(championship: str):
    service = ControlExecutionService()
    values = [championship]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_CHAMPIONSHIP, values)
    print(" ROUTER execution ",  objectL)
    if objectL:
        return ControlExecutionModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/job_name/{job_name}", response_description="Data retrieved")
async def getJobByChampionsAndStatus(job_name: str):
    service = ControlExecutionService()
    values = [job_name]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_NAME, values)
    if objectL:
        return ControlExecutionModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.post("/", response_description="Data saved successfully")
async def post(data: ControlExecutionModel = Body(...)):
    service = ControlExecutionService()
    try:
        jsonObj = await service.save(data)
        return ControlExecutionModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return ControlExecutionModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.delete("/job_name/{job_name}", response_description="Data deleted from the database")
async def deleteJobForId(job_name: str):
    service = ControlExecutionService()
    values = [job_name]
    data = await service.deleteJobForCondition(MatchConstants.DELETE_JOB_NAME, values)
    if data:
        return ControlExecutionModel.ResponseModel("DELETE", "delete for date successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 500, "Job doesn't exist.")


@router.delete("/date_event/{date_event}", response_description="Data deleted from the database")
async def deleteJobForDateInc(date_event: str):
    service = ControlExecutionService()
    values = [date_event]
    data = await service.deleteJobForCondition(MatchConstants.DELETE_DATE_EVENT, values)
    if data:
        return ControlExecutionModel.ResponseModel("DELETE", "delete for date successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 500, "Job doesn't exist.")


@router.delete("/status/{status}", response_description="Data deleted from the database")
async def deleteJobForStatus(status: str):
    service = ControlExecutionService()
    values = [status]
    data = await service.deleteJobForCondition(MatchConstants.DELETE_STATUS, values)
    if data:
        return ControlExecutionModel.ResponseModel("DELETE", "delete for date successfully")
    return ControlExecutionModel.ErrorResponseModel("An error occurred.", 500, "Job doesn't exist.")
