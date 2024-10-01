from fastapi import APIRouter, Body
from app.server.common.match_constants import MatchConstants
from app.server.model.control_schedule_model import ControlScheduleModel
from app.server.service.control_job_schedule_service import ControlScheduleService

router = APIRouter()


@router.get("/", response_description="Match retrieved")
async def getJobControlByAll():
    service = ControlScheduleService()
    objectL = await service.getJobAll()
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


# async def read_items(tags: List[str] = Query(None)):


@router.get("/championship/{championship}", response_description="Data retrieved")
async def getJobControlByChampionsAndStatus(championship: str):
    service = ControlScheduleService()
    values = [championship]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_CHAMPIONSHIP, values)
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/job_name/{job_name}", response_description="Data retrieved")
async def getJobControlPlainByChampionsAndStatus(job_name: str):
    service = ControlScheduleService()
    values = [job_name]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_NAME, values)
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.post("/", response_description="Data saved successfully")
async def post(data: ControlScheduleModel = Body(...)):
    service = ControlScheduleService()
    try:
        jsonObj = await service.save(data)
        return ControlScheduleModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return ControlScheduleModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.delete("/job_name/{job_name}", response_description="Data deleted from the database")
async def deleteJobForId(job_name: str):
    service = ControlScheduleService()
    values = [job_name]
    data = await service.deleteJobForCondition(MatchConstants.DELETE_JOB_NAME, values)
    if data:
        return ControlScheduleModel.ResponseModel("DELETE", "delete for date successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 500, "Job doesn't exist.")
