from fastapi import APIRouter, Body
from app.server.common.match_constants import MatchConstants
from app.server.model.control_schedule_model import ControlScheduleModel
from app.server.model.job_support_model import JobSupportModel
from app.server.service.control_schedule_support_service import ControlScheduleSupportService
from app.server.service.job_support_service import JobSupportService

router = APIRouter()


@router.get("/support/all", response_description="Match retrieved")
async def getJobSupportByAll():
    service = JobSupportService()
    objectL = await service.getJobAll()
    if objectL:
        return JobSupportModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return JobSupportModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/schedule/all", response_description="Match retrieved")
async def getControlScheduleSupportByAll():
    service = ControlScheduleSupportService()
    objectL = await service.getJobAll()
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Jobs data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/schedule/{job_name}/{job_instance}/{event}")
async def getScheduleSupportByNameAndInstanceAndEvent(name_job: str, job_instance: str, event1: str, event2: str):
    service = ControlScheduleSupportService()
    values = [name_job, event1, event2, job_instance]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_NAME_INSTANCE_EVENT, values)
    if objectL:
        return ControlScheduleModel.ResponseModel(objectL, "Data retrieved successfully")
    return ControlScheduleModel.ErrorResponseModel("An error occurred.", 404, "Championship doesn't exist.")


@router.get("/support/{job_classification_1}/{job_classification_2}", response_description="Data retrieved")
async def getJobByClassification(job_classification_1: str, job_classification_2):
    service = JobSupportService()
    values = [job_classification_1, job_classification_2]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_CLASSIFICATION, values)
    if objectL:
        return JobSupportModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobSupportModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.get("/support/{job_name}", response_description="Data retrieved")
async def getJobByChampionsAndStatus(job_name: str):
    service = JobSupportService()
    values = [job_name]
    objectL = await service.getJobForCondition(MatchConstants.GET_JOB_BY_NAME, values)
    if objectL:
        return JobSupportModel.ResponseModel(objectL, "Data retrieved successfully")
    return JobSupportModel.ErrorResponseModel("An error occurred.", 404, "Job doesn't exist.")


@router.post("/support", response_description="Data saved successfully")
async def postJobSupport(data: JobSupportModel = Body(...)):
    service = JobSupportService()
    try:
        jsonObj = await service.save(data)
        return JobSupportModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return JobSupportModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.post("/support/plan", response_description="Data saved successfully")
async def postPlanningJobSupport_Now(data: JobSupportModel = Body(...)):
    service = JobSupportService()
    try:
        jsonObj = await service.planning_schedule_jobs_support_by_online(data)
        return JobSupportModel.ResponseModel(jsonObj, "Match data retrieved successfully")
    except Exception as e:
        return JobSupportModel.ErrorResponseModel("Error occurred.", 500, "Match data doesn't exist.")


@router.delete("/support/{job_name}", response_description="Data deleted from the database")
async def deleteJobSupport(job_name: str):
    service = JobSupportService()
    values = [job_name]
    data = await service.deleteJobForCondition(MatchConstants.DELETE_JOB_NAME, values)
    if data:
        return JobSupportModel.ResponseModel("DELETE", "delete for date successfully")
    return JobSupportModel.ErrorResponseModel("An error occurred.", 500, "Job doesn't exist.")


@router.put("/{id}", response_description="Data update the database")
async def put(id: str, req: JobSupportModel.as_optional() = Body(...)):
    # req = {k: v for k, v in req.dict().items() if v is not None}
    service = JobSupportService()
    up_currentMatch = await service.update(id, req)
    if up_currentMatch:
        return JobSupportModel.ResponseModel("Update Transaction".format(id), "Success")
    return JobSupportModel.ErrorResponseModel("Error", 404, "Error update transaction")


@router.get("/support/cancel/{job_name}", response_description="Cancel Job Instance in Execution")
async def cancel_job_execute(job_name: str):
    service = JobSupportService()
    result_request = await service.cancelJobSupport(job_name)
    if result_request:
        return JobSupportModel.ResponseModel("Cancel Job Success".format(id), "Success")
    return JobSupportModel.ErrorResponseModel("Error", 404, "Error Cancel transaction")



@router.get("/schedule/cancel/{job_name}/{job_instance}", response_description="Cancel Job Instance in Execution")
async def cancel_instance_schedule(job_name: str, job_instance: str):
    service = ControlScheduleSupportService()
    result_request = await service.cancelInstanceSchedule(job_name, job_instance)
    if result_request:
        return JobSupportModel.ResponseModel("Cancel Job Success".format(id), "Success")
    return JobSupportModel.ErrorResponseModel("Error", 404, "Error Cancel transaction")
