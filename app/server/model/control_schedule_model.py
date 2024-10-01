from pydantic import BaseModel, Field, constr, condate, create_model, condate
from _datetime import datetime, date, timedelta


class ControlScheduleModel(BaseModel):
    job_name: constr(strict=True) = Field(..., alias='job_name')
    job_event: constr(strict=True) = Field(..., alias='job_event')
    championship: constr(strict=True) = Field(..., alias='championship')
    start_date_execution: datetime = None
    end_date_execution: datetime = None
    collection_name: constr(strict=True) = Field(..., alias='collection_name')
    status: constr(strict=True) = Field(..., alias='status')

    class config:
        job_name = "job_name"
        job_event = "job_event"
        championship = "championship"
        start_date_execution = "start_date_execution"
        end_date_execution = "end_date_execution"
        collection_name = "collection_name"
        status = "status"

    @classmethod
    def as_optional(cls):
        annonations = cls.__fields__
        OptionalModel = create_model(
            f"Optional{cls.__name__}",
            __base__=ControlScheduleModel,
            **{
                k: (v.annotation, None) for k, v in ControlScheduleModel.model_fields.items()
            })
        return OptionalModel

    def ResponseModel(data, message):
        return {
            "data": data,
            "code": 200,
            "message": message,
        }

    def ErrorResponseModel(error, code, message):
        return {"error": error, "code": code, "message": message}

    @staticmethod
    def data_helper(jobControl) -> dict:
        return {
            "_id": str(jobControl['_id']),
            "job_name": str(jobControl["job_name"]),
            "job_event": str(jobControl["job_event"]),
            "championship": str(jobControl["championship"]),
            "start_date_execution": jobControl["start_date_execution"],
            "end_date_execution": jobControl["end_date_execution"],
            "collection_name": str(jobControl["collection_name"]),
            "status": str(jobControl["status"])
        }
