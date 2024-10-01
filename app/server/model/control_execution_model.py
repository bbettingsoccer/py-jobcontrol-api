from pydantic import BaseModel, Field, constr, conint, create_model
from _datetime import datetime, date, timedelta


class ControlExecutionModel(BaseModel):
    job_name: constr(strict=True) = Field(..., alias='job_name')
    job_event: constr(strict=True) = Field(..., alias='job_event')
    date_event: datetime = None
    championship: constr(strict=True) = Field(..., alias='championship')
    msg_info: constr(strict=True) = Field(..., alias='msg_info')
    status: constr(strict=True) = Field(..., alias='status')



    class config:
        job_name = "job_name"
        job_event = "job_event"
        date_event = "date_event"
        championship = "championship"
        msg_info = "msgInfo"
        status = "status"

    @classmethod
    def as_optional(cls):
        annonations = cls.__fields__
        OptionalModel = create_model(
            f"Optional{cls.__name__}",
            __base__=ControlExecutionModel,
            **{
                k: (v.annotation, None) for k, v in ControlExecutionModel.model_fields.items()
            })
        return OptionalModel

    def ResponseModel(data, message):
        return {
            "data": [data],
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
            "date_event": jobControl["date_event"],
            "championship": str(jobControl["championship"]),
            "msg_info": str(jobControl["msg_info"]),
            "status": str(jobControl["status"])
        }
