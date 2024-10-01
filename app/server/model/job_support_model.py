from pydantic import BaseModel, Field, constr, conint, create_model, condate
from typing import List
from _datetime import datetime, date


class JobSupportModel(BaseModel):
    job_name: constr(strict=True) = Field(...)
    job_type: constr(strict=True) = Field(...)
    description: constr(strict=True) = Field(...)
    date_start: date = Field(repr=False, frozen=True)
    date_end: date = Field(repr=False, frozen=True)
    interval: conint(strict=True) = Field(...)
    method_external: constr(strict=True) = Field(...)
    class_external: constr(strict=True) = Field(...)
    timezones_execute: constr(strict=True) = Field(...)
    time_start: constr(strict=True) = Field(...)
    time_end: constr(strict=True) = Field(...)
    status: constr(strict=True) = Field(...)

    class config:
        job_name = "job_name"
        job_type = "job_type"
        description = "description"
        date_start = "date_start"
        date_end = "date_end"
        interval = "interval"
        class_external = "class_external"
        method_external = "method_external"
        timezones_execute = "timezones_execute"
        time_start = "time_start"
        time_end = "time_end"
        status = "status"

    @classmethod
    def as_optional(cls):
        annonations = cls.__fields__
        OptionalModel = create_model(
            f"Optional{cls.__name__}",
            __base__=JobSupportModel,
            **{
                k: (v.annotation, None) for k, v in JobSupportModel.model_fields.items()
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
            "job_type": str(jobControl["job_type"]),
            "description": str(jobControl["description"]),
            "date_start": jobControl["date_start"],
            "date_end": jobControl["date_end"],
            "interval": jobControl["interval"],
            "class_external": jobControl["class_external"],
            "method_external": jobControl["method_external"],
            "call_method": str(jobControl["call_method"]),
            "timezones_execute": str(jobControl["timezones_execute"]),
            "time_start": str(jobControl["time_start"]),
            "time_end": str(jobControl["time_end"]),
            "status": str(jobControl["status"])
        }
