from pydantic import BaseModel, Field, constr, conint, create_model, condate
from _datetime import datetime, date

from app.server.model.datetime_model import DatetimeModel


class JobSupportModel(BaseModel):
    job_name: constr(strict=True) = Field(...)
    job_type: constr(strict=True) = Field(...)
    job_classification: constr(strict=True) = Field(...)
    function: constr(strict=True) = Field(...)
    datetime_use: dict = Field(default={}, extra={})
    method_external: constr(strict=True) = Field(...)
    class_external: constr(strict=True) = Field(...)
    status: constr(strict=True) = Field(...)

    class config:
        job_name = "job_name"
        job_type = "job_type"
        job_classification = "job_classification"
        function = "function"
        datetime_use = "datetime_use"
        class_external = "class_external"
        method_external = "method_external"
        status = "status"
        use_datetime = "use_datetime"
        date_start = "date_start"
        date_end = "date_end"
        time_start = "time_start"
        time_end = "time_end"
        interval = "interval"
        timezones_execute = "timezones_execute"

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
            "job_classification": str(jobControl["job_classification"]),
            "function": str(jobControl["function"]),
            "datetime_use": jobControl["datetime_use"],
            "class_external": jobControl["class_external"],
            "method_external": jobControl["method_external"],
            "status": str(jobControl["status"])
        }
