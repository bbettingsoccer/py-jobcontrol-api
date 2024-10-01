from _datetime import datetime, date
from pydantic import BaseModel, Field, constr, conint, create_model
from typing import List


class JobFunctionalModel(BaseModel):
    job_name: constr(strict=True) = Field(...)
    job_type: constr(strict=True) = Field(...)
    scrapy: constr(strict=True) = Field(...)
    url: constr(strict=True) = Field(...)
    championship: constr(strict=True) = Field(...)
    country_execute: constr(strict=True) = Field(...)
    interval: conint(strict=True) = Field(...)
    week_days: List[str] = Field(...)
    timezones_execute: constr(strict=True) = Field(...)
    time_start: constr(strict=True) = Field(...)
    time_end: constr(strict=True) = Field(...)
    collection_name: constr(strict=True) = Field(...)
    date_start: date = Field(repr=False, frozen=True)
    date_end: date = Field(repr=False, frozen=True)
    status: constr(strict=True) = Field(...)

    class config:
        job_name = "job_name"
        job_type = "job_type"
        scrapy = "scrapy"
        url = "url"
        championship = "championship"
        country_execute = "country_execute"
        interval = "interval"
        week_days = "week_days"
        date_start = "date_start"
        date_end = "date_end"
        timezones_execute = "timezones_execute"
        time_start = "time_start"
        time_end = "time_end"
        collection_name = "collection_name"
        status = "status"

    @classmethod
    def as_optional(cls):
        annonations = cls.__fields__
        OptionalModel = create_model(
            f"Optional{cls.__name__}",
            __base__=JobFunctionalModel,
            **{
                k: (v.annotation, None) for k, v in JobFunctionalModel.model_fields.items()
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
            "scrapy": str(jobControl["scrapy"]),
            "url": str(jobControl["url"]),
            "championship": str(jobControl["championship"]),
            "country_execute": str(jobControl["country_execute"]),
            "interval": jobControl["interval"],
            "week_days": jobControl["week_days"],
            "timezones_execute": str(jobControl["timezones_execute"]),
            "time_start": str(jobControl["time_start"]),
            "time_end": str(jobControl["time_end"]),
            "collection_name": str(jobControl["collection_name"]),
            "date_start": datetime.strptime(jobControl["date_start"], '%Y-%m-%d'),
            "date_end": datetime.strptime(jobControl["date_end"], '%Y-%m-%d'),
            "status": str(jobControl["status"])
        }
