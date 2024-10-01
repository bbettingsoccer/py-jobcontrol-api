import datetime


class JobScheduleDto:
    job_name: str
    job_type: str
    championship: str
    date_start: datetime
    date_end: datetime
    external_class: str
    external_method: str
    interval: int

    def __init__(self, job_name: str, job_type: str, championship: str, date_start: datetime, date_end: datetime, external_class: str, external_method: str, interval: int):
        self.job_name = job_name
        self.job_type = job_type
        self.championship: championship
        self.date_start = date_start
        self.date_end = date_end
        self.external_class = external_class
        self.external_method = external_method
        self.interval = interval


