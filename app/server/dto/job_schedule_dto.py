
class JobScheduleDto:
    job_name: str
    job_type: str
    job_classification: str
    job_instance: str
    championship: str
    datetime_start: str
    datetime_end: str
    class_external: str
    method_external: str
    interval: int

    def __init__(self, job_name: str, job_type: str, job_classification: str, job_instance: str, championship: str, datetime_start: str, datetime_end: str, class_external: str, method_external: str, interval: int):
        self.job_name = job_name
        self.job_type = job_type
        self.job_classification = job_classification
        self.job_instance = job_instance
        self.championship = championship
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self.class_external = class_external
        self.method_external = method_external
        self.interval = interval


