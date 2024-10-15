from abc import ABC, abstractmethod


class ScheduleFactory(ABC):

    @abstractmethod
    def job_instance(self):
        pass

    @abstractmethod
    def job_runtime(self):
        pass

    @abstractmethod
    def handle_schedule_notification(self, event):
        pass

    @abstractmethod
    def handle_start_notification(self):
        pass

    @abstractmethod
    def handle_error_notification(self, event):
        pass

    @abstractmethod
    def handle_finish_notification(self, event):
        pass
