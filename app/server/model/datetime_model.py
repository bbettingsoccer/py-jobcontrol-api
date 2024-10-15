from _datetime import datetime, date


class DatetimeModel:
    use_datetime: str
    date_start: date
    date_end: date
    time_start: str
    time_end: str
    interval: int
    timezones_execute: str

    class config:
        use_datetime = "use_datetime"
        date_start = "date_start"
        date_end = "date_end"
        time_start = "time_start"
        time_end = "time_end"
        interval = "interval"
        timezones_execute = "timezones_execute"
