from _datetime import datetime, date, timedelta
import pytz
import tzlocal
from app.server.common.match_constants import MatchConstants


class TimezoneUtil:

    def convertDateTimeOrigenToDateTimeLocal(self, date_start: date, date_end: date, time_start: str, time_end: str,
                                             timezones_execute: str) -> dict:
        timezone_current = tzlocal.get_localzone_name()
        dateTimeLocalTimezone = None

        if timezones_execute != timezone_current:
            dateTimeObj = self.getDateTimeTogether(date_start, date_end, time_start, time_end)
            dateTimeLocalTimezone = self.getDateTimeConvertFromTimezoneLocal(dateTimeObj["datetime_start"],
                                                                             dateTimeObj["datetime_end"],
                                                                             timezones_execute)
        elif timezones_execute == timezone_current:
            dateTimeLocalTimezone = self.getDateTimeTogether(date_start, date_end, time_start, time_end)

        return dateTimeLocalTimezone


    @staticmethod
    def getDateTimeFromTimezone(timezone_from: str) -> date:
        # GET DateTime in Timezone Origen
        tz_origen = pytz.timezone(timezone_from)
        dateTimeLocal = datetime.now()
        dateTimeZona = dateTimeLocal.astimezone(tz_origen)
        return dateTimeZona

    @staticmethod
    def getDateTimeTogether(date_start: date, date_end: date, time_start: str, time_end: str) -> dict:
        date_result = {}
        dtStart_Str = str(date_start)
        dtEnd_Str = str(date_end)
        dateTimeStart = dtStart_Str + " " + time_start
        dateTimeEnd = dtEnd_Str + " " + time_end
        dateStart_obj = datetime.strptime(dateTimeStart, "%Y-%m-%d %H:%M:%S")
        dateEnd_obj = datetime.strptime(dateTimeEnd, "%Y-%m-%d %H:%M:%S")
        date_result = {"datetime_start": dateStart_obj, "datetime_end": dateEnd_obj}
        return date_result

    @staticmethod
    def getDateTimeTogetherOne(time_start: str) -> dict:
        dtStart_Str = str(date.today())
        dateTimeStart = dtStart_Str + " " + time_start
        dateStart_obj = datetime.strptime(dateTimeStart, "%Y-%m-%d %H:%M:%S")
        return dateStart_obj

    @staticmethod
    def getTimeConvertFromTimezoneLocal(time_start: str, timezone_from: str):

        data_now = str(date.today())
        dataTime_str = data_now + ' ' + time_start
        dateTime_start = datetime.strptime(dataTime_str, '%Y-%m-%d %H:%M:%S')

        # SET TIMEZONE BASED IN DATE FROM/ORIGIN
        timezone_From = pytz.timezone(timezone_from)
        dateStart_From = timezone_From.localize(dateTime_start)

        # SET TIMEZONE (SYSTEM-LOCAL) BASED IN DATE TO/DESTINATION
        timezone_current = tzlocal.get_localzone_name()
        timezone_To = pytz.timezone(timezone_current)

        # CONVERT DATE/TIME TO DATE/TIME LOCAL-SYSTEM/DATE
        dateStart_To = dateStart_From.astimezone(timezone_To)
        time = dateStart_To.strftime('%H:%M:%S')
        return time

    @staticmethod
    def getTimeFromDateTime(dateTime_Str: str):
        time_t = dateTime_Str.strftime('%H:%M:%S')
        times_schedule = time_t.split(":")
        return times_schedule
    @staticmethod
    def getTimezoneCurrent():
        return tzlocal.get_localzone()

    @staticmethod
    def getTimeCurrentMoreInterval(minutes: int):
        now = datetime.now()
        current_time = now + timedelta(minutes=minutes)
        time_t = current_time.strftime('%H:%M:%S')
        return time_t

    @staticmethod
    def confirmDayWeek(dateTomorrow: date, week: str) -> bool:
        date_obj = datetime.strptime(dateTomorrow, '%Y-%m-%d')
        weekday_name = date_obj.strftime('%A')  # e.g., 'Monday'
        if weekday_name == week:
            return True
        else:
            return False

    @staticmethod
    def getDateTimeConvertFromTimezoneLocal(dateTime_start: datetime, dateTime_end: datetime,
                                            timezone_from: str) -> dict:
        dateTimeObj = {}
        # SET TIMEZONE BASED IN DATE FROM/ORIGIN
        timezone_From = pytz.timezone(timezone_from)
        dateStart_From = timezone_From.localize(dateTime_start)
        dateEnd_From = timezone_From.localize(dateTime_end)

        # SET TIMEZONE (SYSTEM-LOCAL) BASED IN DATE TO/DESTINATION
        timezone_current = tzlocal.get_localzone_name()
        timezone_To = pytz.timezone(timezone_current)

        # CONVERT DATE/TIME TO DATE/TIME LOCAL-SYSTEM/DATE
        dateStart_To = dateStart_From.astimezone(timezone_To)
        dateEnd_To = dateEnd_From.astimezone(timezone_To)

        dateStart_To = dateStart_To.strftime('%Y-%m-%d %H:%M:%S')
        dateEnd_To = dateEnd_To.strftime('%Y-%m-%d %H:%M:%S')

        dateTimeObj["datetime_start"] = dateStart_To
        dateTimeObj["datetime_end"] = dateEnd_To
        return dateTimeObj

    @staticmethod
    def dateNowUTC() -> datetime:
        dt = datetime.now()
        formatted_dt = dt.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_dt

    @staticmethod
    def getTimeCurrent() -> str:
        dt = datetime.now()
        time_current = dt.strftime('%H:%M:%S')
        return time_current

    @staticmethod
    def getDateTimeUTC360() -> datetime:
        dt = datetime.now()
        dateTime360 = dt + timedelta(days=1)
        dateTime360_f = dateTime360.strftime('%Y-%m-%d %H:%M:%S')
        return dateTime360_f

    @staticmethod
    def dateToday() -> datetime:
        return date.today()

    @staticmethod
    def getDateFormat(day_type: str) -> date:
        day = date.today()
        day_format = None
        match day_type:
            case MatchConstants.DAY_TODAY:
                day_format = day.strftime('%Y-%m-%d')
            case MatchConstants.DAY_TOMORROW:
                tomorrow = day + timedelta(days=1)
                day_format = tomorrow.strftime('%Y-%m-%d')
            case MatchConstants.DAY_360:
                tomorrow = day + timedelta(days=360)
                day_format = tomorrow.strftime('%Y-%m-%d')
        return day_format

    @staticmethod
    def getTimezones() -> dict:
        new_dict = {}
        timezone_country = {}
        for countrycode in pytz.country_timezones:
            timezones = pytz.country_timezones[countrycode]
            new_dict["countrycode"] = countrycode
            new_dict["timezones"] = timezones
            for timezone in timezones:
                timezone_country[timezone] = countrycode
        return new_dict

    @staticmethod
    def getDateTimeForSearchMongo(dateTimeChange: datetime) -> datetime:
        date_time = datetime.strptime(dateTimeChange, "%Y-%m-%d %H:%M:%S")
        date_time = date_time.strftime("%Y-%m-%dT%H:%M:%S")
        return date_time
