from typing import Final
from pytz import timezone

HKT: Final["tz"] = timezone("Asia/Hong_Kong")


def str_to_datetime(date_str: str) -> "datetime":
    from dateutil.parser import parse

    return parse(date_str)


def utc_to_hkt(
        utc_date_str: str,
) -> str:
    utc_date = str_to_datetime(utc_date_str)
    hkt_date = utc_date.astimezone(HKT).date()
    return str(hkt_date)


def udm_utc_to_hkt(utc_date_str: str) -> str:
    return utc_to_hkt(utc_date_str)
