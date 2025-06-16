import re
import uuid
from datetime import datetime
from typing import List


def remove_hex_from_string(str):
    """Remove hex value from input string"""
    return re.sub(r"[^\x00-\x7f]", r"", str)


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


def _split_dates_gen(test_date1, test_date2, N):

    diff = (test_date2 - test_date1) / N
    for idx in range(N):

        # using generator function to solve problem
        # returns intermediate result
        yield (test_date1 + diff * idx)
    yield test_date2


def split_dates(test_date1, test_date2, N):
    """Split dates in N equally distanced date ranges in list
    date obj e.g.:
    test_date1 = datetime.datetime(1997, 1, 1)"""
    temp = list(_split_dates_gen(test_date1, test_date2, N))

    # using strftime to convert to userfriendly format
    res = []
    for sub in temp:
        res.append(sub.strftime("%Y-%m-%d %H:%M:%S"))

    return res


def get_dates_from_string(text: str) -> List[datetime]:
    """Extract dates from a given string and return datetime objects in a list"""
    matches = re.findall(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text)
    for i, match in enumerate(matches):
        matches[i] = datetime.strptime(match, "%Y-%m-%d %H:%M:%S")
    return matches


def replace_dates_in_string(text: str, dates: List) -> str:
    new = text
    for i, match in enumerate(re.finditer(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", text)):
        new = "".join((new[: match.start()], dates[i], new[match.end() :]))
    return new


def sanitize_filename(filename: str) -> str:
    """Replace illegal filename characters with underscores."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)
