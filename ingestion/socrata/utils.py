import datetime
from typing import Optional


def date_interval(
    start: Optional[str] = None,
    stop: Optional[str] = None,
    delta: Optional[dict] = None,
):
    default_weeks = 0
    default_days = 1
    if not delta:
        delta = {"weeks": default_weeks, "days": default_days}

    if start and stop:
        return {"start": start, "stop": stop}

    fmt = "%Y-%m-%d"

    delta_weeks = delta.get("weeks", default_weeks)
    delta_days = delta.get("days", default_days)
    delta = datetime.timedelta(weeks=delta_weeks, days=delta_days)
    # print(delta)

    if not start and not stop:
        today = datetime.datetime.today()
        stop = today.strftime(fmt)
        return {"start": (today - delta).strftime(fmt), "stop": stop}
    if not start:
        stop_date = datetime.datetime.strptime(stop, fmt)
        return {"start": (stop_date - delta).strftime(fmt), "stop": stop}
    if not stop:
        start_date = datetime.datetime.strptime(start, fmt)
        return {"start": start, "stop": (start_date + delta).strftime(fmt)}


if __name__ == "__main__":
    delta = {"weeks": 2, "days": 7}
    print("\ndate_interval (no start, no stop, no delta)")
    print(date_interval())

    print(f"\ndate_interval (no start, no stop, delta {delta})")
    print(date_interval(delta=delta))

    print(f"\ndate_interval (no start, stop, delta {delta})")
    print(date_interval(stop="2024-06-15", delta=delta))

    print(f"\ndate_interval (no stop, stop, delta {delta})")
    print(date_interval(start="2024-06-15", delta=delta))

    print(f"\ndate_interval (start, stop, delta {delta})")
    print(date_interval(start="2024-04-01", stop="2024-05-30"))
