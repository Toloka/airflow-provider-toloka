from datetime import datetime, timedelta, timezone

DATA_INTERVAL_START = datetime(2021, 9, 13, tzinfo=timezone(timedelta(hours=3)))
DATA_INTERVAL_END = DATA_INTERVAL_START + timedelta(days=1)
