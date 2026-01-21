# Python
import datetime
import time

# Define the UTC start and end times as strings
start_utc_str = "2025-11-03 05:00:00"
end_utc_str = "2025-12-01 04:59:00"

# Convert strings to datetime objects (UTC)
start_dt = datetime.datetime.strptime(start_utc_str, "%Y-%m-%d %H:%M:%S")
end_dt = datetime.datetime.strptime(end_utc_str, "%Y-%m-%d %H:%M:%S")

# Convert datetime objects to Unix timestamps
start_ts = int(start_dt.replace(tzinfo=datetime.timezone.utc).timestamp())
end_ts = int(end_dt.replace(tzinfo=datetime.timezone.utc).timestamp())

print("Start Unix Timestamp:", start_ts)
print("End Unix Timestamp:", end_ts)