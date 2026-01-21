import requests

# Replace with the Polymarket user identifier you want to query.
# Usually this is a wallet address (e.g. 0xabc...) or the platform user id.
USER = "0x1ff649450ac921cd725efd60dfbf2f5df3d749a9"
LIMIT = 1000

url = "https://data-api.polymarket.com/activity"
params = {"user": USER}

res = requests.get(url, params=params)
print(res.status_code)
print(res.text)
