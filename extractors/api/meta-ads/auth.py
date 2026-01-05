import requests
import json

url = "https://graph.facebook.com/v23.0/oauth/access_token?grant_type=fb_exchange_token&client_id=24544812708479929&client_secret=7fd71d2f70a26f0667273634d2e3ad95&fb_exchange_token=EAFczYIqRQ7kBP7OC87jJeHWwjzZB52wUCZBOIL5n1WZC4IfZBBiqNtMibZBHoI2GMGE0Fg7neteYtQxYUZBht3DhaXMkhQcTVYtgYwJ2GWCZANZCy3BFoTjzlUIce92He3v4LruQH3LxmKz82BS0R8dkozHYef0AwVZB6O1oYZBTjx72ZCWVZC9ot1RfxoZCSrb7VU1yoj8xi9s2ZA"

response = requests.get(url).json()

with open('long_lived_token.txt', 'w') as f:
    f.write(json.dumps(response))