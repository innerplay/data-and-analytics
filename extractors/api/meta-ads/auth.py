import requests
import json

grant_type = "fb_exchange_token"
fb_exchange_token = "EAFczYIqRQ7kBQS9NRQPjiGSdcS1r9UJ5coZBtqlpT4q7VqXZA0lPZCNyazJXQSPhKxXNp5PZA7MtFUKOwGupHpWGnRbiuTE5za8O1jiHFhKCQHqvoFvDmpb0qh8FwldWr6UEMpkVdZAExLfCJO3ywPh4dWMBbXWtIbNd4aOCsxJnwE4D5B34ilJXcomGxY96MR7W3sfLR"
client_id = "24544812708479929"
client_secret = "7fd71d2f70a26f0667273634d2e3ad95"
access_token = "EAFczYIqRQ7kBQRZAU8AmFUgml3ktGWuD8jgQPWAcd2rRusrwMFTG9AnUzk1dg1FJz9rZBV2M1H7L3D0Y6AAHOoacZBbj62xyZAqFqDRiofQZCWaWUkeWZBViZAzrpGV2ZC7bnNzWg8SZCmQ5tVjliby3ZBuhZAvbLhefyE5GE8J2hgZAokhx1COyNdbsxbHhEyYsx6iRRZCVX0EvfoZAYAZBGcF5BQgLz0x60aIEFPf0Ms7KY7VPIwZD"

url = f"https://graph.facebook.com/v23.0/oauth/access_token?grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&fb_exchange_token={fb_exchange_token}"
url_1 = f"https://graph.facebook.com/v21.0/me/accounts?access_token={access_token}"

response = requests.get(url).json()
response_1 = requests.get(url_1).json()

with open('long_lived_token.txt', 'w') as f:
    f.write(json.dumps(response))

with open('page_token.txt', 'w') as f:
    f.write(json.dumps(response_1))