from splunk_http_event_collector import http_event_collector 
import random
import json

def commitCrime():

    # list of sample values
    suspects = ['Miss Scarett','Professor Plum','Miss Peacock','Mr. Green','Colonel Mustard','Mrs. White']
    weapons = ['candlestick','knife','lead pipe','revolver','rope','wrench']
    rooms = ['kitchen','ballroom','conservatory','dining room','cellar','billiard room','library','lounge','hall','study']

    return {"killer":random.choice(suspects), "weapon":random.choice(weapons), "location":random.choice(rooms), "victim":"Mr Boddy"}

# Create event collector object, default SSL and HTTP Event Collector Port
http_event_collector_key = "DB84F19F-B2F1-4B89-BB38-643DFB641B34"
http_event_collector_host = "45.55.161.5"

testevent = http_event_collector(http_event_collector_key, http_event_collector_host, input_type='json',host="",http_event_port='8088',http_event_server_ssl=True, http_event_collector_debug=True)

# Start event payload and add the metadata information
payload = {}
payload.update({"index":"http-test"})
payload.update({"sourcetype":"crime"})
payload.update({"source":"witness"})
payload.update({"host":"mansion"})

# Report 5 Crimes
for i in range(5):
    event = commitCrime()
    event.update({"action":"success"})
    event.update({"crime_type":"single"})
    event.update({"crime_number":i})
    payload.update({"event":event})
    

