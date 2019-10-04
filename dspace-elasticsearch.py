import requests
import time
import json
import urllib3
import secrets
import dateutil.parser
import datetime

# Supplimentary secrets.py file containing assignments
baseURL = secrets.baseURL
email = secrets.email
password = secrets.password
verify = secrets.verify
es_host = secrets.es_host
es_index = secrets.es_index

# No SSL Verification Warning, bad practice but only takes effect is verify = False in secrets.py
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# authentication
startTime = time.time()
data = {'email': email, 'password': password}
header = {'content-type': 'application/json', 'accept': 'application/json'}
session = requests.post(baseURL + '/rest/login', headers=header, verify=verify,
                        params=data).cookies['JSESSIONID']
cookies = {'JSESSIONID': session}


status = requests.get(baseURL + '/rest/status', headers=header,
                      cookies=cookies, verify=verify).json()
userFullName = status['fullname']
print('authenticated', userFullName)


print("Get items")
# Filter out items, currently set to just get yesterdays items, as well as a batch size of 50
filter = {'limit': 50, 'offset': 0,
          'query_field[]': 'dc.date.accessioned', 'query_op[]': 'like',
          'query_val[]': str(datetime.date.strftime(datetime.date.today() - datetime.timedelta(1), '%Y-%m-%d')) + '%',
          'expand': 'all'}

print("Applying item filter", filter)
items = ''
itemlist = []
# Get items in batches of 50
while items != []:
    items = requests.get(baseURL
                         + "/rest/filtered-items", params=filter, headers=header,
                         cookies=cookies, verify=verify)
    # If we are too aggressive, backoff for 5 seconds
    while items.status_code != 200:
        time.sleep(5)
        items = requests.get(baseURL
                             + '/rest/filtered-items', params=filter, headers=header,
                             cookies=cookies, verify=verify)
    filter['offset'] += filter['limit']
    print("At offset: ", filter['offset'])
    items = items.json()['items']
    
    print("Items in this pass: ", len(items))

    # Loop through retrieved batch
    for item in items:
        current_record = {}
        policies = []
        embargo_list = []
        current_record['uuid'] = item['uuid']
        current_record['parentCollection'] = item['parentCollection']['name']
        current_record['parentCommunityList'] = item['parentCommunityList'][0]['name']
        bitstreams = item['bitstreams']
        # Loop through this items associated bitstreams and add relevant data to record
        for bitstream in range(0, len(bitstreams)):
            time.sleep(0.125)
            policies.append(requests.get(baseURL + bitstreams[bitstream]['link'] + "/policy", headers=header, cookies=cookies,
                                         verify=verify).json())
            for index in range(0, len(policies)):
                for policy in policies[index]:
                    for key, value in policy.items():
                        current_record['policies.policy' + str(index) + "." + str(key)] = value
                        if key == 'startDate' and value is not None and value != 'null':
                            for metadata in item['metadata']:
                                if metadata['key'] == 'dc.date.accessioned':
                                    dcAccessioned = datetime.datetime.strptime(datetime.datetime.strftime(
                                        datetime.datetime.strptime(metadata['value'], "%Y-%m-%dT%H:%M:%S%z"), "%Y-%m-%d"), "%Y-%m-%d")
                                    etime = datetime.datetime.strptime(datetime.datetime.strftime(
                                        datetime.datetime.strptime(value, "%Y-%m-%d"), "%Y-%m-%d"), "%Y-%m-%d")
                                    embargo_list.append(
                                        abs((dcAccessioned - etime).days))
        # If there are associated embargo's calculate average duration for this item
        if(len(embargo_list) > 0):
            current_record['embargoDuration'] = sum(embargo_list) / len(embargo_list)
        # Loop through item metadata and store Key-Value pairs as record data
        for metadata in item['metadata']:
            if metadata['key'].startswith("dc.date") and metadata['key'] != 'dc.date.accessioned':
                # One-off user input correction, ideally this wouldn't be needed
                if metadata['value'] == '20018-7':
                    metadata['value'] = '2018-7'
                if metadata['value'] == '0022-08-01':
                    metadata['value'] = '2022-08-01'
                metadata['value'] = str(dateutil.parser.parse(metadata['value']))

            if metadata['key'] == 'dc.rights' or metadata['key'] == 'dc.identifier' or metadata['key'] == 'dc.type' or metadata['key'] == 'dc.subject':
                current_record[metadata['key'] + '.base'] = metadata['value']
            else:
                current_record[metadata['key']] = metadata['value']
        # Add record to item list that will be looped through and sent to Elasticsearch
        itemlist.append(json.dumps(current_record))


print("Number of Records: ", len(itemlist))
# Loop through all retrieved items and send to configured Elasticsearch, creating a new document for each record
for record in itemlist:
    print(record)
    m = requests.post(es_host+ "/"+es_index+"/_doc",
                      headers={"Content-Type": "application/json"}, data=record)
    print(m.text)
