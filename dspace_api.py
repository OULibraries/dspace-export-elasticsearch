import requests
import time
import json
import urllib3
import secrets
import dateutil.parser
import datetime

"""
This object contains functions to connect to a DSpace REST API endpoint as well as retrieve items using specified filters

The following functions exist:
authenticate:
    - This function creates an authenticated DSpace REST API Session
retrieve:
    - Retrieve a list of parsed and formatted items based on filters

"""

class dspace_api:
    def __init__(self):
        """Object defintions:
              - baseURL:
                - DSpace URL stored in a secrets file, secrets.py,
                  this will need to be created/modified.
              - email:
                - Email to use for authentication, stored in a secrets file, secrets.py,
                  this will need to be created/modified.
              - password:
                - Password used for authentication, stored in a secrets file, secrets.py,
                  this will need to be created/modified.
              - verify:
                - SSL verification flag, stored in a secrets file, secrets.py,
                  this will need to be created/modified.
        """

        self.baseURL = secrets.baseURL
        self.email = secrets.email
        self.password = secrets.password
        self.verify = secrets.verify
        # No SSL Verification Warning, bad practice but only takes effect is verify = False in secrets.py
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def authenticate(self):
        """This function creates an authenticated DSpace REST API Session

            Keyword arguments:
            - None
        
            Returns:
            - None
        """
        # authentication
        self.data = {'email': self.email, 'password': self.password}
        self.header = {'content-type': 'application/json', 'accept': 'application/json'}
        self.session = requests.post(self.baseURL + '/rest/login', headers=self.header, verify=self.verify,
                                params=self.data).cookies['JSESSIONID']
        self.cookies = {'JSESSIONID': self.session}


        status = requests.get(self.baseURL + '/rest/status', headers=self.header,
                            cookies=self.cookies, verify=self.verify).json()
        if status['authenticated'] == False:
            print('Authentication Failed')
            exit()

        userFullName = status['fullname']
        print('authenticated', userFullName)
    
    def logout(self):
        """This function logs out of an authenticated DSpace REST API Session

        Keyword arguments:
        - None
        
        Returns:
        - None
        """
        requests.post(self.baseURL + '/rest/logout', headers=self.header, verify=self.verify,
                                cookies=self.cookies)
        status = requests.get(self.baseURL + '/rest/status', headers=self.header,
                            cookies=self.cookies, verify=self.verify).json()
        print('Logged out: ', status['okay'])

    def retrieve(self, limit=50, query_field='dc.title', query_operator='exists', query_value='', expand='all'):
        """Retrieve a list of parsed and formatted items

        Keyword arguments:
        - limit 
            - the batch size to return for each item query pass (default 50)
        - query_field
            - the field in which you are filtering against (default dc.title)
        - query_operator
            - the operation you are using to for matched fields (defualt exists)
        - query_value
            - the value you are filtering on (default '')
        - expand
            - extra fields to return (default all)

        Returns:
        - itemlist
            - A list containing parsed JSON item records 
        """

        print("Get items")
        # Filter out items, currently set to just get yesterdays items, as well as a batch size of 50
        filter = {'limit': limit, 'offset': 0,
                'query_field[]': query_field, 'query_op[]': query_operator,
                'query_val[]': query_value,
                'expand': expand}

        print("Applying item filter", filter)
        items = ''
        itemlist = []
        # Get items in batches of 50
        while items != []:
            items = requests.get(self.baseURL
                                + "/rest/filtered-items", params=filter, headers=self.header,
                                cookies=self.cookies, verify=self.verify)
            # If we are too aggressive, backoff for 5 seconds
            while items.status_code != 200:
                time.sleep(5)
                items = requests.get(self.baseURL
                                    + '/rest/filtered-items', params=filter, headers=self.header,
                                    cookies=self.cookies, verify=self.verify)
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
                current_record['lastModified'] = item['lastModified']
                current_record['parentCollection'] = item['parentCollection']['name']
                current_record['parentCommunityList'] = item['parentCommunityList'][0]['name']
                bitstreams = item['bitstreams']
                # Loop through this items associated bitstreams and add relevant data to record
                for bitstream in range(0, len(bitstreams)):
                    time.sleep(0.125)
                    policies.append(requests.get(self.baseURL + bitstreams[bitstream]['link'] + "/policy", headers=self.header, cookies=self.cookies,
                                                verify=self.verify).json())
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
                    if (metadata['key'].startswith("dc.date") and metadata['key'] != 'dc.date.accessioned') or metadata['key'] == 'lastModified':
                        # One-off user input correction, ideally this wouldn't be needed
                        if metadata['value'] == '20018-7':
                            metadata['value'] = '2018-7'
                        if metadata['value'] == '0022-08-01':
                            metadata['value'] = '2022-08-01'
                        metadata['value'] = str(dateutil.parser.parse(metadata['value']))

                    if len(metadata['key'].split(".")) < 3:
                        current_record[metadata['key'] + '.text'] = metadata['value']
                    else:
                        current_record[metadata['key']] = metadata['value']
                # Add record to item list that will be looped through and sent to Elasticsearch
                itemlist.append(json.dumps(current_record))
        return itemlist
