import datetime
from dspace_api import dspace_api
from elasticsearch_api import elasticsearch_api

"""
This module exports filtered DSpace item metadata results to Elasticsearch

This is accomplished by:

* Connecting to DSpace via an authenticated REST API Session
* Querying for items that were uploaded on the previous day
* Parsing the results and formatting them
* Uploading the results to Elasticsearch

The following files are expected to exist in the directory
from which this module is executed:

* secrets.py  
    - Connection, Authentication, and Elasticsearch information is stored here this will need to be created/modified.

    
"""

def main():
    dapi = dspace_api()
    dapi.authenticate()
    es = elasticsearch_api()
    items = dapi.retrieve(limit=50, query_field='lastModified',
                          query_operator='like',
                          query_value=str(datetime.date.strftime(datetime.date.today() - datetime.timedelta(1), '%Y-%m-%d')) + '%',
                          expand='all')
    es.upload(itemlist=items)
    dapi.logout()

if __name__ == "__main__":
    main()