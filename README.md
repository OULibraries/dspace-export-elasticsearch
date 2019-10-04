# dspace-elasticsearch
_Export Filtered Items and Metadata to Elasticsearch_
---

This module exports filtered DSpace item metadata results to Elasticsearch

---
**Requires:**

REST API Access to DSpace
* Elevated permissions may be needed to view embargoed items/bitstreams. 

Python 3.6+

pip

Python Packages
* [requests](https://requests.kennethreitz.org/en/master/)

  - _"Requests allows you to send HTTP/1.1 requests extremely easily. There’s no need to manually add query strings to your URLs, or to form-encode your POST data. Keep-alive and HTTP connection pooling are 100% automatic, thanks to urllib3."_


* [dateutil](https://dateutil.readthedocs.io/en/stable/)

  - _"The dateutil module provides powerful extensions to the standard datetime module, available in Python."_

---
This is accomplished by:

* Connecting to DSpace via an authenticated REST API Session
* Querying for items that were uploaded on the previous day
* Parsing the results and formatting them
* Uploading the results to Elasticsearch

The following files are expected to exist in the directory
from which this module is executed:

* secrets.py  
    - Connection, Authentication, and Elasticsearch information is stored here this will need to be created/modified.

---

Example Usage:

* Update secrets.py with local fields
```python
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
    items = dapi.retrieve(limit=50, query_field='dc.date.accessioned',
                          query_operator='like',
                          query_value=str(datetime.date.strftime(datetime.date.today() - datetime.timedelta(1), '%Y-%m-%d')) + '%',
                          expand='all')
    es.upload(itemlist=items)
    dapi.logout()

if __name__ == "__main__":
    main()
```

License
-------

[MIT](https://github.com/OULibraries/dspace-export-elasticsearch/blob/master/LICENSE)
