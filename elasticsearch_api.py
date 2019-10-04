import requests
import secrets

"""
This object contains functions to connect to an Elasticsearch REST API endpoint as well as upload parsed records

The following functions exist:

upload:
    - This function loops through a provided list of JSON records and uploads them to an Elasticsearch instance

"""

class elasticsearch_api:
    def __init__(self):
        """
        Object defintions:
              es_host:
                - Elasticsearch URL, stored in a secrets file, secrets.py,
                  this will need to be created/modified.
              es_index:
                - Elasticsearch Index to upload documents to, stored in a secrets file, secrets.py,
                  this will need to be created/modified.

        """
        self.es_host = secrets.es_host
        self.es_index = secrets.es_index

    def upload(self, itemlist):
        """This function loops through a provided list of JSON Records and uploads them to an Elasticsearch Index

        Parameters:
        itemlist -- List of JSON Records

        Returns:
        None
        """

        print("Number of Records: ", len(itemlist))
        # Loop through all retrieved items and send to configured Elasticsearch, creating a new document for each record
        for record in itemlist:
            print(record)
            m = requests.post(self.es_host+ "/"+self.es_index+"/_doc",
                            headers={"Content-Type": "application/json"}, data=record)
            print(m.text)