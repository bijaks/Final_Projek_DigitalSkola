import requests
import pandas as pd
import logging

class Dataload():
    def __init__(self,url):
        self.url = url
    
    def get_data(self):
        response = requests.get(self.url)
        result = response.json()['data']['content']
        logging.info("GET DATA DARI API BERHASIL")
        df = pd.json_normalize(result)
        logging.info("DATA DARI API KE DATAFRAME TELAH SIAP")
        return df