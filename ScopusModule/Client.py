# -*- coding: utf-8 -*-
"""
Created on Mon May 17 2021

@author: Nicolas
"""

import requests
import json
import time
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class Client():
    """Clase que implementa una interfaz de Python para api.elsevier.com"""

    # class variables
    __min_req_interval = 1  # Min. request interval in sec
    __ts_last_req = time.time()  # Tracker for throttlingF

    def __init__(self, api_key, auth_token=None, inst_token=None):
        """Inicializa un cliente dada una API key y, opcionalmente, insttoken 
        y authtoken."""
        self.api_key = api_key
        self.auth_token = auth_token
        self.inst_token = inst_token
        
    def exec_request(self, URL):
        # Throttle request, if need be
        interval = time.time() - self.__ts_last_req
        if (interval < self.__min_req_interval):
            time.sleep(self.__min_req_interval - interval)

        # Construcción y ejecución de la petición
        headers = {
            "X-ELS-APIKey": self.api_key,
            "Accept": 'application/json'
        }

        if self.auth_token:
            headers["X-ELS-Authtoken"] = self.auth_token
        if self.inst_token:
            headers["X-ELS-Insttoken"] = self.inst_token


        session = requests.Session()
        retry = Retry(connect=10, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
            
        r = session.get(
            URL,
            headers=headers
        )
        self.__ts_last_req = time.time()
        self._status_code = r.status_code
        if r.status_code == 200:
            self._status_msg = 'datos recuperados'
            return json.loads(r.text)
        else:
            self._status_msg = "HTTP " + str(r.status_code) + " Error desde: " + \
                URL + " y usando los hearders: " + str(headers) + ": " + r.text
            raise requests.HTTPError("HTTP " + str(r.status_code) + " Error desde: " +
                                     URL + "\ny usando los hearders: " + str(headers) + ":\n" + r.text)
