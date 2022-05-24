#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2022 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
#
# This file is part of tc_etl_lib
#
# tc_etl_lib is free software: you can redistribute it and/or
# modify it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# tc_etl_lib is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
# General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with IoT orchestrator. If not, see http://www.gnu.org/licenses/.

'''
ContextBroker routines for Python:
  - cbManager.send_batch
  - cbManager.get_entities_page
'''
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Any, Optional

import logging
import time

from . import authManager


logger = logging.getLogger(__name__)

class FetchError(Exception):
    """
    FetchError encapsulates all parameters of an HTTP request and the erroneous response
    """

    response: requests.Response
    method: str
    url: str
    params: Optional[Any] = None
    headers: Optional[Any] = None
    body: Optional[Any] = None

    def __str__(self) -> str:
        return f"Failed to {self.method} {self.url} (headers: {self.headers}, params: {self.params}, body: {self.body}): [{self.response.status_code}] {self.response.text}"


class cbManager:
    """ContextBroker Manager
    
    endpoint: define service endpoint cb (example: https://<service>:<port>)
    timeout: timeout in seconds (default: 10)
    post_retry_connect: number of retries (defaul: 3)
    post_retry_backoff_factor: retry factor delay -> {backoff factor} * (2 ** ({number of total retries} - 1)) (defaul: 20). (ref. urllib3 docs) (default: False)
    sleep_send_batch: sleep X seconds afters send update batch. (default: 0). 
    cb_flowcontrol: Opción del Context Broker, que permite un mejor rendimiento en caso de envío masivo de datos (batch updates). Este mecanismo, requiere arrancar el Context Broker con un flag concreto y en las peticiones de envío de datos, añadir esa opción. Referencia en Fiware Orion Docs (default: False)
    """
    
    endpoint: str
    timeout: int = 10
    post_retry_connect: int = 3
    post_retry_backoff_factor: int = 20
    sleep_send_batch: int = 0
    cb_flowcontrol: bool = False
    
    def __init__(self,*, endpoint: str = None, timeout: int = 10, post_retry_connect: int = 3, post_retry_backoff_factor: int = 20, sleep_send_batch: int = 0, cb_flowcontrol: bool = False) -> None:
        
        if (endpoint == None):
            raise ValueError(f'You must define <<endpoint>> in cbManager')
        
        self.endpoint = endpoint
        self.timeout = timeout
        self.post_retry_connect = post_retry_connect
        self.post_retry_backoff_factor = post_retry_backoff_factor
        self.sleep_send_batch = sleep_send_batch
        self.cb_flowcontrol = cb_flowcontrol 

    def get_entities_page(self, *, subservice: str = None, auth: authManager = None, offset: int = None, limit: int = None, type: str = None, orderBy: str = None):
        """Retrieve data from context broker

        :param subservice: Define subservice from which entities are retrieved, defaults to None
        :param auth: Define authManager, defaults to None
        :param offset: Establishes the offset from where entities are retrieved, defaults to None
        :param limit: Limits the number of entities to be retrieved, defaults to None
        :param type: Retrieve entities whose type matches one of the elements in the list, defaults to None
        :param orderBy: Criteria for ordering results, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises FetchError: is thrown when the response from the cb indicates an error
        :return: json data
        """
        if (auth == None):
            raise ValueError('You must define a authManager')

        if (not hasattr(auth,"tokens")):
            auth.tokens = {}

        if (subservice == None):
            if (not hasattr(auth,"subservice")):
                raise ValueError('You must define <<subservice>> in authManager')
            else:
                subservice = auth.subservice
    
        if subservice not in auth.tokens.keys():
            auth.get_auth_token_subservice(subservice = subservice)
        
        headers = {
            'Fiware-Service': auth.service,
            'Fiware-ServicePath': subservice,
            'X-Auth-Token': auth.tokens[subservice]
        }
        
        params = {"offset": offset, "limit": limit, "type": type, "orderBy": orderBy}
        req_url = f"{self.endpoint}/v2/entities"
        resp = requests.get(req_url, params=params, headers=headers, verify=False, timeout=self.timeout)
        
        if resp.status_code < 200 or resp.status_code > 204:
            raise FetchError(response=resp, method="GET", url=req_url, params=params, headers=headers)

        return resp.json()

    def send_batch(self, *, subservice: str = None, auth: authManager, entities: str) -> bool:
        """Send batch data to context broker

        :param auth: Define authManager 
        :param entities: Entities data
        :param subservice: Define subservice to send batch data, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises Exception: is thrown when the cotext broker response isn't ok operation
        :return: True if the operation is correct
        """
        
        if (auth == None):
            raise ValueError('You must define a authManager')
        
        if (not hasattr(auth,"tokens")):
            auth.tokens = {}

        if (subservice == None):
            if (not hasattr(auth,"subservice")):
                raise ValueError('You must define <<subservice>> in authManager')
            else:
                subservice = auth.subservice            
            
        if subservice not in auth.tokens.keys():
            auth.get_auth_token_subservice(subservice = subservice)

        res = self.__batch_creation_update(auth=auth, subservice = subservice, entities=entities)
        if res.status_code == 401:
            auth.get_auth_token_subservice(subservice = subservice)
            res = self.__batch_creation_update(auth=auth, subservice = subservice, entities=entities)

        if res.status_code != 204:
            raise Exception(f'Error in batch operation ({res.status_code}): {res.json()}')

        logger.debug(f'- Update batch of {len(entities)} entities')

        if (self.sleep_send_batch != 0):
            time.sleep(self.sleep_send_batch)
            
        return True 
        
    def __batch_creation_update(self, *, subservice: str = None, auth: authManager = None, entities: str):
        """Send batch data to Context Broker

        :param entities: Entities data
        :param subservice: Define subservice to send batch data, defaults to None
        :param auth: Define authManager, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :return: Http response code
        """
        if (auth == None):
            raise ValueError('You must define a authManager')
        
        if (subservice == None):
            if (not hasattr(auth,"subservice")):
                raise ValueError('You must define <<subservice>> in authManager')
            else:
                subservice = auth.subservice   
        
        if (not hasattr(self,"endpoint")):
            raise ValueError('You must define <<endpoint>> in cbManager')

        headers = {
            'Fiware-Service': auth.service,
            'Fiware-ServicePath': subservice,
            'X-Auth-Token': auth.tokens[subservice],
            'Content-Type': 'application/json'
        }

        body = {
            'actionType': 'append',
            'entities': entities
        }

        if (self.cb_flowcontrol):
            req_url = f'{self.endpoint}/v2/op/update?options=flowControl'
        else:
            req_url = f'{self.endpoint}/v2/op/update'
    
        http = requests.Session()
        retry_strategy = Retry(
            total=self.post_retry_connect,
            read=self.post_retry_connect,
            backoff_factor=self.post_retry_backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            method_whitelist=('HEAD', 'GET', 'OPTIONS', 'POST')
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        http.mount('http://', adapter)
        http.mount('https://', adapter)
        return http.post(req_url, json=body, headers=headers, verify=False, timeout=self.timeout)
