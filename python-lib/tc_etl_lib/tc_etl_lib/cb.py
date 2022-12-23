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
  - cbManager.get_entities
  - cbManager.delete_entities
'''
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from typing import Iterable, List, Any, Optional

import logging
import time
import json

from . import authManager

# control urllib3 post and get verify in false
import urllib3, urllib3.exceptions
urllib3.disable_warnings(category=urllib3.exceptions.InsecureRequestWarning)

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

    def __init__(self, response: requests.Response, method: str, url: str, params: Optional[Any] = None, headers: Optional[Any] = None, body: Optional[Any] = None):
        """Constructor for FetchError class"""
        self.response = response
        self.method = method
        self.url = url
        self.params = params
        self.headers = headers
        self.body = body

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
    block_size: maximum size per batch, in bytes. Default is 800kb and it is not recommended to change.
    batch_size: maximum size per batch, in entities. Default is 0 (no limitiation, other than block_size).
    """

    endpoint: str
    timeout: int = 10
    post_retry_connect: int = 3
    post_retry_backoff_factor: int = 20
    sleep_send_batch: int = 0
    cb_flowcontrol: bool = False
    # Block size is string chars (= bytes, with ASCII codification). This is calculated
    # for 800k (Orion max request size is 1MB,
    # see https://fiware-orion.readthedocs.io/en/master/user/known_limitations.html)
    # and it is not recommended to change it
    block_size = 800000
    # Batch size is entities. Ignored when batch_size = 0.
    # If > 0 , limits the number of entities per batch.
    # We might want to limit the amount of entities per batch for
    # reasons other than block size (e.g to avoid triggering too
    # many subscriptions per batch and stressing cygnus queues)
    batch_size = 0

    def __init__(self,*, endpoint: Optional[str] = None, timeout: int = 10, post_retry_connect: int = 3, post_retry_backoff_factor: int = 20, sleep_send_batch: int = 0, cb_flowcontrol: bool = False, block_size: int = 800000, batch_size: int = 0) -> None:

        if endpoint is None:
            raise ValueError(f'You must define <<endpoint>> in cbManager')

        self.endpoint = endpoint
        self.timeout = timeout
        self.post_retry_connect = post_retry_connect
        self.post_retry_backoff_factor = post_retry_backoff_factor
        self.sleep_send_batch = sleep_send_batch
        self.cb_flowcontrol = cb_flowcontrol
        self.batch_size = batch_size

        # check block_size limit.
        if (int(block_size) > int(800000)):
            raise ValueError('Block size limit reached! <<block_size>> value cannot be greater than 800000')

        self.block_size = block_size


    def delete_entities(self, *, service: str = None, subservice: str = None, auth: authManager = None, limit: int = 100, type: str = None, q: str = None, mq: str = None, georel: str = None, geometry: str = None, coords: str = None, id: str = None, options_get: list = [], options_send: list = []):
        """Delete data from context broker

        :param service: Define service from which entities are deleted, defaults to None
        :param subservice: Define subservice from which entities are deleted, defaults to None
        :param auth: Define authManager, defaults to None
        :param limit: Limits the number of entities to be retrieved, deleted to None
        :param type: Deleted entities whose type matches one of the elements in the list, defaults to None
        :param q: Retrieve entities filtering by attribute value, defaults to None
        :param mq: Retrieve entities filtering by metadata, defaults to None
        :param georel: Georel is intended to specify a spatial relationship between matching entities and a reference shape (geometry), defaults to None
        :param geometry: Allows to define the reference shape to be used when resolving the query. (point | polygon | line | box), defaults to None
        :param coords: Must be a string containing a semicolon-separated list of pairs of geographical coordinates in accordance with the geometry specified, defaults to None
        :param id: Delete entities filtering by Identity, defaults to None
        :param options_get: Options used in Context Broker to find entities, defaults to None
        :param options_send: Options used in Context Broker to delete entities, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises FetchError: is thrown when the response from the cb indicates an error
        """
        data = self.get_entities(service=service, subservice=subservice, auth=auth, limit = limit, type = type, q = q, mq = mq, georel = georel, geometry = geometry, coords = coords, id = id, options=options_get)

        entities = []
        for i, item in enumerate(data):
            entity = {
                'id': f'{item["id"]}',
                'type': f'{item["type"]}',
            }
            entities.append(entity)

        self.send_batch(service=service, subservice=subservice, auth=auth, entities=entities, actionType='delete', options=options_send)

    def get_entities(self, *, service: str = None, subservice: str = None, auth: authManager = None, limit: int = 100, type: str = None, orderBy: str = None, q: str = None, mq: str = None, georel: str = None, geometry: str = None, coords: str = None, id: str = None, options: list = []):
        """Retrieve data from context broker

        :param service: Define service from which entities are retrieved, defaults to None
        :param subservice: Define subservice from which entities are retrieved, defaults to None
        :param auth: Define authManager, defaults to None
        :param limit: Limits the number of entities to be retrieved, defaults to None
        :param type: Retrieve entities whose type matches one of the elements in the list, defaults to None
        :param orderBy: Criteria for ordering results, defaults to None
        :param q: Retrieve entities filtering by attribute value, defaults to None
        :param mq: Retrieve entities filtering by metadata, defaults to None
        :param georel: Georel is intended to specify a spatial relationship between matching entities and a reference shape (geometry), defaults to None
        :param geometry: Allows to define the reference shape to be used when resolving the query. (point | polygon | line | box), defaults to None
        :param coords: Must be a string containing a semicolon-separated list of pairs of geographical coordinates in accordance with the geometry specified, defaults to None
        :param id: Retrieve entities filtering by Identity, defaults to None
        :param options: Options used to retrive entities, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises FetchError: is thrown when the response from the cb indicates an error
        :return: json data
        """

        result = []
        pg = 1

        data = ['go!']
        while (data != []) :
            offset = (pg-1)*limit
            data = self.get_entities_page(service=service, subservice=subservice, auth=auth, offset = offset, limit = limit, type = type, orderBy = orderBy, q = q, mq = mq, georel = georel, geometry = geometry, coords = coords, id = id, options = options)
            pg += 1
            result += data
        return result

    def get_entities_page(self, *, service: Optional[str] = None, subservice: Optional[str] = None, auth: Optional[authManager] = None, offset: Optional[int] = None, limit: Optional[int] = None, type: Optional[str] = None, orderBy: Optional[str] = None, q: Optional[str] = None, mq: Optional[str] = None, georel: Optional[str] = None, geometry: Optional[str] = None, coords: Optional[str] = None, id: Optional[str] = None, options: list = []):
        """Retrieve data from context broker

        :param service: Define service from which entities are retrieved, defaults to None or auth.service defined value
        :param subservice: Define subservice from which entities are retrieved, defaults to None or auth.subservice defined value
        :param auth: Define authManager, defaults to None
        :param offset: Establishes the offset from where entities are retrieved, defaults to None
        :param limit: Limits the number of entities to be retrieved, defaults to None
        :param type: Retrieve entities whose type matches one of the elements in the list, defaults to None
        :param orderBy: Criteria for ordering results, defaults to None
        :param q: Retrieve entities filtering by attribute value, defaults to None
        :param mq: Retrieve entities filtering by metadata, defaults to None
        :param georel: Georel is intended to specify a spatial relationship between matching entities and a reference shape (geometry), defaults to None
        :param geometry: Allows to define the reference shape to be used when resolving the query. (point | polygon | line | box), defaults to None
        :param coords: Must be a string containing a semicolon-separated list of pairs of geographical coordinates in accordance with the geometry specified, defaults to None
        :param id: Retrieve entities filtering by Identity, defaults to None
        :param options: Options used, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises FetchError: is thrown when the response from the cb indicates an error
        :return: json data
        """

        if (auth is not None and not hasattr(auth,"tokens")):
            auth.tokens = {}

        #check subservice defined
        if subservice is None:
            if auth is not None:
                if (not hasattr(auth,"subservice")):
                    raise ValueError('You must define <<subservice>> in authManager')
                else:
                    subservice = auth.subservice
            else:
                raise ValueError('You must define <<subservice>>')
        # auth.subservice is Optional, it might be None
        if subservice is None:
            raise ValueError('You must define <<subservice>> either here or in authManager')

        #check service defined
        if service is None:
            if auth is not None:
                if (not hasattr(auth,"service")):
                    raise ValueError('You must define <<service>> in authManager')
                else:
                    service = auth.service
            else:
                raise ValueError('You must define <<service>>')

        if (auth is not None and subservice not in auth.tokens.keys()):
            auth.get_auth_token_subservice(subservice = subservice)

        headers = {
            'Fiware-Service': service,
            'Fiware-ServicePath': subservice
        }
        if auth is not None:
            headers['X-Auth-Token'] = auth.tokens[subservice]

        # check if use geographical queries, must specify georel, geometry, coords
        if (georel is not None or geometry is not None or coords is not None):
            if (georel is not None and georel != '') and (geometry is not None and geometry != '') and (coords is not None and coords != ''):
                pass
            else:
                raise ValueError('If use geographical queries, you must define georel, geometry and coords in params')


        params = {"offset": offset, "limit": limit, "type": type, "orderBy": orderBy, "q": q, "mq": mq, "georel": georel, "geometry": geometry, "coords": coords, "id": id}

        req_url = ""
        if (options is not None and len(options) > 0):
            req_url = f"{self.endpoint}/v2/entities?options={','.join(options)}"
        else:
            req_url = f"{self.endpoint}/v2/entities"

        resp = requests.get(req_url, params=params, headers=headers, verify=False, timeout=self.timeout)
        if resp.status_code == 400 or resp.status_code == 401:
            respjson = resp.json()
            logger.error(f'{respjson["name"]}: {respjson["message"]}')
        if resp.status_code < 200 or resp.status_code > 204:
            raise FetchError(response=resp, method="GET", url=req_url, params=params, headers=headers)

        return resp.json()


    def send_batch(self, *, service:str = None, subservice: str = None, auth: authManager = None, entities: Iterable[Any], actionType: str = 'append', options: list = []) -> bool:
        """Send batch data to context broker with block control

        :param auth: Define authManager
        :param entities: Entities data
        :param service: Define service to send batch data, defaults to None
        :param subservice: Define subservice to send batch data, defaults to None
        :param actionType: Batch action type, defaults is append
        :param options: Options used, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises Exception: is thrown when the cotext broker response isn't ok operation
        :return: True if the operation is correct
        """
        entitiesToSend = []
        accumulated_block = 0
        for entity in entities:
            entitiesToSend.append(entity)
            accumulated_block += len(json.dumps(entity))

            if accumulated_block > self.block_size or (self.batch_size > 0 and len(entitiesToSend) >= self.batch_size) :
                logger.debug(f'- Sending a batch {actionType} of {len(entitiesToSend)} entities')
                # FIXME: This is actually not needed since `__send_batch` never returns False
                # (it either returns True or raises an exception).
                # But technically, both `__send_batch` and `send_batch` return a bool,
                # so we have to manage this and take some action to make the type checker happy.
                ok = self.__send_batch(auth=auth, service=service, subservice=subservice, entities=entitiesToSend, actionType=actionType, options=options)
                if not ok:
                    return False
                entitiesToSend = []
                accumulated_block = 0

        # Remaining block, if any
        if accumulated_block > 0:
            logger.debug(f'- Sending final batch {actionType} of {len(entitiesToSend)} entities')
            return self.__send_batch(auth=auth, service=service, subservice=subservice, entities=entitiesToSend, actionType=actionType, options=options)

        return True

    def __send_batch(self, *, service:str = None, subservice: str = None, auth: authManager = None, entities: List[Any], actionType: str = 'append', options: list = []) -> bool:
        """Send batch data to context broker

        :param auth: Define authManager
        :param entities: Entities data
        :param service: Define service to send batch data, defaults to None
        :param subservice: Define subservice to send batch data, defaults to None  or auth.subservice defined value
        :param actionType: Batch action type, defaults is append
        :param options: Options used, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises Exception: is thrown when the cotext broker response isn't ok operation
        :return: True if the operation is correct
        """

        if (auth is not None and not hasattr(auth,"tokens")):
            auth.tokens = {}

        #check subservice defined
        if subservice is None:
            if auth is not None:
                if not hasattr(auth,"subservice"):
                    raise ValueError('You must define <<subservice>> in authManager')
                else:
                    subservice = auth.subservice
            else:
                raise ValueError('You must define <<subservice>>')

        if (auth is not None and subservice not in auth.tokens.keys()):
            auth.get_auth_token_subservice(subservice = subservice)

        res = self.__batch_creation(auth=auth, service=service, subservice = subservice, entities=entities, actionType=actionType, options=options)
        if (auth is not None and res.status_code == 401):
            auth.get_auth_token_subservice(subservice = subservice)
            res = self.__batch_creation(auth=auth, service=service, subservice = subservice, entities=entities, actionType=actionType, options=options)

        if res.status_code != 204:
            raise Exception(f'Error in batch {actionType} operation ({res.status_code}): {res.json()}')

        logger.debug(f'- Update batch {actionType} of {len(entities)} entities')

        if (self.sleep_send_batch != 0):
            time.sleep(self.sleep_send_batch)

        return True

    def __batch_creation(self, *, service: str = None, subservice: str = None, auth: authManager = None, entities: List[Any], actionType: str = 'append', options: list = []):
        """Send batch data to Context Broker

        :param entities: Entities data
        :param service: Define service to send batch data, defaults to None or auth.service defined value
        :param subservice: Define subservice to send batch data, defaults to None or auth.subservice defined value
        :param auth: Define authManager, defaults to None
        :param actionType: Batch action type, defaults is append
        :params options: Options used, default to None
        :raises ValueError: is thrown when some required argument is missing
        :return: Http response code
        """

        #check subservice defined
        if subservice is None:
            if auth is not None:
                if not hasattr(auth,"subservice"):
                    raise ValueError('You must define <<subservice>> in authManager')
                else:
                    subservice = auth.subservice
            else:
                raise ValueError('You must define <<subservice>>')
        # auth.subservice is Optional, it could still be None
        if subservice is None:
            raise ValueError('You must define <<subservice>> either here or in authManager')

        #check service defined
        if service is None:
            if auth is not None:
                if not hasattr(auth,"service"):
                    raise ValueError('You must define <<service>> in authManager')
                else:
                    service = auth.service
            else:
                raise ValueError('You must define <<service>>')


        if (not hasattr(self,"endpoint")):
            raise ValueError('You must define <<endpoint>> in cbManager')

        headers = {
            'Fiware-Service': service,
            'Fiware-ServicePath': subservice,
            'Content-Type': 'application/json'
        }
        if (auth is not None):
            headers['X-Auth-Token'] = auth.tokens[subservice]

        body = {
            'actionType': f'{actionType}',
            'entities': entities
        }

        # if cb_flowcontrol, add flowControl flag to options
        if (self.cb_flowcontrol):
            if (options is None):
                options = ['flowControl']
            else:
                # check if flowcontrol is in options.
                if 'flowControl' not in options:
                    options.append('flowControl')

        if (options is not None and len(options) > 0):
            req_url = f"{self.endpoint}/v2/op/update?options={','.join(options)}"
        else:
            req_url = f"{self.endpoint}/v2/op/update"

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
