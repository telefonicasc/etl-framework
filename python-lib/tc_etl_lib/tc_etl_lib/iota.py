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
IoT Agent routines for Python:
  - iotaManager.send_http
  - iotaManager.send_batch_http
'''

from . import exceptions
import pandas as pd
import requests
import tc_etl_lib as tc
import time
from typing import Iterable, Optional, Union

class SendBatchError(Exception):
    "SendBatchError is a class that can handle exceptions."
    def __init__(self, message, original_exception=None, index=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.index = index

class iotaManager:
    """IoT Agent Manager.

    endpoint: define service endpoint iota (example: https://<service>:<port>).
    sensor_id: sensor ID.
    api_key: API key of the corresponding sensor.
    sleep_send_batch: time sleep in seconds (default: 0).
    """

    endpoint: str
    sensor_id: str
    api_key: str
    sleep_send_batch: float

    def __init__(self, endpoint: str, sensor_id: str, api_key: str, sleep_send_batch: float = 0):
        self.endpoint = endpoint
        self.sensor_id = sensor_id
        self.api_key = api_key
        self.sleep_send_batch = sleep_send_batch

    def send_http(self,
                data: dict) -> Union[None, bool]:

        if not isinstance(data, dict):
                raise TypeError("The 'data' parameter should be a dictionary with key-value pairs.")

        params = {
            'i': self.sensor_id,
            'k': self.api_key
        }
        headers = {
            "Content-Type": "application/json"
        }

        try:
            resp = requests.post(url=self.endpoint, json=data, params=params, headers=headers)
            if resp.status_code == 200:
                return True
            else:
                raise exceptions.FetchError(
                    response=resp,
                    method="POST",
                    url=self.endpoint,
                    params=params,
                    headers=headers)
        except requests.exceptions.ConnectionError as e:
            raise e

    def send_batch_http(self, data: Iterable) -> Union[None, bool]:

            if isinstance(data, pd.DataFrame):
                # Convert each row of the DataFrame to a dictionary.
                for i, row in data.iterrows():
                    try:
                        self.send_http(row.to_dict())
                        time.sleep(self.sleep_send_batch)
                    except Exception as e:
                        raise SendBatchError(f"send_batch_http error. Row that caused the error: {i}\nError detail: {str(e)}", original_exception=e, index=i) from e
            else:
                for i, dictionary in enumerate(data):
                    try:
                        self.send_http(dictionary)
                        time.sleep(self.sleep_send_batch)
                    except Exception as e:
                        raise SendBatchError(f"send_batch_http error. Index where the error occurred: {i}\nError detail: {str(e)}", original_exception=e, index=i) from e
            return True