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
IoT routines for Python:
  - iot.send_json
  - iot.send_batch
'''

from . import exceptions
import pandas as pd
import requests
import tc_etl_lib as tc
import time
from typing import Any, Iterable

class SendBatchError(Exception):
    "SendBatchError is a class that can handle exceptions."
    def __init__(self, message, original_exception=None, index=None):
        super().__init__(message)
        self.original_exception = original_exception
        self.index = index

class IoT:
    """IoT is a class that allows us to communicate with the IoT Agent."""

    def __init__(self):
        pass

    def send_json(self,
                sensor_name: str,
                api_key: str,
                req_url: str,
                data: Any) -> None:

        if not isinstance(data, dict):
                raise ValueError("The 'data' parameter should be a dictionary with key-value pairs.")

        params = {
            'i': sensor_name,
            'k': api_key
        }
        headers = {
            "Content-Type": "application/json"
        }

        try:
            resp = requests.post(url=req_url, json=data, params=params, headers=headers)
            if resp.status_code == 200:
                return True
            else:
                raise exceptions.FetchError(
                    response=resp,
                    method="POST",
                    url=req_url,
                    params=params,
                    headers=headers)
        except requests.exceptions.ConnectionError as e:
            raise e

    def send_batch(self,
                    sensor_name: str,
                    api_key: str,
                    req_url: str,
                    time_sleep: float,
                    data: Iterable[pd.DataFrame | dict]) -> None:

            if isinstance(data, pd.DataFrame):
                # Convierte cada fila del DataFrame a un diccionario.
                for i, row in data.iterrows():
                    try:
                        self.send_json(sensor_name, api_key, req_url, row.to_dict())
                        time.sleep(time_sleep)
                    except Exception as e:
                        raise SendBatchError(f"send_batch error. Row that caused the error: {i}\nError detail: {str(e)}", original_exception=e, index=i) from e
            else:
                for i, dictionary in enumerate(data):
                    try:
                        self.send_json(sensor_name, api_key, req_url, dictionary)
                        time.sleep(time_sleep)
                    except Exception as e:
                        raise SendBatchError(f"send_batch error. Index where the error occurred: {i}\nError detail: {str(e)}", original_exception=e, index=i) from e