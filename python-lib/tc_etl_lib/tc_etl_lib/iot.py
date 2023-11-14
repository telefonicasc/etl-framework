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
import requests
import tc_etl_lib as tc
import time
from typing import Any, Iterable


class IoT:
    """IoT is a class that allows us to communicate with the IoT Agent."""

    def __init__(self):
        pass

    def send_json(self,
                  sensor_name: str,
                  api_key: str,
                  req_url: str,
                  data: Any) -> None:
        params = {
            'i': sensor_name,
            'k': api_key
        }
        headers = {
            "Content-Type": "application/json"
        }

        try:
            # Verify if data is a single dictionary.
            if isinstance(data, dict):
                resp = requests.post(url=req_url, json=data,
                                     params=params, headers=headers)
                if resp.status_code == 200:
                    return True
                else:
                    raise exceptions.FetchError(
                        response=resp,
                        method="POST",
                        url=req_url,
                        params=params,
                        headers=headers)
            else:
                raise ValueError(
                    "The parameter 'data' should be a single dictionary {}.")
        except requests.exceptions.ConnectionError as e:
            raise e

    def send_batch(self,
                   sensor_name: str,
                   api_key: str,
                   req_url: str,
                   time_sleep: float,
                   data: Iterable[Any]) -> None:
        params = {
            'i': sensor_name,
            'k': api_key
        }
        headers = {
            "Content-Type": "application/json"
        }

        try:
            for i in range(0, len(data)):
                resp = requests.post(
                    url=req_url, json=data[i], params=params, headers=headers)

                if resp.status_code == 200:
                    time.sleep(time_sleep)
                    return True
                else:
                    raise exceptions.FetchError(response=resp,
                                           method="POST",
                                           url=req_url,
                                           params=params,
                                           headers=headers)
        except requests.exceptions.ConnectionError as e:
            raise e