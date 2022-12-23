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

"""
Authorization routines for Python:
  - authManager.get_auth_token_subservice
"""

import requests
import logging
from typing import Dict, Optional, cast

logger = logging.getLogger(__name__)

class authManager:
    """Authentication Manager

    endpoint: define service endpoint auth (example: https://<service>:<port>)
    user: define user to authenticate
    password: define password to authenticate
    service: service to authenticate
    subservice: subservice to authenticate
    tokens: token list stored when athenticate in a subservice
    """
    endpoint: str
    user: str
    password: str
    service: str
    subservice: Optional[str]
    tokens: Dict[str, str]

    def __init__(self, *, endpoint: Optional[str] = None, service: Optional[str] = None, user: Optional[str] = None, password: Optional[str] = None,
                 subservice: Optional[str] = None) -> None:

        messageError = []
        if endpoint is None:
            messageError.append('<<endpoint>>')

        if service is None:
            messageError.append('<<service>>')

        if user is None:
            messageError.append('<<user>>')

        if password is None:
            messageError.append('<<password>>')

        if len(messageError) != 0:
            defineParams = messageError[0]
            if len(messageError) != 1:
                defineParams = " and ".join([", ".join(messageError[:-1]), messageError[-1]])
            raise ValueError(f'You must define {defineParams} in authManager')

        # At this point, all Optional[str] have been validated to be not None.
        # cast them to let type checker knows.
        self.endpoint = cast(str, endpoint)
        self.service = cast(str, service)
        self.user = cast(str, user)
        self.password = cast(str, password)
        self.subservice = subservice
        self.tokens = {}

    def set_token(self, subservice: str, token: str):
        self.subservice = subservice
        self.tokens[subservice] = token

    def get_info(self):
        """ Show auth info

        only for debug uses
        """
        if (hasattr(self, "endpoint")):
            logger.info(f'endpoint: {self.endpoint}')
        if (hasattr(self, "service")):
            logger.info(f'service: {self.service}')
        if (hasattr(self, "subservice")):
            logger.info(f'service: {self.subservice}')
        if (hasattr(self, "user")):
            logger.info(f'user: {self.user}')
        if (hasattr(self, "password")):
            logger.info(f'password: {self.password}')
        if (hasattr(self, "tokens")):
            for key in self.tokens:
                logger.info('tokens[' + key + ']: ' + self.tokens[key])

    def check_mandatory_fields(self):
        """Raise ValueError if some mandatory field is missing"""
        messageError = []
        if (not hasattr(self, "endpoint")):
            messageError.append('<<endpoint>>')

        if (not hasattr(self, "service")):
            messageError.append('<<service>>')

        if (not hasattr(self, "user")):
            messageError.append('<<user>>')

        if (not hasattr(self, "password")):
            messageError.append('<<password>>')

        if len(messageError) != 0:
            defineParams = messageError[0]
            if len(messageError) != 1:
                defineParams = " and ".join([", ".join(messageError[:-1]), messageError[-1]])
            raise ValueError(f'You must define {defineParams} in authManager')

    def _post_auth_request(self, detail: str, scope: dict):
        """Send a POST Auth request to keystone for the given scope

        :param detail: a detail string to include in logging messages
        :param scope: the scope object to send in the request, in keystone v3 format
        :raises Exception: when authorization fails
        :return: requests.Response
        """
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        body = {
            "auth": {
                "scope": scope,
                "identity": {
                    "password": {
                        "user": {
                            "domain": {
                                "name": self.service
                            },
                            "password": self.password,
                            "name": self.user
                        }
                    },
                    "methods": [
                        "password"
                    ]
                }
            }
        }

        logger.debug(f'getting auth token ({detail})...')
        req_url = self.endpoint + '/v3/auth/tokens'
        res = requests.post(req_url, json=body, headers=headers, verify=False)

        if res.status_code != 201:
            raise Exception(f'Failed to get auth token ({detail}) ({res.status_code}): {res.json()}')

        logger.debug(f'Authentication token ({detail}) was created successfully')
        return res

    def get_auth_token_subservice(self, *, subservice: Optional[str] = None):
        """Authenticate in a service/subservice and get a token

        :param subservice: define subservice to be authenticated, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises Exception: is thrown when the response from the auth service indicates an error
        :return: the token returned by the auth service
        """

        self.check_mandatory_fields()
        if (not hasattr(self, "tokens")):
            self.tokens = {}

        if subservice is None:
            if (not hasattr(self, "subservice")):
                raise ValueError('You must define <<subservice>> in authManager')
            else:
                subservice = self.subservice
        # auth.subservice is Optional, it might be None
        if subservice is None:
            raise ValueError('You must define <<subservice>>')

        res = self._post_auth_request(detail=f"subservice {subservice}", scope={
            "project": {
                "domain": {
                    "name": self.service
                },
                "name": subservice
            }
        })

        token = res.headers['X-Subject-Token']
        self.tokens[subservice] = token
        return token

    def get_auth_token_service(self):
        """Authenticate in a service and get a token.
        The service token is NOT cached.

        :raises ValueError: is thrown when some required argument is missing
        :raises Exception: is thrown when the response from the auth service indicates an error
        :return: an object with { 'token': .... 'user_id': ..., 'domain_id': ... }
        """

        self.check_mandatory_fields()
        res = self._post_auth_request(detail=f"service {self.service}", scope={
            "domain": {
                "name": self.service
            }
        })

        token = res.headers['X-Subject-Token']
        tbody = res.json()['token']
        return {
            'token': token,
            'user_id': tbody['user']['id'],
            'domain_id': tbody['user']['domain']['id'],
        }
