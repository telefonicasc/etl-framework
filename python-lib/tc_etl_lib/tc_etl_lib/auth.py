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
from typing import Dict

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
    subservice: str
    tokens: Dict[str, str]


    def __init__(self,*, endpoint: str = None, service: str = None, user: str = None, password: str = None, subservice: str = None) -> None:
        
        messageError = []
        if (endpoint == None):
            messageError.append('<<endpoint>>')
        
        if (service == None):
            messageError.append('<<service>>')

        if (user == None):
            messageError.append('<<user>>')

        if (password == None):
            messageError.append('<<password>>')
        
        if len(messageError) != 0:
            defineParams = messageError[0]
            if len(messageError) != 1:
                defineParams = " and ".join([", ".join(messageError[:-1]), messageError[-1]])
            raise ValueError(f'You must define {defineParams} in authManager')
        
        self.endpoint = endpoint
        self.service = service
        self.user = user
        self.password = password
        self.subservice = subservice
        self.tokens = {}  

    def get_info(self):
        """ Show auth info
        
        only for debug uses
        """
        if (hasattr(self,"endpoint")):
            logger.info(f'endpoint: {self.endpoint}')
        if (hasattr(self,"service")):
            logger.info(f'service: {self.service}')
        if (hasattr(self,"subservice")):
            logger.info(f'service: {self.subservice}')
        if (hasattr(self,"user")):
            logger.info(f'user: {self.user}')
        if (hasattr(self,"password")):
            logger.info(f'password: {self.password}')
        if (hasattr(self,"tokens")):
            for key in self.tokens:
                logger.info('tokens[' + key + ']: ' + self.tokens[key])

    def get_auth_token_subservice(self, *, subservice: str = None):
        """Authenticate in a service/subservice and get a token

        :param subservice: define subservice to be authenticated, defaults to None
        :raises ValueError: is thrown when some required argument is missing
        :raises Exception: is thrown when the response from the auth service indicates an error
        :return: the token returned by the auth service
        """
                
        messageError = []
        if (not hasattr(self,"endpoint")):
            messageError.append('<<endpoint>>')
        
        if (not hasattr(self,"service")):
            messageError.append('<<service>>')

        if (not hasattr(self,"user")):
            messageError.append('<<user>>')

        if (not hasattr(self,"password")):
            messageError.append('<<password>>')
        
        if len(messageError) != 0:
            defineParams = messageError[0]
            if len(messageError) != 1:
                defineParams = " and ".join([", ".join(messageError[:-1]), messageError[-1]])
            raise ValueError(f'You must define {defineParams} in authManager')
        
        if (not hasattr(self,"tokens")):
            self.tokens = {}
            
        if (subservice == None):
            if (not hasattr(self,"subservice")):
                raise ValueError('You must define <<subservice>> in authManager')
            else:
                subservice = self.subservice

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

        body = {
            "auth": {
                "scope": {
                    "project": {
                        "domain": {
                            "name": self.service
                        },
                        "name": subservice
                    }
                },
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

        logger.debug(f'getting auth token (subservice "{subservice}")...')
        req_url = self.endpoint + '/v3/auth/tokens'
        res = requests.post(req_url, json=body, headers=headers, verify=False)

        if res.status_code != 201:
            raise Exception(f'Failed to get auth token (subservice "{subservice}") ({res.status_code}): {res.json()}')

        logger.debug(f'Authentication token for subservice "{subservice}" was created successfully')
        token = res.headers['X-Subject-Token']
        self.tokens[subservice] = token
        return token
