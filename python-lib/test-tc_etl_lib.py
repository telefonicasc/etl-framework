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
# along with tc_etl_lib. If not, see http://www.gnu.org/licenses/.

import logging

import tc_etl_lib as tc

# Sets the logging configuration
logging.basicConfig(
    level=logging.DEBUG,
    format="time=%(asctime)s | lvl=%(levelname)s | comp=%(name)s | op=%(filename)s[%(lineno)d]:%(funcName)s | msg=%(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger()

# declare authManager
auth: tc.auth.authManager = tc.auth.authManager(endpoint='http://<auth_endpoint>:<port>', 
                                                service = 'alcobendas', 
                                                user = '<user>', 
                                                password = '<password>')

# test get auth token with manager
try:
    #auth.get_auth_token_subservice()
    token = auth.get_auth_token_subservice(subservice = '/energia')
    logger.info(f'token energia: {token}')
    token = auth.get_auth_token_subservice(subservice = '/activos')
    logger.info(f'token activos: {token}')
    pass
except Exception as err:
    logger.error('get_auth_token_subservice problems!!')
    logger.error(err)

# show info about authManager

auth.get_info()

# get entities
cbm: tc.cb.cbManager = tc.cb.cbManager(endpoint = 'http://<cb_endpoint>:<port>')
try :
    entities = [
            {
                "id": "myEntity15",
                "type": "myType",
                "description": {
                    "value": "My happy entity",
                    "type": "Text"
                },
                "online": {
                    "value": 'true',
                    "type": "Boolean"
                },
                "temperature": {
                    "value": '42.3',
                    "type": "Number"
                }
            }
	    ]
    
    cbm.send_batch(auth=auth, subservice='/energia', entities=entities)
    json = cbm.get_entities_page(subservice='/energia', auth=auth, type='myType')
    for i, item in enumerate(json):
        logger.info(f'[{i}]--> {item["id"]} ({item["type"]})')

    json = cbm.get_entities_page(auth=auth, subservice='/activos', offset = 0, limit = 3, orderBy='id')
    for i, item in enumerate(json):
        logger.info(f'[{i}]--> {item["id"]} ({item["type"]})')

        
except Exception as err:
    logger.error(err)

