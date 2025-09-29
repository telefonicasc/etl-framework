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

import pathlib
from setuptools import find_packages, setup

HERE = pathlib.Path(__file__).parent

VERSION = '0.16.0'
PACKAGE_NAME = 'tc_etl_lib'
AUTHOR = ''
AUTHOR_EMAIL = ''
URL = ''

LICENSE = '' #Tipo de licencia
DESCRIPTION = 'Librería común para uso en ETL' # Descripción corta
LONG_DESCRIPTION = (HERE / "README.md").read_text(encoding='utf-8') #Referencia al documento README con una descripción más elaborada
LONG_DESC_TYPE = "text/markdown"

#Paquetes necesarios para que funcione la librería. Se instalarán a la vez si no lo tuvieras ya instalado
#Dos listas separadas (para Python >=3.12 y para Python <3.12)
INSTALL_REQUIRES = [
    'requests==2.25.1',
    'urllib3==1.26.16',
    'psycopg2-binary>=2.9.5',
    'pandas==2.0.3',
    # Pandas < 2.2.2 requiere numpy < 2.0.0, ver https://pandas.pydata.org/docs/whatsnew/v2.2.2.html
    # Con pandas < 2.2.2 y numpy >= 2.0.0, se produce el error:
    # ValueError: numpy.dtype size changed, may indicate binary incompatibility. Expected 96 from C header, got 88 from PyObject
    # La última release de numpy antes de 2.0.0 es 1.26.4.
    # La última release de numpy compatible con python 3.8 es 1.24.4
    'numpy==1.24.4'
]
INSTALL_REQUIRES_PYTHON_3_12 = [
    'requests>=2.28.2,<2.33.0',
    'urllib3==1.26.16',
    'psycopg2-binary>=2.9.5',
    'pandas==2.2.2',
    'numpy==2.2.0'
]

setup(
    name=PACKAGE_NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type=LONG_DESC_TYPE,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    url=URL,
    extras_require={
        ':python_version<"3.12"': INSTALL_REQUIRES,
        ':python_version>="3.12"': INSTALL_REQUIRES_PYTHON_3_12
    },
    license=LICENSE,
    packages=find_packages(),
    include_package_data=True
)
