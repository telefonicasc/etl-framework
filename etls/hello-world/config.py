# -*- coding: utf-8 -*-
#
# Copyright 2023 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
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

import os

ETL_LOG_LEVEL = str(os.getenv(f"ETL_LOG_LEVEL", "INFO"))

GRETTING = os.getenv("ETL_HELLO_WORLD_GRETTING", "Hello")
NAME = os.getenv("ETL_HELLO_WORLD_NAME", "world")
