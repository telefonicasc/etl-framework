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

import tc_etl_lib as tc

# declare minioManager
minio_manager = tc.minioManager(endpoint='<minio_endpoint>:<port>',
                             access_key='<user>',
                             secret_key='<password>')

# Upload test-file.txt to python-test-bucket/output/example.txt
minio_manager.uploadFile(bucket_name='python-test-bucket',
                         destination_file='/output/example.txt',
                         source_file="test-file.txt")

# Retrieve example.txt and apply print method to each 3 bytes
minio_manager.getProcessedFile(bucket_name='python-test-bucket',
                               destination_file='/output/example.txt',
                               chunk_size=3,
                               processing_method=print)

# Custom method that writes the file chunks in a CSV (he receives and writes bytes)
def customCSVProcessingMethod(file_chunk):
    processed_file = open("salida.csv", "ab")
    processed_file.write(file_chunk)
    processed_file.close()

# Upload CSV
minio_manager.uploadFile(bucket_name='python-test-bucket',
                         destination_file='/output/reallyBigFile.csv',
                         source_file="movimientos_padronales_20250822_v2.csv")

# Retrieve reallyBigFile.csv and apply customCSVProcessingMethod method to each 1000000 bytes
minio_manager.getProcessedFile(bucket_name='python-test-bucket',
                               destination_file='/output/reallyBigFile.csv',
                               chunk_size=1000000,
                               processing_method=customCSVProcessingMethod)
