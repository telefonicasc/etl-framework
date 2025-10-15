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

from tc_etl_lib.minioManager import minioManager

# declare minioManager
minio_manager = minioManager(endpoint='localhost:9000',
                             access_key='admin',
                             secret_key='admin123')
minio_client = minio_manager.initClient()

# Upload test-file.txt to python-test-bucket/output/example.txt
# Important: the bucket must already exist, so 
minio_manager.uploadFile(minio_client, bucket_name='python-test-bucket',
                         destination_file='/output/example.txt',
                         source_file="test-file.txt")

# Retrieve example.txt and apply print method to each 3 bytes
minio_manager.getProcessedFile(minio_client,
                               bucket_name='python-test-bucket',
                               destination_file='/output/example.txt',
                               chunk_size=3,
                               processing_method=print)

# Custom method that writes the file chunks in a CSV (he receives and writes bytes)
def customCSVProcessingMethod(file_chunk):
    fichero_procesado = open("salida.csv", "ab")
    fichero_procesado.write(file_chunk)
    fichero_procesado.close()

# Upload CSV
minio_manager.uploadFile(minio_client, bucket_name='python-test-bucket',
                         destination_file='/output/reallyBigFile.csv',
                         source_file="movimientos_padronales_20250822_v2.csv")

# Retrieve reallyBigFile.csv and apply customCSVProcessingMethod method to each 1000000 bytes
minio_manager.getProcessedFile(minio_client,
                               bucket_name='python-test-bucket',
                               destination_file='/output/reallyBigFile.csv',
                               chunk_size=1000000,
                               processing_method=customCSVProcessingMethod)
