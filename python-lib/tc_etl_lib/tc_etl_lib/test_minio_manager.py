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
# along with IoT orchestrator. If not, see http://www.gnu.org/licenses/.

'''
MinIO Manager tests.
'''

from pytest_minio_mock.plugin import minio_mock
from unittest import mock
from tc_etl_lib.minio import minioManager
import os


def initMinioManager():
    return minioManager(
        endpoint='localhost:9000',
        access_key='admin',
        secret_key='admin123')


def test_createBucket(minio_mock):
    minio_manager = initMinioManager()

    minio_manager.createBucket("test_bucket")
    buckets = minio_manager.client.list_buckets()
    assert len(buckets) == 1


def test_removeBucket(minio_mock):
    minio_manager = initMinioManager()

    minio_manager.createBucket("test_bucket")
    minio_manager.removeBucket("test_bucket")
    buckets = minio_manager.client.list_buckets()
    assert len(buckets) == 0


def test_uploadFile(minio_mock):
    minio_manager = initMinioManager()
    bucket_name = 'test-bucket'
    file = 'test_minioManager_file.txt'

    # Create the test file if it doesnt exist
    fichero_test = open(file, "w")
    fichero_test.write("Test text")
    fichero_test.close()

    minio_manager.createBucket(bucket_name)
    result = minio_manager.uploadFile(bucket_name,
                                      destination_file=file,
                                      source_file=file)

    # Remove the test file
    os.remove(file)
    # pytest_minio_mock returns a string while real minio returns an object
    assert result == "Upload successful"


def test_processFile(minio_mock):
    minio_manager = initMinioManager()
    bucket_name = 'test-bucket'
    file = "test-minioManager-file.txt"
    out_file_name = "out.txt"

    # Create the test file if it doesnt exist
    fichero_test = open(file, "w")
    fichero_test.write("Test text")
    fichero_test.close()

    minio_manager.createBucket(bucket_name)

    minio_manager.uploadFile(bucket_name,
                             destination_file=file,
                             source_file=file)

    # Custom processing method that saves locally the minio file
    def test_processingMethod(file_chunk):
        fichero_procesado = open(out_file_name, "ab")
        fichero_procesado.write(file_chunk)
        fichero_procesado.close()

    class obectStat:
        size = 9

    mocked_return = obectStat()
    with mock.patch('pytest_minio_mock.plugin.MockMinioObject.stat_object', return_value=mocked_return) as irrelevant:
        minio_manager.processFile(bucket_name,
                                  file=file,
                                  chunk_size=9,
                                  processing_method=test_processingMethod)

    # Reads the out file
    out_file = open(out_file_name, "r")
    result = out_file.read()
    out_file.close()

    # Remove the created files
    os.remove(file)
    os.remove(out_file_name)
    # Check the downloaded file content is equal to the uploaded one
    assert result == "Test text"
