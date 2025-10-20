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
Minio routines for Python:
    - minioManager.
"""
from minio import Minio
from typing import Optional, cast
import logging

logger = logging.getLogger(__name__)


class minioManager:
    """Minio Manager

    endpoint: define minio endpoint
    access_key: user to log in to minio
    secret_key: password to log in to minio
    client: authenticated MinIO client
    """
    endpoint: str
    access_key: str
    secret_key: str
    client: Minio

    def __init__(self, endpoint: Optional[str] = None, access_key: Optional[str] = None, secret_key: Optional[str] = None):

        messageError = []
        if endpoint is None:
            messageError.append('<<endpoint>>')

        if access_key is None:
            messageError.append('<<access_key>>')

        if secret_key is None:
            messageError.append('<<secret_key>>')

        if len(messageError) != 0:
            defineParams = messageError[0]
            if len(messageError) != 1:
                defineParams = " and ".join(
                    [", ".join(messageError[:-1]), messageError[-1]])
            raise ValueError(f'You must define {defineParams} in minioManager')

        # At this point, all Optional[str] have been validated to be not None.
        # cast them to let type checker knows.
        self.endpoint = cast(str, endpoint)
        self.access_key = cast(str, access_key)
        self.secret_key = cast(str, secret_key)
        self.client = self.__initClient()

    def __initClient(self):
        """
        Create a MinIO client with the class endpoint, its access key and secret key.

        :return authenticated MinIO client
        """
        return Minio(
            self.endpoint,
            self.access_key,
            self.secret_key
        )

    def createBucket(self, bucket_name):
        """
        Create the bucket if it doesn't exist.

        :param bucket_name: name of the bucket where the file is located
        """
        found = self.client.bucket_exists(bucket_name)
        if not found:
            self.client.make_bucket(bucket_name)
            logger.debug(f'Created bucket ({bucket_name})')
        else:
            logger.debug(f'Bucket {bucket_name} already exists')

    def removeBucket(self, bucket_name):
        """
        Remove the bucket if it exists.

        :param bucket_name: name of the bucket where the file is located
        """
        found = self.client.bucket_exists(bucket_name)
        if found:
            self.client.remove_bucket(bucket_name)
            logger.debug(f'Removed bucket {bucket_name}')
        else:
            logger.debug(f'Bucket {bucket_name} doesnt exist')

    def uploadFile(self, bucket_name, destination_file, source_file):
        """
        Upload the file, renaming it in the process

        :param bucket_name: name of the bucket where the file is located
        :param destination_file: name of the file to retrieve (can include path without bucket_name)
        :param source_file: name of the file to upload (can include path)
        :return object with the status of the upload
        """
        # Bucket must exist before uploading file
        self.createBucket(bucket_name)

        logger.debug(
            f'Uploading {source_file} as object {destination_file} to bucket {bucket_name}')
        return self.client.fput_object(
            bucket_name,
            object_name=destination_file,
            file_path=source_file,
        )

    def processFile(self, bucket_name, file, processing_method, chunk_size=500000):
        """Retrieves a file in chunks and applies a function to each chunk

        :param bucket_name: name of the bucket where the file is located
        :param file: name of the file to retrieve (can include path without bucket_name)
        :param chunk_size: size in bytes of the chunks to retrieve
        :param processing_method: method to apply to each chunk of the retrieved file
        """
        file_size = self.client.stat_object(
            bucket_name, object_name=file).size or 0

        response = None
        for offset in range(0, file_size, chunk_size):
            # Get the file
            try:
                response = self.client.get_object(
                    bucket_name, file, offset, chunk_size)
                # response.data returns bytes
                processing_method(response.data)
            except Exception as e:
                raise Exception(f'An error occured while processing the file: {e}')

        logger.debug(f'Processing ended.')
        if response:
            response.close()
            response.release_conn()
