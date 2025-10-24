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
Object storage routines for Python:
    - objectStorageManager.
"""
from typing import Optional, cast
import logging
import boto3

logger = logging.getLogger(__name__)


class objectStorageManager:
    """Object storage Manager

    endpoint: define Object storage endpoint
    access_key: user to log in to Object storage
    secret_key: password to log in to Object storage
    """
    endpoint: str
    access_key: str
    secret_key: str

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
            raise ValueError(f'You must define {defineParams} in objectStorageManager')

        # At this point, all Optional[str] have been validated to be not None.
        # cast them to let type checker knows.
        self.endpoint = cast(str, endpoint)
        self.access_key = cast(str, access_key)
        self.secret_key = cast(str, secret_key)
        self.client = self.__init_client()

    def __init_client(self):
        """
        Create a Object storage client with the class endpoint, its access key and secret key.

        :return authenticated Object storage client
        """
        return boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            aws_session_token=None
        )

    def create_bucket(self, bucket_name):
        """
        Create the bucket if it doesn't exist.

        :param bucket_name: name of the bucket where the file is located
        """
        try:
            self.client.create_bucket(Bucket=bucket_name)
            logger.debug(f'Created bucket ({bucket_name})')
        except Exception as e:
            # BucketAlreadyExists or BucketAlreadyOwnedByYou
            logger.debug(f'Error creating the bucket: {e}')

    def remove_bucket(self, bucket_name):
        """
        Remove the bucket if it exists.

        :param bucket_name: name of the bucket where the file is located
        """
        try:
            self.client.delete_bucket(Bucket=bucket_name)
            logger.debug(f'Removed bucket {bucket_name}')
        except Exception as e:
            logger.debug(f'An error ocurred while deleting {bucket_name}: {e}')
            raise Exception(f'An error ocurred while deleting {bucket_name}: {e}')

    def upload_file(self, bucket_name, destination_file, source_file):
        """
        Upload the file, renaming it in the process

        :param bucket_name: name of the bucket where the file is located
        :param destination_file: name of the file to retrieve (can include path without bucket_name)
        :param source_file: name of the file to upload (can include path)
        """
        # Bucket must exist before uploading file
        self.create_bucket(bucket_name)

        logger.debug(
            f'Uploading {source_file} as object {destination_file} to bucket {bucket_name}')
        try:
            self.client.upload_file(source_file, bucket_name, destination_file)
        except Exception as e:
            logger.debug(f'An error ocurred while uploading the file: {e}')
            raise Exception(f'An error ocurred while uploading the file: {e}')

    def process_file(self, bucket_name, file, processing_method, chunk_size=500000):
        """Retrieves a file in chunks and applies a function to each chunk

        :param bucket_name: name of the bucket where the file is located
        :param file: name of the file to retrieve (can include path without bucket_name)
        :param processing_method: method to apply to each chunk of the retrieved file
        :param chunk_size: size in bytes of the chunks to retrieve (500000 by default)
        """
        file_size = self.client.get_object_attributes(
            Bucket=bucket_name, Key=file, ObjectAttributes=['ObjectSize'])['ObjectSize'] or 0

        for offset in range(0, file_size, chunk_size):
            # Get the file
            try:
                byte_range = f'bytes={offset}-{offset+chunk_size-1}'
                response = self.client.get_object(
                    Bucket=bucket_name, Key=file, Range=byte_range)
                # response.data returns bytes
                processing_method(response['Body'].read())
            except Exception as e:
                raise Exception(
                    f'An error occured while processing the file: {e}')

        logger.debug(f'Processing ended.')
