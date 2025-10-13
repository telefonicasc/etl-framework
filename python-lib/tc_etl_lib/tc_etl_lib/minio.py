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


class minioManager:
    """Minio Manager

    endpoint: define minio endpoint
    access_key: str
    secret_key: str
    source_file: path to the file to upload
    bucket_name: destination bucket on the MinIO server
    destination_file: destination filename on the MinIO server
    processing_method: method to apply to each chunk of the file to retrieve
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

        if access_key is None:
            messageError.append('<<access_key>>')

        if len(messageError) != 0:
            defineParams = messageError[0]
            if len(messageError) != 1:
                defineParams = " and ".join(
                    [", ".join(messageError[:-1]), messageError[-1]])
            raise ValueError(f'You must define {defineParams} in authManager')

        # At this point, all Optional[str] have been validated to be not None.
        # cast them to let type checker knows.
        self.endpoint = cast(str, endpoint)
        self.access_key = cast(str, access_key)
        self.secret_key = cast(str, secret_key)

    def getProcessedFile(self, bucket_name, destination_file, source_file, processing_method):
        """Retrieves a file in chunks and applies a function to each chunk

        :param bucket_name: name of the bucket where the file is located
        :param destination_file: name of the file to retrieve (include path without bucket_name)
        :param processing_method: method to apply to each chunk of the retrieved file
        :return: the processed file
        """
        client = self.__initClient()

        # Development purposes
        self.__createBucket(client, bucket_name)
        self.__uploadFile(client, bucket_name, destination_file, source_file)

        # Get the file
        try:
            response = client.get_object(
                bucket_name, destination_file, offset=0, length=9)
            processing_method(response.read())
        finally:
            response.close()
            response.release_conn()

    def __initClient(self):
        """
        Create a MinIO client with the class endpoint, its access key and secret key.
        """
        return Minio(
            self.endpoint,
            self.access_key,
            self.secret_key,
            # TODO This flag is EXCLUSIVELY for developpment purposes, it uses http instead of https because the
            # local server doesnt have tsl configured. Change for production
            secure=False
        )

    def __createBucket(self, client, bucket_name):
        """
        Create the bucket if it doesn't exist.
        """
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            print("Created bucket", bucket_name)
        else:
            print("Bucket", bucket_name, "already exists")

    def __uploadFile(self, client, bucket_name, destination_file, source_file):
        """
        Upload the file, renaming it in the process

        :param bucket_name: name of the bucket where the file is located
        :param destination_file: name of the file to retrieve (include path without bucket_name)
        :param source_file: name of the file to upload (include path without bucket_name)
        """
        client.fput_object(
            bucket_name,
            object_name=destination_file,
            file_path=source_file,
        )
        print(
            source_file, "successfully uploaded as object",
            destination_file, "to bucket", bucket_name,
        )
