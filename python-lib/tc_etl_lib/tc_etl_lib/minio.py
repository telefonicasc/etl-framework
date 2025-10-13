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

    endpoint: define minio endpoint (example: https://<service>:<port>)
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
    source_file: str
    bucket_name: str
    destination_file: str

    def __init__(self, endpoint: Optional[str] = None, access_key: Optional[str] = None, secret_key: Optional[str] = None,
                 source_file: Optional[str] = None, bucket_name: Optional[str] = None, destination_file: Optional[str] = None,
                 processing_method=print):

        messageError = []
        if endpoint is None:
            messageError.append('<<endpoint>>')

        if access_key is None:
            messageError.append('<<access_key>>')

        if secret_key is None:
            messageError.append('<<secret_key>>')

        if access_key is None:
            messageError.append('<<access_key>>')

        if source_file is None:
            messageError.append('<<source_file>>')

        if bucket_name is None:
            messageError.append('<<bucket_name>>')

        if destination_file is None:
            messageError.append('<<destination_file>>')

        if processing_method is None:
            messageError.append('<<processing_method>>')

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
        self.source_file = cast(str, source_file)
        self.bucket_name = cast(str, bucket_name)
        self.destination_file = cast(str, destination_file)
        self.processing_method = processing_method

    def getProcessedFile(self):
        client = self.__initClient()

        # Development purposes
        self.__createBucket(client)
        self.__uploadFile(client)

        # Get the file
        try:
            response = client.get_object(
                self.bucket_name, self.destination_file, offset=0, length=9)
            self.processing_method(response.read())
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

    def __createBucket(self, client):
        """
        Create the bucket if it doesn't exist.
        """
        found = client.bucket_exists(bucket_name=self.bucket_name)
        if not found:
            client.make_bucket(bucket_name=self.bucket_name)
            print("Created bucket", self.bucket_name)
        else:
            print("Bucket", self.bucket_name, "already exists")

    def __uploadFile(self, client):
        """
        Upload the file, renaming it in the process
        """
        client.fput_object(
            bucket_name=self.bucket_name,
            object_name=self.destination_file,
            file_path=self.source_file,
        )
        print(
            self.source_file, "successfully uploaded as object",
            self.destination_file, "to bucket", self.bucket_name,
        )
