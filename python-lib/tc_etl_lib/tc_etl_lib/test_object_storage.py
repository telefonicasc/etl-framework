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

'''
Object storage Manager tests.
'''

from unittest import mock, TestCase
from unittest.mock import patch
from tc_etl_lib.object_storage import objectStorageManager


class TestObjectStorageService(TestCase):

    def init_object_storage_manager(self):
        return objectStorageManager(
            endpoint='http://localhost:9000',
            access_key='admin',
            secret_key='admin123')

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_create_bucket(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client
        object_storage_manager = self.init_object_storage_manager()
        object_storage_manager.create_bucket("test-bucket")

        mock_object_storage_client.create_bucket.assert_called_once_with(
            Bucket='test-bucket'
        )

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_remove_bucket(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client
        object_storage_manager = self.init_object_storage_manager()
        object_storage_manager.remove_bucket("test-bucket")

        mock_object_storage_client.delete_bucket.assert_called_once_with(
            Bucket='test-bucket'
        )

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_remove_bucket_error(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client

        mock_object_storage_client.delete_bucket.side_effect = Exception(
            "Test error removing bucket")
        with self.assertRaises(Exception):
            object_storage_manager = self.init_object_storage_manager()
            object_storage_manager.remove_bucket("test-bucket")

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_upload_file(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client
        object_storage_manager = self.init_object_storage_manager()
        object_storage_manager.upload_file("test-bucket", "destination", "source")

        mock_object_storage_client.upload_file.assert_called_once_with(
            "source", "test-bucket", "destination"
        )

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_upload_file_error(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client

        mock_object_storage_client.upload_file.side_effect = Exception(
            "Test error uploading file")
        with self.assertRaises(Exception):
            object_storage_manager = self.init_object_storage_manager()
            object_storage_manager.upload_file("test-bucket", "destination", "source")

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_process_file(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client
        fake_attr = {'ObjectSize': 10}
        mock_object_storage_client.get_object_attributes.return_value(fake_attr)

        def custom_processing_method(chunk):
            pass
        object_storage_manager = self.init_object_storage_manager()
        object_storage_manager.process_file(bucket_name='test-bucket',
                                            file='file',
                                            processing_method=custom_processing_method,
                                            chunk_size=10)

        mock_object_storage_client.get_object.assert_called_once_with(
            Bucket='test-bucket', Key='file', Range='bytes=0-9'
        )

    @patch('tc_etl_lib.object_storage.boto3.client')
    def test_process_file_error(self, mock_boto_client):
        mock_object_storage_client = mock.MagicMock()
        mock_boto_client.return_value = mock_object_storage_client
        fake_attr = {'ObjectSize': 10}
        mock_object_storage_client.get_object_attributes.return_value(fake_attr)

        def custom_processing_method(chunk):
            pass

        mock_object_storage_client.get_object.side_effect = Exception(
            "Test error uploading file")
        with self.assertRaises(Exception):
            object_storage_manager = self.init_object_storage_manager()
            object_storage_manager.process_file(bucket_name='test-bucket',
                                                file='file',
                                                processing_method=custom_processing_method,
                                                chunk_size=10)
