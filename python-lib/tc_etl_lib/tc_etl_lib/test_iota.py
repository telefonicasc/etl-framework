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
IoT Agent tests.
'''

from . import exceptions
from tc_etl_lib.iota import SendBatchError, iotaManager
import pandas as pd
import pytest
import requests
import unittest
from unittest.mock import patch, Mock, MagicMock


class TestIotaManager(unittest.TestCase):
    def test_send_http_success(self):
        """A success message should be displayed when
        HTTP request is executed successfully."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with patch('requests.post') as mock_post:
            fake_response = Mock()
            # Simulates a successful code status.
            fake_response.status_code = 200
            mock_post.return_value = fake_response
            resp = iot.send_http(data={"key": "value"})
            assert resp == True

    def test_send_http_connection_error(self):
        """Should raise an exception when there is a server connection error."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError()
            with pytest.raises(requests.exceptions.ConnectionError):
                iot.send_http(data={"key": "value"})

    def test_send_http_invalid_data_type(self):
        """Should raise an exception when the data type is incorrect."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with pytest.raises(TypeError) as exc_info:
            iot.send_http(data=["data"])
        exception_message = str(exc_info.value)
        assert "The 'data' parameter should be a dictionary with key-value pairs." in str(
            exception_message)

    def test_send_http_set_not_unique(self):
        """Should raise an exception if the data is an array of dictionaries."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with pytest.raises(TypeError) as exc_info:
            iot.send_http(data=[
                            {"key_1": "value_1"},
                            {"key_2": "value_2"}])
        exception_message = str(exc_info.value)
        assert "The 'data' parameter should be a dictionary with key-value pairs." in str(
            exception_message)

    def test_send_http_unauthorized(self):
        """Should raise an exception when the request is unauthorized."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 401
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_http(data={"key": "value"})
            exception_raised = str(exc_info.value)
            assert "401" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised

    def test_send_http_not_found(self):
        """Should raise an exception when the request is not found."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 404
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_http(data={"key": "value"})

            exception_raised = str(exc_info.value)
            assert "404" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised

    def test_send_http_server_error(self):
        """Should raise an exception if there is a server error."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key')
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 500
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_http(data={"key": "value"})

            exception_raised = str(exc_info.value)
            assert "500" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised

    def test_send_batch_http_dict_success(self):
        """send_http should be called twice when we send an array with 2 dictionaries."""
        with patch('tc_etl_lib.iota.iotaManager.send_http') as mock_send_http:
            iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key', sleep_send_batch=0.25)
            iot.send_batch_http(data=[{'key1': 'value1'}, {'key2': 'value2'}])
        self.assertEqual(mock_send_http.call_count, 2)

    def test_send_batch_http_dict_value_error(self):
        """Should raise TypeError and then raise SendBatchError with the index that failed."""
        iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key', sleep_send_batch=0.25)
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 200
            with self.assertRaises(SendBatchError) as context:
                iot.send_batch_http(data=[{"key_1": "value_1"}, 2])

        expected_message = "send_batch_http error. Index where the error occurred: 1\nError detail: The 'data' parameter should be a dictionary with key-value pairs."
        self.assertEqual(str(context.exception), expected_message)
        # Verify that the POST request was called for the first dictionary.
        mock_post.assert_called_once()

    def test_send_batch_http_connection_error(self):
            """Should raise a ConnectionError exception and then raise SedBatchError with the index that failed."""
            iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key', sleep_send_batch=0.25)
            with patch('requests.post') as mock_post:
                mock_success = MagicMock()
                mock_success.status_code = 200

                mock_connection_error = MagicMock(side_effect=ConnectionError("Simulated connection error"))
                # Simulates a connection error when tries to send the second dictionary.
                mock_post.side_effect = [mock_success, mock_connection_error]

                with self.assertRaises(SendBatchError) as exc_info:
                    iot.send_batch_http(data=[{"key_1": "value_1"}, {"key_2": "value_2"}])

            exception_raised = str(exc_info.exception)

            assert "send_batch_http error. Index where the error occurred: 1\nError detail:" in exception_raised
            assert "Failed to POST" in exception_raised
            # Verify that it tried to do a POST request for each dictionary.
            assert mock_post.call_count == 2

    def test_send_batch_http_data_frame_success(self):
        """send_http should be called 3 times when we send a DataFrame with 3 rows."""
        with patch('tc_etl_lib.iota.iotaManager.send_http') as mock_send_http:
            iot = iotaManager(endpoint='http://fakeurl.com', sensor_id='fake_sensor_id', api_key='fake_api_key', sleep_send_batch=0.25)
            data = pd.DataFrame({
            'column1': [1, 2, 3],
            'column2': ['a', 'b', 'c']
            })
            iot.send_batch_http(data=data)
        self.assertEqual(mock_send_http.call_count, 3)