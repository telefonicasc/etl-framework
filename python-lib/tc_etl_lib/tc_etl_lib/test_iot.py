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
IoT tests
'''

from . import IoT, exceptions
import pytest
import requests
import unittest
from unittest.mock import patch, Mock


class TestIoT(unittest.TestCase):
    def test_send_json_success(self):
        """A success message should be displayed when
        HTTP request is executed successfully."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            fake_response = Mock()
            # Simulates a successful code status.
            fake_response.status_code = 200
            mock_post.return_value = fake_response
            resp = iot.send_json("fake_sensor", "fake_api_key",
                        "http://fakeurl.com", {"key": "value"})
            assert resp == True


    def test_send_json_connection_error(self):
        """Should raise an exception if there is a server connection error."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError()
            with pytest.raises(requests.exceptions.ConnectionError):
                iot.send_json(
                    "fake_sensor_name",
                    "fake_api_key",
                    "http://fakeurl.com",
                    {"key": "value"})


    def test_send_json_invalid_data_type(self):
        """Should raise an exception if the data type is icorrect."""
        iot = IoT()
        with pytest.raises(ValueError) as exc_info:
            iot.send_json("fake_sensor_name",
                        "fake_api_key",
                        "http://fakeurl.com",
                        ["data"])
        exception_message = str(exc_info.value)
        assert "The parameter 'data' should be a single dictionary {}." in str(
            exception_message)


    def test_send_json_set_not_unique(self):
        """Should raise an exception if the data is an array of dictionaries."""
        iot = IoT()
        with pytest.raises(ValueError) as exc_info:
            iot.send_json("fake_sensor_name", "fake_api_key",
                        "http://fakeurl.com",
                        [
                            {"key_1": "value_1"},
                            {"key_2": "value_2"}])
        exception_message = str(exc_info.value)
        assert "The parameter 'data' should be a single dictionary {}." in str(
            exception_message)


    def test_send_json_unauthorized(self):
        """Should raise an exception if the request is unauthorized."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 401
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_json("fake_sensor_name",
                            "fake_api_key",
                            "http://fakeurl.com",
                            {"key": "value"})
            exception_raised = str(exc_info.value)
            assert "401" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised


    def test_send_json_not_found(self):
        """Should raise an exception if the request is not found."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 404
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_json("fake_sensor_name", "fake_api_key",
                            "http://fakeurl.com", {"key": "value"})

            exception_raised = str(exc_info.value)
            assert "404" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised


    def test_send_json_server_error(self):
        """Should raise an exception if there is a server error."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 500
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_json("fake_sensor_name",
                            "fake_api_key",
                            "http://fakeurl.com",
                            {"key": "value"})

            exception_raised = str(exc_info.value)
            assert "500" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised


    def test_send_batch_success(self):
        """The status code should be 200 if a request is success."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            fake_response = Mock()
            # Simulates a successful status code.
            fake_response.status_code = 200
            mock_post.return_value = fake_response
            resp = iot.send_batch("fake_sensor",
                        "fake_api_key",
                        "http://fakeurl.com",
                        0.25,
                        [
                            {"key_1": "value_1"},
                            {"key_2", "value_2"}
                        ])


            assert resp == True


    def test_send_batch_connection_error(self):
        """Should raise an exception if there is a connection error."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.side_effect = requests.exceptions.ConnectionError()
            with pytest.raises(requests.exceptions.ConnectionError):
                iot.send_batch(
                    "fake_sensor_name",
                    "fake_api_key",
                    "http://fakeurl.com",
                    0.25,
                    [
                        {"key_1": "value_1"},
                        {"key_2": "value_2"}
                    ])



    def test_send_batch_unauthorized(self):
        """Should raise an exception if the request is unauthorized."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 401
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_batch("fake_sensor_name",
                            "fake_api_key",
                            "http://fakeurl.com",
                            0.25,
                            [
                                {"key_1": "value_1"},
                                {"key_2": "value_2"}])
            exception_raised = str(exc_info.value)
            assert "401" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised


    def test_send_batch_not_found(self):
        """Should raise an exception if the request is not found."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 404
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_batch("fake_sensor_name",
                            "fake_api_key",
                            "http://fakeurl.com",
                            0.25,
                            [
                                {"key_1": "value_1"},
                                {"key_2": "value_2"}])

            exception_raised = str(exc_info.value)
            assert "404" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised


    def test_send_batch_server_error(self):
        """Should raise an exception if there is a server error."""
        iot = IoT()
        with patch('requests.post') as mock_post:
            mock_post.return_value.status_code = 500
            with pytest.raises(exceptions.FetchError) as exc_info:
                iot.send_batch("fake_sensor_name",
                            "fake_api_key",
                            "http://fakeurl.com",
                            0.25,
                            [
                                {"key_1": "value_1"},
                                {"key_2": "value_2"}])

            exception_raised = str(exc_info.value)
            assert "500" in exception_raised
            assert "Failed to POST http://fakeurl.com" in exception_raised