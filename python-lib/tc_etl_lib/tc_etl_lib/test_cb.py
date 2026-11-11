#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Telefónica Soluciones de Informática y Comunicaciones de España, S.A.U.
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

from unittest.mock import Mock

import pytest

from tc_etl_lib.cb import cbManager
from tc_etl_lib import exceptions


@pytest.fixture
def cb_manager():
    manager = cbManager(endpoint="http://context-broker:0000")
    manager.session = Mock()
    return manager


def test_cb_manager_initialization_successfully():
    manager = cbManager(
        endpoint="http://context-broker:0000",
        timeout=20,
        post_retry_connect=5,
        post_retry_backoff_factor=10,
        sleep_send_batch=2,
        cb_flowcontrol=True,
        block_size=500000,
        batch_size=100,
    )

    assert manager.endpoint == "http://context-broker:0000"
    assert manager.timeout == 20
    assert manager.post_retry_connect == 5
    assert manager.post_retry_backoff_factor == 10
    assert manager.sleep_send_batch == 2
    assert manager.cb_flowcontrol is True
    assert manager.block_size == 500000
    assert manager.batch_size == 100


def test_cb_manager_without_endpoint():
    with pytest.raises(ValueError, match="You must define <<endpoint>>"):
        cbManager()


def test_cb_manager_block_size_exceeds_limit():
    with pytest.raises(ValueError, match="Block size limit reached"):
        cbManager(
            endpoint="http://context-broker:0000",
            block_size=800001,
        )


def test_get_entities_page_successfully(monkeypatch, cb_manager):
    entities = [{"id": "1", "type": "AirQualityObserved"}]

    response = Mock()
    response.status_code = 200
    response.json.return_value = entities

    get_mock = Mock(return_value=response)
    monkeypatch.setattr("tc_etl_lib.cb.requests.get", get_mock)

    result = cb_manager.get_entities_page(
        service="medioambiente",
        subservice="/MedioAmbiente",
        offset=0,
        limit=100,
        type="AirQualityObserved",
    )

    assert result == entities

    get_mock.assert_called_once_with(
        "http://context-broker:0000/v2/entities",
        params={
            "offset": 0,
            "limit": 100,
            "type": "AirQualityObserved",
            "orderBy": None,
            "q": None,
            "mq": None,
            "georel": None,
            "geometry": None,
            "coords": None,
            "id": None,
            "idPattern": None,
        },
        headers={
            "Fiware-Service": "medioambiente",
            "Fiware-ServicePath": "/MedioAmbiente",
        },
        verify=False,
        timeout=10,
    )


def test_get_entities_page_without_subservice(cb_manager):
    with pytest.raises(ValueError, match="You must define <<subservice>>"):
        cb_manager.get_entities_page(
            service="medioambiente",
        )


def test_get_entities_page_without_service(cb_manager):
    with pytest.raises(ValueError, match="You must define <<service>>"):
        cb_manager.get_entities_page(
            subservice="/MedioAmbiente",
        )


def test_get_entities_page_with_incomplete_geographical_query(cb_manager):
    with pytest.raises(
        ValueError,
        match="you must define georel, geometry and coords",
    ):
        cb_manager.get_entities_page(
            service="medioambiente",
            subservice="/MedioAmbiente",
            georel="near",
        )


def test_get_entities_page_http_error(monkeypatch, cb_manager):
    response = Mock()
    response.status_code = 500

    get_mock = Mock(return_value=response)
    monkeypatch.setattr("tc_etl_lib.cb.requests.get", get_mock)

    with pytest.raises(exceptions.FetchError):
        cb_manager.get_entities_page(
            service="medioambiente",
            subservice="/MedioAmbiente",
        )


def test_get_entities_page_with_authentication(monkeypatch, cb_manager):
    auth_mock = Mock()
    auth_mock.tokens = {"/MedioAmbiente": "token-prueba"}

    response = Mock()
    response.status_code = 200
    response.json.return_value = []

    get_mock = Mock(return_value=response)
    monkeypatch.setattr("tc_etl_lib.cb.requests.get", get_mock)

    result = cb_manager.get_entities_page(
        subservice="/MedioAmbiente",
        service="medioambiente",
        auth=auth_mock,
    )

    assert result == []

    headers = get_mock.call_args.kwargs["headers"]
    assert headers["X-Auth-Token"] == "token-prueba"

    auth_mock.get_auth_token_subservice.assert_not_called()


def test_get_entities_successfully(monkeypatch, cb_manager):
    page_1 = [{"id": "1"}, {"id": "2"}]
    page_2 = [{"id": "3"}]

    get_page_mock = Mock(side_effect=[page_1, page_2, []])
    monkeypatch.setattr(
        cb_manager,
        "get_entities_page",
        get_page_mock,
    )

    result = cb_manager.get_entities(
        service="medioambiente",
        subservice="/MedioAmbiente",
        limit=2,
    )

    assert result == page_1 + page_2
    assert get_page_mock.call_count == 3

    assert get_page_mock.call_args_list[0].kwargs["offset"] == 0
    assert get_page_mock.call_args_list[1].kwargs["offset"] == 2
    assert get_page_mock.call_args_list[2].kwargs["offset"] == 4


def test_get_entities_without_results(monkeypatch, cb_manager):
    get_page_mock = Mock(return_value=[])
    monkeypatch.setattr(
        cb_manager,
        "get_entities_page",
        get_page_mock,
    )

    result = cb_manager.get_entities(
        service="medioambiente",
        subservice="/MedioAmbiente",
    )

    assert result == []
    get_page_mock.assert_called_once()


def test_get_subscriptions_page_successfully(monkeypatch, cb_manager):
    subscriptions = [{"id": "subscription-1"}]

    response = Mock()
    response.status_code = 200
    response.json.return_value = subscriptions

    get_mock = Mock(return_value=response)
    monkeypatch.setattr("tc_etl_lib.cb.requests.get", get_mock)

    result = cb_manager.get_subscriptions_page(
        service="medioambiente",
        subservice="/MedioAmbiente",
        offset=0,
        limit=100,
        options=["count"],
    )

    assert result == subscriptions

    get_mock.assert_called_once_with(
        "http://context-broker:0000/v2/subscriptions",
        params={
            "offset": 0,
            "limit": 100,
            "options": "count",
        },
        headers={
            "Fiware-Service": "medioambiente",
            "Fiware-ServicePath": "/MedioAmbiente",
        },
        verify=False,
        timeout=10,
    )


def test_get_subscriptions_page_without_subservice(cb_manager):
    with pytest.raises(ValueError, match="You must define <<subservice>>"):
        cb_manager.get_subscriptions_page(
            service="medioambiente",
        )


def test_get_subscriptions_page_http_error(monkeypatch, cb_manager):
    response = Mock()
    response.status_code = 500

    get_mock = Mock(return_value=response)
    monkeypatch.setattr("tc_etl_lib.cb.requests.get", get_mock)

    with pytest.raises(exceptions.FetchError):
        cb_manager.get_subscriptions_page(
            service="medioambiente",
            subservice="/MedioAmbiente",
        )


def test_get_subscriptions_successfully(monkeypatch, cb_manager):
    page_1 = [{"id": "subscription-1"}]
    page_2 = [{"id": "subscription-2"}]

    get_page_mock = Mock(side_effect=[page_1, page_2, []])
    monkeypatch.setattr(
        cb_manager,
        "get_subscriptions_page",
        get_page_mock,
    )

    result = cb_manager.get_subscriptions(
        service="medioambiente",
        subservice="/MedioAmbiente",
        limit=1,
    )

    assert result == page_1 + page_2
    assert get_page_mock.call_count == 3

    assert get_page_mock.call_args_list[0].kwargs["offset"] == 0
    assert get_page_mock.call_args_list[1].kwargs["offset"] == 1
    assert get_page_mock.call_args_list[2].kwargs["offset"] == 2


def test_get_subscriptions_without_results(monkeypatch, cb_manager):
    get_page_mock = Mock(return_value=[])
    monkeypatch.setattr(
        cb_manager,
        "get_subscriptions_page",
        get_page_mock,
    )

    result = cb_manager.get_subscriptions(
        service="medioambiente",
        subservice="/MedioAmbiente",
    )

    assert result == []
    get_page_mock.assert_called_once()


def test_delete_entities_successfully(monkeypatch, cb_manager):
    entities = [
        {
            "id": "air-1",
            "type": "AirQualityObserved",
            "temperature": {"value": 20},
        },
        {
            "id": "air-2",
            "type": "AirQualityObserved",
            "temperature": {"value": 25},
        },
    ]

    get_entities_mock = Mock(return_value=entities)
    send_batch_mock = Mock(return_value=True)

    monkeypatch.setattr(cb_manager, "get_entities", get_entities_mock)
    monkeypatch.setattr(cb_manager, "send_batch", send_batch_mock)

    cb_manager.delete_entities(
        service="medioambiente",
        subservice="/MedioAmbiente",
        type="AirQualityObserved",
    )

    send_batch_mock.assert_called_once_with(
        service="medioambiente",
        subservice="/MedioAmbiente",
        auth=None,
        entities=[
            {"id": "air-1", "type": "AirQualityObserved"},
            {"id": "air-2", "type": "AirQualityObserved"},
        ],
        actionType="delete",
        options=[],
    )


def test_delete_entities_without_entities(monkeypatch, cb_manager):
    get_entities_mock = Mock(return_value=[])
    send_batch_mock = Mock()

    monkeypatch.setattr(cb_manager, "get_entities", get_entities_mock)
    monkeypatch.setattr(cb_manager, "send_batch", send_batch_mock)

    cb_manager.delete_entities(
        service="medioambiente",
        subservice="/MedioAmbiente",
    )

    send_batch_mock.assert_called_once_with(
        service="medioambiente",
        subservice="/MedioAmbiente",
        auth=None,
        entities=[],
        actionType="delete",
        options=[],
    )


def test_send_batch_successfully(monkeypatch, cb_manager):
    entities = [
        {"id": "1", "type": "AirQualityObserved"},
        {"id": "2", "type": "AirQualityObserved"},
    ]

    send_mock = Mock(return_value=True)
    monkeypatch.setattr(
        cb_manager,
        "_cbManager__send_batch",
        send_mock,
    )

    result = cb_manager.send_batch(
        service="medioambiente",
        subservice="/MedioAmbiente",
        entities=entities,
    )

    assert result is True

    send_mock.assert_called_once_with(
        auth=None,
        service="medioambiente",
        subservice="/MedioAmbiente",
        entities=entities,
        actionType="append",
        options=[],
    )


def test_send_batch_splits_entities_by_batch_size(monkeypatch, cb_manager):
    cb_manager.batch_size = 2

    entities = [
        {"id": "1", "type": "AirQualityObserved"},
        {"id": "2", "type": "AirQualityObserved"},
        {"id": "3", "type": "AirQualityObserved"},
    ]

    send_mock = Mock(return_value=True)
    monkeypatch.setattr(
        cb_manager,
        "_cbManager__send_batch",
        send_mock,
    )

    result = cb_manager.send_batch(
        service="medioambiente",
        subservice="/MedioAmbiente",
        entities=entities,
    )

    assert result is True
    assert send_mock.call_count == 2

    assert send_mock.call_args_list[0].kwargs["entities"] == entities[:2]
    assert send_mock.call_args_list[1].kwargs["entities"] == entities[2:]


def test_send_batch_error(monkeypatch, cb_manager):
    entities = [{"id": "1", "type": "AirQualityObserved"}]

    send_mock = Mock(side_effect=Exception("Error al enviar lote"))
    monkeypatch.setattr(
        cb_manager,
        "_cbManager__send_batch",
        send_mock,
    )

    with pytest.raises(Exception, match="Error al enviar lote"):
        cb_manager.send_batch(
            service="medioambiente",
            subservice="/MedioAmbiente",
            entities=entities,
        )


def test_send_batch_returns_false(monkeypatch, cb_manager):
    cb_manager.batch_size = 1

    send_mock = Mock(return_value=False)
    monkeypatch.setattr(
        cb_manager,
        "_cbManager__send_batch",
        send_mock,
    )

    result = cb_manager.send_batch(
        service="medioambiente",
        subservice="/MedioAmbiente",
        entities=[{"id": "1", "type": "AirQualityObserved"}],
    )

    assert result is False
    send_mock.assert_called_once()


def test_update_subscription_successfully(cb_manager):
    response = Mock()
    response.status_code = 204

    cb_manager.session.patch.return_value = response

    result = cb_manager.update_subscription(
        subscription_id="subscription-123",
        payload={"status": "inactive"},
        service="medioambiente",
        subservice="/MedioAmbiente",
    )

    assert result is True

    cb_manager.session.patch.assert_called_once_with(
        "http://context-broker:0000/v2/subscriptions/subscription-123",
        json={"status": "inactive"},
        headers={
            "Content-Type": "application/json",
            "Fiware-Service": "medioambiente",
            "Fiware-ServicePath": "/MedioAmbiente",
        },
        verify=False,
        timeout=10,
    )


def test_update_subscription_without_id(cb_manager):
    with pytest.raises(
        ValueError,
        match="You must define <<subscription_id>>",
    ):
        cb_manager.update_subscription(
            subscription_id="",
            payload={"status": "inactive"},
            subservice="/MedioAmbiente",
        )

    cb_manager.session.patch.assert_not_called()


@pytest.mark.parametrize("payload", [None, {}, "inactive", []])
def test_update_subscription_with_invalid_payload(cb_manager, payload):
    with pytest.raises(
        ValueError,
        match="You must define a valid <<payload>>",
    ):
        cb_manager.update_subscription(
            subscription_id="subscription-123",
            payload=payload,
            subservice="/MedioAmbiente",
        )

    cb_manager.session.patch.assert_not_called()


def test_update_subscription_without_subservice(cb_manager):
    with pytest.raises(
        ValueError,
        match="You must define <<subservice>>",
    ):
        cb_manager.update_subscription(
            subscription_id="subscription-123",
            payload={"status": "inactive"},
        )

    cb_manager.session.patch.assert_not_called()


def test_update_subscription_http_error(cb_manager):
    response = Mock()
    response.status_code = 500
    response.json.return_value = {
        "name": "InternalServerError",
        "message": "Error al actualizar suscripción",
    }

    cb_manager.session.patch.return_value = response

    with pytest.raises(exceptions.FetchError):
        cb_manager.update_subscription(
            subscription_id="subscription-123",
            payload={"status": "inactive"},
            service="medioambiente",
            subservice="/MedioAmbiente",
        )

    cb_manager.session.patch.assert_called_once()


def test_update_subscription_with_authentication(cb_manager):
    auth_mock = Mock()
    auth_mock.service = "medioambiente"
    auth_mock.subservice = "/MedioAmbiente"
    auth_mock.tokens = {"/MedioAmbiente": "token-prueba"}

    response = Mock()
    response.status_code = 204
    cb_manager.session.patch.return_value = response

    result = cb_manager.update_subscription(
        subscription_id="subscription-123",
        payload={"status": "inactive"},
        auth=auth_mock,
    )

    assert result is True

    cb_manager.session.patch.assert_called_once()

    call_args = cb_manager.session.patch.call_args
    headers = call_args.kwargs["headers"]

    assert headers["Fiware-Service"] == "medioambiente"
    assert headers["Fiware-ServicePath"] == "/MedioAmbiente"
    assert headers["X-Auth-Token"] == "token-prueba"

    auth_mock.get_auth_token_subservice.assert_not_called()
