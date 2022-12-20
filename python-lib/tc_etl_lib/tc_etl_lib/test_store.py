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

'''
SQLFile Store tests
'''

import unittest
import textwrap
import os
import tempfile
from pathlib import Path
from .store import sqlFileStore

TestEntities = [
    {
        "id": "id_1",
        "type": "type_A",
        "municipality": {
            "type": "Text",
            "value": "NA"
        },
        "location": {
            "type": "geo:json",
            "value": { "type": "Point", "coordinates": [1, 2] }
        }
    },
    {
        "id": "id_2",
        "type": "type_B",
        "TimeInstant": {
            "type": "DateTime",
            "value": "2022-12-15T18:00:00Z"
        },
        "temperature": {
            "type": "Number",
            "value": 20
        }
    },
    {
        "id": "id_3",
        "type": "type_A",
        "municipality": {
            "type": "Text",
            "value": "NA"
        },
        "location": {
            "type": "geo:json",
            "value": { "type": "Point", "coordinates": [3, 4] }
        }
    },
    {
        "id": "id_4",
        "type": "type_B",
        "TimeInstant": {
            "type": "DateTime",
            "value": "2022-12-15T18:01:00Z"
        },
        "temperature": {
            "type": "Number",
            "value": 21
        }
    },
    {
        "id": "id_5",
        "type": "type_A",
        "municipality": {
            "type": "Text",
            "value": "Alcobendas"
        },
        "location": {
            "type": "geo:json",
            "value": { "type": "Point", "coordinates": [5, 6] }
        }
    },
]


def dedent(text: str) -> str:
    '''Remove leading whitespace'''
    return textwrap.dedent(text.strip("\n"))

class TestSQLFileStore(unittest.TestCase):
    '''Tests for sqlFileStore'''

    def do_test(self, expect: str, append_text: str, **params):
        '''test sqlFileStore with given parameters and expectations'''
        with tempfile.NamedTemporaryFile(mode='w+', encoding='utf-8', delete=False) as tmpFile:
            try:
                tmpFile.close() # we only want the name, not the handle
                if append_text != "":
                    with open(tmpFile.name, "w+", encoding="utf-8") as outfile:
                        outfile.write(dedent(append_text))
                with sqlFileStore(Path(tmpFile.name), **params) as store:
                    store(TestEntities)
                with open(tmpFile.name, "r", encoding="utf-8") as infile:
                    data = infile.read()
                    self.maxDiff = None
                    self.assertEqual(dedent(data).strip(), dedent(expect).strip())
            finally:
                os.unlink(tmpFile.name)

    def test_default_tables(self):
        '''Test SQL File using standard table names'''
        expected = """
        INSERT INTO :target_schema.type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_1','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [1, 2]}'),'NA'),
        ('id_3','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [3, 4]}'),'NA'),
        ('id_5','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [5, 6]}'),'Alcobendas');
        INSERT INTO :target_schema.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20),
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, "", subservice="/testsrv", chunk_size=5)

    def test_schema_tables(self):
        '''Test SQL File using standard table names and custom schema'''
        expected = """
        INSERT INTO schema1.type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_1','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [1, 2]}'),'NA'),
        ('id_3','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [3, 4]}'),'NA'),
        ('id_5','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [5, 6]}'),'Alcobendas');
        INSERT INTO schema1.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20),
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, "", subservice="/testsrv", schema="schema1", chunk_size=5)

    def test_namespaced_tables(self):
        '''Test SQL File using namespaced names'''
        expected = """
        INSERT INTO :target_schema.ns_type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_1','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [1, 2]}'),'NA'),
        ('id_3','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [3, 4]}'),'NA'),
        ('id_5','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [5, 6]}'),'Alcobendas');
        INSERT INTO :target_schema.ns_type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20),
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, "", namespace="ns", subservice="/testsrv")

    def test_rename_entity(self):
        '''Test SQL File overriding some entity tables'''
        expected = """
        INSERT INTO :target_schema.renamed (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_1','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [1, 2]}'),'NA'),
        ('id_3','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [3, 4]}'),'NA'),
        ('id_5','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [5, 6]}'),'Alcobendas');
        INSERT INTO :target_schema.ns_type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20),
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, "", subservice="/testsrv", namespace="ns", table_names={'type_A': 'renamed'})

    def test_skip_entity(self):
        '''Test SQL File skipping some entity types'''
        expected = """
        INSERT INTO :target_schema.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20),
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, "", subservice="/testsrv", table_names={'type_A': None})

    def test_small_chunks(self):
        '''Test SQL File splitting batches in smaller chunks'''
        expected = """
        INSERT INTO :target_schema.type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_1','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [1, 2]}'),'NA'),
        ('id_3','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [3, 4]}'),'NA');
        INSERT INTO :target_schema.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20);
        INSERT INTO :target_schema.type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_5','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [5, 6]}'),'Alcobendas');
        INSERT INTO :target_schema.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, "", subservice="/testsrv", chunk_size=3)

    def test_append(self):
        '''Test SQL File append to existing data'''
        create = """
        CREATE TABLE IF NOT EXISTS myschema.type_a (entityid text, entitytype text, fiwareservicepath text, recvtime timestamp with time zone not null, location geometry, municipality text);
        CREATE TABLE IF NOT EXISTS myschema.type_b (entityid text, entitytype text, fiwareservicepath text, recvtime timestamp with time zone not null, timeinstant timestamp with time zone not null, temperature doule precision);
        """
        expected = """
        CREATE TABLE IF NOT EXISTS myschema.type_a (entityid text, entitytype text, fiwareservicepath text, recvtime timestamp with time zone not null, location geometry, municipality text);
        CREATE TABLE IF NOT EXISTS myschema.type_b (entityid text, entitytype text, fiwareservicepath text, recvtime timestamp with time zone not null, timeinstant timestamp with time zone not null, temperature doule precision);
        INSERT INTO myschema.type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_1','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [1, 2]}'),'NA'),
        ('id_3','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [3, 4]}'),'NA');
        INSERT INTO myschema.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_2','type_B','/testsrv',NOW(),'2022-12-15T18:00:00Z',20);
        INSERT INTO myschema.type_a (entityid,entitytype,fiwareservicepath,recvtime,location,municipality) VALUES
        ('id_5','type_A','/testsrv',NOW(),ST_GeomFromGeoJSON('{"type": "Point", "coordinates": [5, 6]}'),'Alcobendas');
        INSERT INTO myschema.type_b (entityid,entitytype,fiwareservicepath,recvtime,TimeInstant,temperature) VALUES
        ('id_4','type_B','/testsrv',NOW(),'2022-12-15T18:01:00Z',21);
        """
        self.do_test(expected, create, subservice="/testsrv", schema="myschema", chunk_size=3, append=True)
