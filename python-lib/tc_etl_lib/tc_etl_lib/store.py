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
Store API for generic storage of entities
  - orionStore: saves batches to Orion environment
  - sqlFileStore: saves batches to SQL File
'''
from typing import Callable, Dict, Any, Iterable, Sequence, Optional
from contextlib import contextmanager
from pathlib import Path
from collections import defaultdict
import json
import itertools
from .cb import cbManager
from .auth import authManager
import psycopg2


# A Store is a callable where you can save batches of entities.
Store = Callable[[Iterable[Any]], None]

@contextmanager
def orionStore(cb: cbManager, auth: authManager, *, service:str=None, subservice:str=None, actionType:str='append', options:list=[]):
    '''
    Context manager that creates a store to save entities to the given cbManager
    All parameters are the same as for the cbManager.send_batch function
    '''
    def send_batch(entities: Iterable[Any]):
        cb.send_batch(service=service, subservice=subservice, auth=auth, actionType=actionType, options=options, entities=entities)
    yield send_batch

@contextmanager
def sqlFileStore(path: Path, *, subservice:str, schema:str=":target_schema", namespace:str="", table_names:Optional[Dict[str, str]]=None, chunk_size:int=10000, append:bool=False):
    '''
    Context manager that creates a store to save entities to an SQL File.
    SQL syntax used is postgresql.

    subservice: name of the orion subservice
    schema: name of the schema. Defaults to `:target_schema` (a psql variable)
    namespace: optional prefix for all tables. Table name for each entity type defaults to "namespace_entityType"
    table_names: dict to override default table names.
      - if table_name[entityType] exists and is not empty, it is used as table name for the entity type.
      - if table_name[entityType] does not exist, the default table name (namespace_entityType) is used.
      - if table_name[entityType] exists and is empty, entities with the given type are not saved.
    chunk_size: maximum lines in a single insert statement. Default=10000
    append: append to the file instead of overwriting.
    '''
    mode = "a+" if append else "w+"
    handler = path.open(mode=mode, encoding="utf-8")
    some_table_names = table_names or {} # make sure it is not None
    try:
        def send_batch(entities: Iterable[Any]):
            for chunk in iter_chunk(entities, chunk_size):
                handler.write(sqlfile_batch(schema=schema, namespace=namespace, table_names=some_table_names, subservice=subservice, entities=chunk))
                handler.write("\n")
        yield send_batch
    finally:
        handler.close()


def iter_chunk(iterable, chunk_size: int):
    '''
    Chunks an iterable into smaller iterables.
    
    chunk_size: size of the smaller chunks
    '''
    # See https://stackoverflow.com/questions/8991506/iterate-an-iterator-by-chunks-of-n-in-python
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, chunk_size)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)

def sql_escape(obj: Any) -> str:
    '''Escapes a value to be used in a SQL string'''
    # See https://github.com/psycopg/psycopg2/issues/331
    adaptor = psycopg2.extensions.adapt(obj)
    if hasattr(adaptor, 'encoding'):
        adaptor.encoding = 'utf-8'
    return adaptor.getquoted().decode('utf-8')

def sqlfile_values(subservice: str, entity: Dict[str, Any], fields: Iterable[str]) -> str:
    '''
    Generates a string suitable for SQL insert, with all values of the entity
    
    subservice: subservice name
    entity: ngsi entity
    fields: list of fields to save in the entity (omitting id, type)'''
    sql = [
        sql_escape(entity['id']),
        sql_escape(entity['type']),
        sql_escape(subservice),
        "NOW()"
    ]
    for field in fields:
        entry = entity.get(field, {'type':'Text', 'value': None})
        value_untyped = entry.get('value', None)
        value: str
        if value_untyped is None:
            value = "NULL"
        elif 'json' in entry['type']:
            value = sql_escape(json.dumps(value_untyped))
            if 'geo' in entry['type']:
                value = f"ST_GeomFromGeoJSON({value})"
        else:
            value = sql_escape(value_untyped)
        sql.append(value)
    return f"({','.join(sql)})"

def sqlfile_insert(subservice: str, table_name: str, fields: Sequence[str], entities: Iterable[Any]) -> str:
    '''
    Generate SQL INSERT lines from sequence of entities
    
    subservice: subservice name
    table_name: SQL table name to use
    fields: sequence of attribute names
    entities: iterable of entities
    '''
    return "\n".join((
        f"INSERT INTO {table_name} (entityid,entitytype,fiwareservicepath,recvtime,{','.join(fields)}) VALUES",
        ",\n".join(
            sqlfile_values(subservice, entity, fields)
            for entity in entities
        ) + ";"
    ))

def sql_table_name(schema: str, namespace: str, entity_type: str, table_names: Dict[str, str]) -> str:
    '''
    Generates table_name from namespace and entity_type
    
    namespace: namespace to use to prefix entityType
    entity_type: type of entity
    table_names: overrides default names for any entityType
    '''
    default_name = ((namespace + "_") if namespace != "" else "") + entity_type.lower()
    mapped_name  = table_names.get(entity_type, default_name)
    if not mapped_name: # Tables mapped to empty string or None are not saved to SQL
        return ""
    # Tables mapped to some name are prefixed with schema name
    return f"{schema}.{mapped_name}"

def sqlfile_batch(schema: str, namespace: str, table_names: Dict[str, str], subservice: str, entities: Iterable[Any]) -> str:
    '''
    Generate a single SQL insert batch statement

    schema: schema name
    namespace: namespace for table names
    table_names: overrides entytyType to table name defaults
    subservice: subservice name
    entities: Iterable of entities
    '''
    # Group entities by type
    entities_by_type = defaultdict(list)
    for entity in entities:
        entities_by_type[entity['type']].append(entity)
    # For each type, get the fieldset
    fields_by_type = defaultdict(set)
    for entity_type, typed_entities in entities_by_type.items():
        for entity in typed_entities:
            for field in entity:
                if field not in ('id', 'type'):
                    fields_by_type[entity_type].add(field)
    # Generate all rows for each type
    sql = []
    for entity_type, typed_entities in sorted(entities_by_type.items()):
        table_name = sql_table_name(schema, namespace, entity_type, table_names)
        if table_name:
            table_cols = sorted(fields_by_type[entity_type])
            sql.append(sqlfile_insert(subservice, table_name, table_cols, typed_entities))
    # and return SQL insert code
    return "\n".join(sql)
