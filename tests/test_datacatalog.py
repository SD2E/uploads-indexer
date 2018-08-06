"""Check the functions of the datacatalog submodule"""
import os
import sys
import yaml
import json
from jsonschema import validate, ValidationError

CWD = os.getcwd()
HERE = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)

sys.path.insert(0, PARENT)
sys.path.insert(0, HERE)
from fixtures import credentials, agave, settings

sys.path.insert(0, '/')
import datacatalog


def test_constants():
    '''Constants are present'''
    import uuid
    members = dir(datacatalog.constants)
    assert 'DNS_FOR_NAMESPACE' in members
    assert 'UUID_NAMESPACE' in members
    assert 'STORAGE_ROOT' in members
    assert isinstance(datacatalog.UUID_NAMESPACE, uuid.UUID)

def test_db_connection(settings):
    '''MongoDb connection can be made'''
    db = datacatalog.db_connection(settings['mongodb'])
    colls = db.collection_names()
    assert colls is not None

def test_catalog_uuid():
    '''Verifies that the namespaced UUID function ignores leading slashes'''
    expected_result = '193dfc77-ddea-5ea7-8fff-550ffc518e94'
    assert datacatalog.catalog_uuid(
        '/watch/the/leather/man.txt') == expected_result
    assert datacatalog.catalog_uuid(
        'watch/the/leather/man.txt') == expected_result

def test_init_CatalogStore(settings):
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    assert cata.base is not None
    assert cata.store is not None

def test_abspath(settings):
    expected_result = './tests/store/uploads/201808/protein.png'
    test_data = '201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    abspath = cata.abspath(test_data)
    assert abspath == expected_result

def test_normalize(settings):
    expected_result = '201808/protein.png'
    test_data = '/uploads/201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    npath = cata.normalize(test_data)
    assert npath == expected_result

def test_new_record(settings):
    expected_result = {}
    test_data = '201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    rec = cata.new_record(test_data)
    assert isinstance(rec, dict)


def test_update_record(settings):
    import copy
    from time import sleep
    expected_result = {}
    test_data = '201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    rec1 = cata.new_record(test_data)
    rec1a = copy.deepcopy(rec1)
    sleep(1)
    rec2 = cata.update_record(test_data, rec1)
    assert rec1a['properties']['revision'] != rec2['properties']['revision']
    assert rec1a['properties']['modified_date'] != rec2['properties']['modified_date']

def test_create_record(settings):
    from pymongo import results
    test_data = '201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    rec = cata.create_update_record(test_data)
    assert isinstance(rec, results.InsertOneResult)

def test_update_record(settings):
    from pymongo import results
    from time import sleep
    sleep(1)
    test_data = '201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    rec = cata.create_update_record(test_data)
    assert rec['properties']['revision'] > 0

def test_delete_record(settings):
    test_data = '201808/protein.png'
    cata = datacatalog.CatalogStore(
        settings['mongodb'], settings['catalogstore'])
    resp = cata.delete_record(test_data)
    assert resp['n'] > 0

