from pymongo import MongoClient, ReturnDocument, ASCENDING
from bson.binary import UUID_SUBTYPE, OLD_UUID_SUBTYPE
try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus

from .exceptions import CatalogDatabaseError

def db_connection(settings):
    '''Get an active MongoDB connection'''
    try:
        uri = "mongodb://%s:%s@%s:%s" % (quote_plus(settings['username']),
                                         quote_plus(settings['password']),
                                         settings['host'],
                                         settings['port'])
        client = MongoClient(uri)
        db = client[settings['database']]
        return db
    except Exception as exc:
        raise CatalogDatabaseError('Unable to connect to database', exc)
