import arrow
import datetime
import pytz
import json
import uuid
from pymongo import MongoClient, ReturnDocument
try:
    # Python 3.x
    from urllib.parse import quote_plus
except ImportError:
    # Python 2.x
    from urllib import quote_plus
from .constants import *
from .posixhelpers import *
from .constants import *

class CatalogError(Exception):
    pass

class CatalogUpdateFailure(CatalogError):
    # Errors arising when the Data Catalog can't be updated
    pass

class CatalogDataError(CatalogError):
    # Errors arising from computing or validating metadata
    pass

class CatalogDatabaseError(CatalogError):
    # Errors reading to or writing from backing store
    pass


class CatalogStore(object):
    def __init__(self, mongodb, config):
        self.db = db_connection(mongodb)
        self.files = self.db[config['collection']]
        self.base = config['base']
        self.store = config['store']

    def new_record(self, filename):

        absfilename = self.abspath(filename)
        try:
            uid = catalog_uuid(filename)
            ts = datetime.datetime.utcnow()
            ftype = get_filetype(absfilename)
            cksum = compute_checksum(absfilename)
            size = get_size_in_bytes(absfilename)
            lab = lab_from_path(filename)
        except Exception as exc:
            raise CatalogDataError('Failed to compute values for record', exc)

        return {'properties': {'originator_id': None, 'checksum': cksum,
                            'size_in_bytes': size, 'original_filename': filename,
                            'file_type': ftype, 'created_date': ts,
                            'modified_date': ts, 'revision': 0},
                'filename': filename,
                'uuid': uid,
                'attributes': {'lab': lab},
                'variables': [],
                'annotations': []}

    def update_record(self, filename, record):
        # Update properties
        absfilename = self.abspath(filename)
        try:
            uid = catalog_uuid(filename)
            ts = datetime.datetime.utcnow()
            ftype = get_filetype(absfilename)
            cksum = compute_checksum(absfilename)
            size = get_size_in_bytes(absfilename)
            lab = lab_from_path(filename)

        except Exception as exc:
            raise CatalogDataError('Failed to compute values for record', exc)

        record['properties']['size_in_bytes'] = size
        record['properties']['checksum'] = cksum
        record['properties']['file_type'] = ftype
        record['properties']['modified_date'] = ts
        record['properties']['revision'] = record['properties']['revision'] + 1
        record['attributes']['lab'] = lab
        return record

    def create_update_record(self, filename):
        # Does the record exist under the current filename
        # If yes, fetch it and update it
        #   Increment the revision and modified date
        # Otherwise, create a new instance
        #
        # Returns the record on success
        filename = self.normalize(filename)
        filerec = self.files.find_one({'filename': filename})
        if filerec is None:
            try:
                newrec = self.new_record(filename)
                self.files.insert_one(newrec)
                return newrec
            except Exception:
                raise CatalogUpdateFailure('Failed to create new record')
        else:
            try:
                filerec = self.update_record(filename, filerec)
                updated =  self.files.find_one_and_replace(
                    {'_id': filerec['_id']},
                    filerec,
                    return_document=ReturnDocument.AFTER)
                return updated
            except Exception:
                raise CatalogUpdateFailure('Failed to update existing record')

    def delete_record(self, filename):
        # Delete record by filename
        filename = self.normalize(filename)
        try:
            return self.files.remove({'filename': filename})
        except Exception:
            raise CatalogUpdateFailure('Failed to remove record')

    def normalize(self, filename):
        # Strip leading / and any combination of
        # /uploads/, /uploads, uploads/ since we
        # do not want to reference it
        if filename.startswith('/'):
            filename = filename[1:]
        if filename.startswith(self.store):
            filename = filename[len(self.store):]
        return filename

    def abspath(self, filename):
        if filename.startswith('/'):
            filename = filename[1:]
        if filename.startswith(self.store):
            filename = filename[len(self.store):]
        return os.path.join(self.base, self.store, filename)

def catalog_uuid(filename):
    '''Returns a UUID5 in the appropriate namespace'''
    if filename.startswith('/'):
        filename = filename[1:]
    if filename.startswith(STORAGE_ROOT):
        filename = filename[len(STORAGE_ROOT):]
    return str(uuid.uuid5(UUID_NAMESPACE, filename))

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

def lab_from_path(filename):
    '''Infer experimental lab from a normalized upload path'''
    if filename.startswith('/'):
        raise CatalogDataError('"{}" is not a normalized path')
    path_els = splitall(filename)
    # presently a 1:1 mapping but could be extended to support
    # transforming from path(s) to lab name
    if path_els[0].lower() in Enumerations.LABS:
        return path_els[0].lower()
    raise CatalogDataError('"{}" is not a known lab'.format(path_els[0]))
