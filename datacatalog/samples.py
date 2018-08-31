import json
import copy
from slugify import slugify

from .mongo import db_connection, ReturnDocument, UUID_SUBTYPE
from .utils import catalog_uuid, current_time, time_stamp, validate_file_to_schema
from .dicthelpers import data_merge, dict_compare, filter_dict
from .constants import Constants, Mappings, Enumerations
from .exceptions import *
from .posixhelpers import *


class SampleStore(object):
    def __init__(self, mongodb, config):
        self.db = db_connection(mongodb)
        coll = config['collections']['samples']
        if config['debug']:
            coll = '_'.join([coll, str(time_stamp(rounded=True))])
        self.name = coll
        self.coll = self.db[coll]
        self.base = config['base']
        self.store = config['root']

    def create_update_sample(self, sample, suuid=None):
        # Does the record exist under the current id
        # If yes, fetch it and update it
        #   Increment the revision and modified date
        # Otherwise, create a new record
        #
        # Returns the record on success
        ts = current_time()
        samp_uuid = None
        if suuid is None:
            if 'id' not in sample:
                raise CatalogUpdateFailure('id missing from sample')
            if 'uuid' not in sample:
                samp_uuid = catalog_uuid(sample['id'])
                sample['uuid'] = samp_uuid
            # Default record must have these keys
            template = {'uuid': samp_uuid,
                        'properties': {'created_date': ts,
                                       'modified_date': ts,
                                       'revision': 0},
                        'measurements_ids': []}
            # override skeleton record with right-favoring merge
            sample = data_merge(template, sample)
        else:
            samp_uuid = suuid

        # filter out selected keys
        # "measurements" not needed as we are storing measurement uuids
        sample = filter_dict(sample, ['measurements'])

        # Does record exist already?
        dbsamp = self.coll.find_one({'uuid': samp_uuid})
        if dbsamp is None:
            if suuid is not None:
                if suuid != samp_uuid:
                    raise SampleUpdateFailure('nonexistent uuid specified')
            try:
                result = self.coll.insert_one(sample)
                return self.coll.find_one({'_id': result.inserted_id})
            except Exception:
                raise SampleUpdateFailure('Failed to create sample')
        else:
            if isinstance(dbsamp, dict):
                dbsamp = data_merge(dbsamp, sample)
                dbsamp['properties']['modified_date'] = ts
                dbsamp['properties']['revision'] += 1
                try:
                    uprec = self.coll.find_one_and_replace(
                        {'_id': dbsamp['_id']}, dbsamp,
                        return_document=ReturnDocument.AFTER)
                    return uprec
                except Exception as exc:
                    raise SampleUpdateFailure(
                        'Failed to update existing sample', exc)
            else:
                raise SampleUpdateFailure(
                    'could not access sample to update it')

    def associate_ids(self, samp_uuid, ids):
        identifiers = copy.copy(ids)
        if not isinstance(identifiers, list):
            identifiers = [identifiers]
        meas = {'uuid': samp_uuid,
                'measurements_ids': list(set(identifiers))}
        return self.create_update_sample(meas, suuid=samp_uuid)

    def delete_record(self, sample_id):
        '''Delete record by sample.id'''
        try:
            return self.coll.remove({'id': sample_id})
        except Exception:
            raise SampleUpdateFailure(
                'Failed to delete sample {}'.format(sample_id))

    def normalize(self, filename):
        # Strip leading / and any combination of
        # /uploads/, /uploads, uploads/ since we
        # do not want to reference it
        if filename.startswith('/'):
            filename = filename[1:]
        if filename.startswith(self.store):
            filename = filename[len(self.store):]
        return filename

