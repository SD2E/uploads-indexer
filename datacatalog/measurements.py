import json
import copy
from slugify import slugify

from .mongo import db_connection, ReturnDocument, UUID_SUBTYPE
from .utils import catalog_uuid, current_time, time_stamp, validate_file_to_schema
from .dicthelpers import data_merge, dict_compare, filter_dict
from .constants import Constants, Mappings, Enumerations
from .exceptions import *
from .posixhelpers import *


class MeasurementStore(object):
    def __init__(self, mongodb, config):
        self.db = db_connection(mongodb)
        coll = config['collections']['measurements']
        if config['debug']:
            coll = '_'.join([coll, str(time_stamp(rounded=True))])
        self.name = coll
        self.coll = self.db[coll]
        self.base = config['base']
        self.store = config['root']

    def create_update_measurement(self, measurement, muuid=None):
        # Does the record exist under the current id
        # If yes, fetch it and update it
        #   Increment the revision and modified date
        # Otherwise, create a new record
        #
        # Returns the record on success
        ts = current_time()
        meas_id, meas_uuid = None, None
        if muuid is None:
            if 'id' not in measurement:
                if 'measurement_id' in measurement:
                    meas_id = measurement_id_from_properties(measurement)
                elif 'files' in measurement:
                    meas_id = measurement_id_from_files(measurement)
                else:
                    meas_id = meas_id = measurement_id_from_properties(
                        measurement)
                measurement['id'] = meas_id
            if 'uuid' not in measurement:
                meas_uuid = catalog_uuid(meas_id)
                measurement['uuid'] = meas_uuid
            # Default record must have these keys
            template = {'uuid': meas_uuid,
                        'properties': {'created_date': ts,
                                       'modified_date': ts,
                                       'revision': 0},
                        'files_ids': [],
                        'id': meas_id}
            # override skeleton record with right-favoring merge
            measurement = data_merge(template, measurement)
        else:
            meas_uuid = muuid

        # filter out selected keys:
        # "files" not needed because we're storing file uuid references
        measurement = filter_dict(measurement, ['files'])

        # Does record exist already?
        dbmeas = self.coll.find_one({'uuid': meas_uuid})
        if dbmeas is None:
            if muuid is not None:
                if muuid != meas_uuid:
                    raise MeasurementUpdateFailure(
                        'nonexistent uuid specified')
            try:
                result = self.coll.insert_one(measurement)
                return self.coll.find_one({'_id': result.inserted_id})
            except Exception:
                raise MeasurementUpdateFailure('Failed to create measurement')
        else:
            if isinstance(dbmeas, dict):
                dbmeas = data_merge(dbmeas, measurement)
                dbmeas['properties']['modified_date'] = ts
                dbmeas['properties']['revision'] += 1
                try:
                    uprec = self.coll.find_one_and_replace(
                        {'_id': dbmeas['_id']}, dbmeas,
                        return_document=ReturnDocument.AFTER)
                    return uprec
                except Exception as exc:
                    raise MeasurementUpdateFailure(
                        'Failed to update existing measurement', exc)
            else:
                raise MeasurementUpdateFailure(
                    'could not access measurement to update it')

    def delete_record(self, measurement_id):
        '''Delete record by measurement.id'''
        try:
            meas_uuid = catalog_uuid(measurement_id)
            return self.coll.remove({'uuid': measurement_id})
        except Exception:
            raise MeasurementUpdateFailure(
                'Failed to delete measurement {}'.format(meas_uuid))

    def associate_ids(self, meas_uuid, ids):
        identifiers = copy.copy(ids)
        if not isinstance(identifiers, list):
            # using list on a str will return a character iterator
            identifiers = [identifiers]
        meas = {'uuid': meas_uuid,
                'files_ids': list(set(identifiers))}
        return self.create_update_measurement(meas, muuid=meas_uuid)

    def normalize(self, filename):
        # Strip leading / and any combination of
        # /uploads/, /uploads, uploads/ since we
        # do not want to reference it
        if filename.startswith('/'):
            filename = filename[1:]
        if filename.startswith(self.store):
            filename = filename[len(self.store):]
        return filename


def measurement_id_from_files(measurement, prefix=None):
    '''Returns a unique measurement identifier from linked files'''

    mprefix = 'files:'
    if prefix is not None:
        mprefix = '.'.join([prefix, mprefix])

    file_refs = []
    for f in measurement['files']:
        if 'file_id' in f:
            file_refs.append(str(f['file_id']).lower())
        elif 'name' in f:
            file_refs.append(str(f['name']).lower())
    return mprefix + '|'.join(sorted(file_refs))


def measurement_id_from_properties(measurement, prefix=None):
    '''Returns a unique measurement identifier'''
    # # Rely on lab-provided identifier if present
    # if 'measurement_id' in measurement:
    #     return 'measurement_id:' + str(measurement['measurement_id'])
    mprefix = 'properties:'
    if prefix is not None:
        mprefix = '.'.join([prefix, mprefix])

    meas = copy.deepcopy(measurement)
    # Exclude uninformative keys
    # measurement_name is included because its a free-text field
    # the *ids and *uuids fields are checked by set comparison
    for exclude in ['sample_ids', 'files_uuids', 'measurement_name']:
        try:
            meas.pop(exclude)
        except KeyError:
            pass
    kvlist = []
    if prefix is not None:
        kvlist.append(prefix.lower())
    for k in sorted(meas):
        if not isinstance(meas[k], (dict, list, tuple)):
            kvlist.append(k + ':' + slugify(meas[k]))
    joined = mprefix + '|'.join(kvlist)
    return joined
