import json
import copy
from slugify import slugify

from .mongo import db_connection, ReturnDocument, UUID_SUBTYPE
from .utils import catalog_uuid, current_time, time_stamp, validate_file_to_schema
from .dicthelpers import data_merge, dict_compare, filter_dict
from .constants import Constants, Mappings, Enumerations
from .exceptions import *
from .posixhelpers import *

# FIXME The *Store classes are not DRY at all. Bring a towel.
# FIXME Code relies too much on hard-coded reference integrity

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

    # def new_record(self, measurement):
    #     # FIXME this can't stand - if the measurement is updated with additional properties, the ID will change and so will the computable UUID
    #     meas_id = measurement_id_from_properties(measurement)
    #     uid = catalog_uuid(meas_id)
    #     ts = current_time()
    #     extras = {'uuid': uid,
    #               'properties': {'created_date': ts,
    #                              'modified_date': ts,
    #                              'revision': 0},
    #               'files_ids': [],
    #               'id': meas_id}
    #     rec = data_merge(measurement, extras)
    #     return rec

    # def update_record(self, measurement):
    #     uid = catalog_uuid(measurement['id'])
    #     # Update properties
    #     measurement['uuid'] = uid
    #     measurement['properties']['modified_date'] = current_time()
    #     measurement['properties']['revision'] += 1
    #     return measurement

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
                    raise MeasurementUpdateFailure('nonexistent uuid specified')
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

    # def new_record(self, sample):
    #     try:
    #         uid = catalog_uuid(sample['id'])
    #         ts = current_time()
    #     except Exception as exc:
    #         raise CatalogDataError('Failed to auto-assign values for record', exc)
    #     extras = {'properties': {'created_date': ts,
    #                              'modified_date': ts,
    #                              'revision': 0},
    #               'uuid': uid}
    #     return data_merge(copy.deepcopy(sample), extras)

    # def update_record(self, sample):
    #     # Update properties
    #     sample['properties']['modified_date'] = current_time()
    #     sample['properties']['revision'] += 1
    #     return sample

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

class CatalogStore(object):
    def __init__(self, mongodb, config):
        self.db = db_connection(mongodb)
        coll = config['collections']['files']
        if config['debug']:
            coll = '_'.join([coll, str(time_stamp(rounded=True))])
        self.name = coll
        self.coll = self.db[coll]
        self.base = config['base']
        self.store = config['root']

    def get_fixity_properties(self, filename):
        """Safely try to learn properties of filename
        Params:
            filename (str): a datafile.filename, which is a relative path
        Returns:
            dict containing a datafiles.properties
        """
        absfilename = self.abspath(filename)
        properties = {}
        # file type
        try:
            ftype = get_filetype(absfilename)
            properties['inferred_file_type'] = ftype
        except Exception:
            pass
        # checksum
        try:
            cksum = compute_checksum(absfilename)
            properties['checksum'] = cksum
        except Exception:
            pass
        # size in bytes
        try:
            size = get_size_in_bytes(absfilename)
            properties['size_in_bytes'] = size
        except Exception:
            pass
        return properties

    def new_record(self, record):
        filename = record['filename']
        ts = current_time()

        record['uuid'] = catalog_uuid(filename)
        # update attributes
        record['attributes'] = data_merge(record.get('attributes', {}), {'lab': lab_from_path(filename)})
        # build up properties set if possible
        file_extras = {'created_date': ts, 'modified_date': ts, 'revision': 0, 'original_filename': filename}
        # in-place transform: move 'state' into properties along with file type and fixity
        if 'state' in record:
            file_extras['state'] = record.pop('state')
        file_props = self.get_fixity_properties(filename)
        extras = data_merge(file_extras, file_props)
        # amend existing properties
        props = data_merge(record.get('properties', {}), extras)
        record['properties'] = props
        return record

    def update_record(self, record):
        # Update revision and timestamp
        filename = record['filename']
        record['uuid'] = catalog_uuid(filename)
        ts = current_time()
        rev = record['properties'].get('revision', 0) + 1
        # build up properties if possible
        file_extras = {'modified_date': ts,
                       'revision': rev}
        if 'state' in record:
            file_extras['state'] = record.pop('state')
        # If updating a stub record, which means it might not have a created date
        if 'created_date' not in record['properties']:
            file_extras['created_date'] = ts
        # update fixity properties (in case they've changed due to re-upload etc)
        file_props = self.get_fixity_properties(filename)
        extras = data_merge(file_extras, file_props)
        props = data_merge(record.get('properties', {}), extras)
        record['properties'] = props
        return record

    def create_update_record(self, record):
        """Update the Data Catalog representation of a file

        Parameters:
            record (dict) containing a 'filename' slot
            record (str) containing a filename

        Returns:
            dict of the new or updated record
        """

        if not isinstance(record, (str, dict)):
            raise CatalogDataError('record must be a string or dict')
        if isinstance(record, str):
            record = {'filename': record}
        elif isinstance(record, dict):
            # Accept a string filename as well as the preferred dict structure
            if 'filename' not in record:
                raise CatalogDataError('filename is mandatory')

        filename = self.normalize(record['filename'])
        record['filename'] = filename
        filerec = self.coll.find_one({'filename': filename})

        difft = False
        if isinstance(filerec, dict):
            rec_dict = copy.deepcopy(filerec)
            # Filter out uninformative keys
            for p in ['_id', 'uuid', 'filename']:
                try:
                    rec_dict.pop(p)
                except KeyError:
                    pass
            # Filter out properties we expect to be mutable
            for p in ['created_date', 'modified_date', 'revision']:
                try:
                    rec_dict['properties'].pop(p)
                except KeyError:
                    pass
            difft = dict_compare(rec_dict, record)

        if filerec is None:
            try:
                newrec = self.new_record(record)
                self.coll.insert_one(newrec)
                return newrec
            except Exception:
                raise FileUpdateFailure('Create failed')
        else:
            if difft:
                try:
                    filerec = self.update_record(filerec)
                    updated = self.coll.find_one_and_replace(
                        {'_id': filerec['_id']},
                        filerec,
                        return_document=ReturnDocument.AFTER)
                    return updated
                except Exception:
                    raise FileUpdateFailure('Update failed')
            else:
                return filerec

    def delete_record(self, filename):
        '''Delete record by filename'''
        filename = self.normalize(filename)
        try:
            return self.coll.remove({'filename': filename})
        except Exception:
            raise FileUpdateFailure('Delete failed')

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

    def checkfile(self, filepath):
        '''Check if a filepath exists and is believed by the OS to be a file'''
        full_path = self.abspath(filepath)
        return os.path.isfile(full_path)

def lab_from_path(filename):
    '''Infer experimental lab from a normalized upload path'''
    print('lab_from_path', filename)
    if filename.startswith('/'):
        raise CatalogDataError('"{}" is not a normalized path')
    path_els = splitall(filename)
    print(path_els)
    if path_els[0].lower() in Enumerations.LABPATHS:
        return path_els[0].lower()
    else:
        raise CatalogDataError('"{}" is not a known uploads path'.format(path_els[0]))

def labname_from_path(filename):
    '''Infer experimental lab from a normalized upload path'''
    if filename.startswith('/'):
        raise CatalogDataError('"{}" is not a normalized path')
    path_els = splitall(filename)
    if path_els[0].lower() in Enumerations.LABPATHS:
        return Mappings.LABPATHS.get(path_els[0].lower(), 'Unknown')
    else:
        raise CatalogDataError(
            '"{}" is not a known uploads path'.format(path_els[0]))

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

