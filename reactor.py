import os
import json

from attrdict import AttrDict
from pprint import pprint
from random import random, shuffle
from time import sleep

from reactors.runtime import Reactor, agaveutils
from datacatalog.agavehelpers import AgaveHelper, AgaveHelperException
from datacatalog import FileFixityStore, FileFixtyUpdateFailure


def main():
    # Minimal Message Body:
    # { "uri": "agave://storagesystem/uploads/path/to/target.txt"}

    r = Reactor()
    m = AttrDict(r.context.message_dict)
    # ! This code fixes an edge case and will be moved lower in the stack
    if m == {}:
        try:
            jsonmsg = json.loads(r.context.raw_message)
            m = jsonmsg
        except Exception:
            pass

    # Use JSONschema-based message validator
    if not r.validate_message(m):
        r.on_failure('Invalid message received', None)

    # Rename m.Key so it makes semantic sense elsewhere in the code
    ag_uri = m.get('uri')
    # only_sync = m.get('sync', False)
    # generated_by = m.get('generated_by', [])
    r.logger.info('Processing {}'.format(ag_uri))

    to_process = list()
    ah = AgaveHelper(r.client)
    ag_sys, ag_path, ag_file = agaveutils.from_agave_uri(ag_uri)
    ag_full_relpath = os.path.normpath(os.path.join(ag_path, ag_file))
    posix_src = ah.mapped_posix_path(ag_full_relpath)

    r.logger.debug('POSIX.path: ' + posix_src)

    if ah.isfile(ag_full_relpath, ag_sys):
        # Index the file
        r.logger.info('Indexing {}'.format(ag_file))
        store = FileFixityStore(mongodb=r.settings.mongodb,
                                config=r.settings.get(
                                    'catalogstore', {}))
        try:
            resp = store.create_update_file(ag_full_relpath)
            r.logger.info('Indexed {} as uuid:{}'.format(
                ag_file, resp.get('uuid', None)))
        except Exception as exc:
            r.on_failure('Indexing failed on {}'.format(ag_full_relpath), exc)
    else:
        # LIST DIR; FIRE OFF TASKS FOR FILES
        r.logger.debug('Listing {}'.format(ag_full_relpath))
        to_process = ah.listdir(ag_full_relpath, recurse=True,
                                storage_system=ag_sys, directories=False)
        pprint(to_process)
        r.logger.info(
            'Found {} potential targets'.format(len(to_process)))
        r.logger.debug('Messaging self with index tasks')

        # to_list was constructed in listing order, recursively;
        # adding a shuffle() the processing evenly over all targets
        shuffle(to_process)
        batch_sub = 0
        for procpath in to_process:
            try:
                # Implements sync behavior
                r.logger.debug(
                    'Launch task for {}'.format(procpath))
                actor_id = r.uid
                resp = dict()
                message = {'uri': 'agave://' + ag_sys + procpath}
                if r.local is False:
                    resp = r.send_message(
                        actor_id, message, retryMaxAttempts=3)
                else:
                    pprint(message)
                batch_sub += 1
                if batch_sub > r.settings.batch.size:
                    batch_sub = 0
                    if r.settings.batch.randomize_sleep:
                        sleep(random() * r.settings.batch.sleep_duration)
                    else:
                        sleep(r.settings.batch.sleep_duration)
                if 'executionId' in resp:
                    r.logger.debug('Processing {} with task {}'.format(
                        procpath, resp['executionId']))
            except Exception:
                r.logger.critical(
                    'Failed to launch task for {}'.format(ag_full_relpath))

    #     ############
    # agave_uri = m.get('uri')
    # r.logger.info('Indexing {}'.format(agave_uri))
    # agave_sys, agave_path, agave_file = agaveutils.from_agave_uri(agave_uri)
    # agave_full_path = os.path.join(agave_path, agave_file)

    # store = FileFixityStore(mongodb=r.settings.mongodb,
    #                         config=r.settings.get('catalogstore', {}))

    # try:
    #     resp = store.create_update_file(agave_full_path)
    #     r.logger.info('Indexed {} as uuid:{}'.format(
    #         os.path.basename(agave_uri), resp.get('uuid', None)))
    # except Exception as exc:
    #     r.on_failure('Indexing failed for {}'.format(agave_full_path), exc)


if __name__ == '__main__':
    main()
