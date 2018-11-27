import os
import json
import re
from time import sleep
from random import random, shuffle

from attrdict import AttrDict
from reactors.runtime import Reactor, agaveutils
from datacatalog.references import ReferenceFixityStore, FileFixtyUpdateFailure
from datacatalog.agavehelpers import AgaveHelper

EXCLUDES = ['\.log$', '\.err$', '\.out$', '^.container']

def main():
    # Message Body:
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

    if not r.validate_message(m):
        r.on_failure('Invalid message received', None)

    agave_uri = m.get('uri')
    generated_by = m.get('generated_by', [])
    r.logger.info('Indexing {}'.format(agave_uri))
    agave_sys, agave_path, agave_file = agaveutils.from_agave_uri(agave_uri)
    agave_full_path = os.path.join(agave_path, agave_file)

    ah = AgaveHelper(client=r.client)
    to_index = []
    if ah.isfile(agave_full_path):
        # INDEX THE FILE
        store = ReferenceFixityStore(mongodb=r.settings.mongodb,
                                     config=r.settings.get('catalogstore', {}))
        try:
            resp = store.index(agave_full_path, storage_system=agave_sys, generated_by=generated_by)
            r.logger.debug('Indexed {} as uuid:{}'.format(
                os.path.basename(agave_uri), resp.get('uuid', None)))
        except Exception as exc:
            r.on_failure('Indexing failed for {}'.format(agave_full_path), exc)
    else:
        # LIST DIR AND FIRE OFF INDEX TASKS
        r.logger.debug('Recursively listing {}'.format(agave_full_path))
        to_index = ah.listdir(agave_full_path, recurse=True, storage_system=agave_sys, directories=False)
        r.logger.info('Found {} files to index'.format(len(to_index)))
        r.logger.debug('Messaging self with indexing jobs')

        # to_list was constructed in listing order, recursively; adding a shuffle
        # spreads the indexing process evenly over all files
        shuffle(to_index)
        batch_sub = 0
        for idxpath in to_index:
            try:
                r.logger.debug('Self, please index {}'.format(idxpath))
                if r.local is False:
                    actor_id = r.uid
                    message = {'uri': 'agave://' + agave_sys + '/' + idxpath,
                               'generated_by': generated_by,
                               '__options': {'parent': agave_uri}}
                    resp = r.send_message(actor_id, message, retryMaxAttempts=3)
                    batch_sub += 1
                    if batch_sub > r.settings.batch.size:
                        batch_sub = 0
                        if r.settings.batch.randomize_sleep:
                            sleep(random() * r.settings.batch.sleep_duration)
                        else:
                            sleep(r.settings.batch.sleep_duration)
                    if 'executionId' in resp:
                        r.logger.debug('Dispatched indexing task for {} in execution {}'.format(idxpath, resp['executionId']))
            except Exception as exc:
                r.logger.critical(
                    'Failed to dispatch indexing task for {}'.format(agave_full_path))

if __name__ == '__main__':
    main()
