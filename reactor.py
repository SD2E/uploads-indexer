import os
import json
import re
from time import sleep
from random import random, shuffle

from attrdict import AttrDict
from reactors.runtime import Reactor, agaveutils
from datacatalog.linkedstores.fixity import FixityStore
from datacatalog.agavehelpers import AgaveHelper
from datacatalog.managers.pipelinejobs.indexer import Indexer

EXCLUDES = ['\.log$', '\.err$', '\.out$', '^.container']

def main():

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
    agave_full_path = re.sub('^(/)+', '/', agave_full_path)

    ah = AgaveHelper(client=r.client)
    to_index = []
    if ah.isfile(agave_full_path):
        # INDEX THE FILE
        mgr = Indexer(mongodb=r.settings.mongodb, agave=r.client)
        try:
            mgr.index_if_exists(agave_full_path, storage_system=agave_sys)
        except Exception as exc:
            r.on_failure('Indexing failed for {}'.format(agave_uri, exc))
        # file_store = mgr.stores['file']
        # fixity_store = mgr.stores['fixity']

        # try:
        #     resp = fixity_store.index(agave_full_path, storage_system=agave_sys, generated_by=generated_by)
        #     r.logger.debug('Fixity indexed {} to uuid:{}'.format(
        #         os.path.basename(agave_uri), resp.get('uuid', None)))
        # except Exception as exc:
        #     r.on_failure('Indexing failed for {}'.format(agave_full_path), exc)
    else:
        # LIST DIR AND FIRE OFF INDEX TASKS
        r.logger.debug('Recursively listing {}'.format(agave_full_path))
        to_index = ah.listdir(agave_full_path,
                              recurse=True,
                              storage_system=agave_sys,
                              directories=False)

        r.logger.info('Found {} files to index'.format(len(to_index)))
        r.logger.debug('Messaging self with indexing targets')

        # Contents of to_list are likely to be in a sorted order. Adding a
        # shuffle spreads the indexing process evenly over all indexing targets
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
                    'Failed to launch indexing task for {}: {}'.format(
                        agave_full_path, exc))

if __name__ == '__main__':
    main()
