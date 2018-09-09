import os
import json
from attrdict import AttrDict
from reactors.runtime import Reactor, agaveutils
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

    agave_uri = m.get('uri')
    r.logger.info('Processing {}'.format(agave_uri))
    agave_sys, agave_path, agave_file = agaveutils.from_agave_uri(agave_uri)
    agave_full_path = os.path.join(agave_path, agave_file)

    store = FileFixityStore(mongodb=r.settings.mongodb,
                            config=r.settings.catalogstore)
    print(store.name)
    try:
        resp = store.create_update_file(agave_full_path)
        r.logger.info('datafile.uuid {} updated'.format(
            resp.get('uuid', None)))
    except Exception as exc:
        r.on_failure('Failed to process {}'.format(agave_full_path), exc)

if __name__ == '__main__':
    main()
