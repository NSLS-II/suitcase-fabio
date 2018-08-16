from ._version import get_versions

__version__ = get_versions()['version']
del get_versions
from typing import Iterator, Tuple, List, Generator
import fabio
import numpy as np
import json
from difflib import SequenceMatcher
import uuid
import os
import time


def export(gen: Iterator[Tuple[str, dict]], handler: fabio.fabioimage) -> List[str]:
    extension = handler.DEFAULT_EXTENSIONS[0]
    descriptors = {}
    fields_of_interest = {}
    startdoc = None
    exportpaths = []

    for name, doc in gen:
        if name == 'start':
            startdoc = doc
        elif name == 'descriptor':
            descriptors[doc["uid"]] = doc
            fields_of_interest[doc["uid"]] = {k for k, v in doc['data_keys'].items() if len(v['shape'] or []) > 1}
        elif name == 'event':
            eventheader = doc.copy()
            del eventheader['data']
            for field in doc['data']:
                if field in fields_of_interest[doc["descriptor"]]:
                    path = f'{doc["uid"]}_{field}.{extension}'
                    writer = handler(data=doc['data'][field], header={'start': startdoc,
                                                                      'event': eventheader,
                                                                      'descriptor': descriptors[doc['descriptor']]})
                    writer.write(path)
                    exportpaths.append(path)
        elif name == 'stop':
            path = f'{doc["uid"]}.json'
            with open(path, 'w') as f:
                json.dump(doc, f)
            exportpaths.append(path)
        else:
            raise KeyError('Malformed header.')

    return exportpaths


def ingest(paths: Iterator[str]) -> Generator[Tuple[str, dict], None, None]:
    if isinstance(paths, list):
        firstpath = paths.pop(0)
    else:
        firstpath = next(paths)

    # Generate start doc
    startuuid = str(uuid.uuid4())
    start = {'time': time.time(),
             'uid': startuuid,
             }

    yield 'start', start

    # Generate descriptor doc
    descriptoruuid = str(uuid.uuid4())
    descriptor = {'data_keys':
                      {'image': {'source': 'file', 'dtype': 'array', 'shape': None}, },
                  'time': time.time(),
                  'uid': descriptoruuid,
                  'start': startuuid,
                  }

    first_event = _gen_event(firstpath, descriptoruuid)
    descriptor['data_keys']['image']['shape'] = first_event['data']['image'].shape
    # Generate first descriptor and event docs and other event docs
    yield 'descriptor', descriptor
    yield 'event', first_event
    for path in paths:
        yield 'event', _gen_event(path, descriptoruuid)

    # Generate stop doc
    stop = {'exit_status': 'success',
            'time': time.time(),
            'uid': str(uuid.uuid4()),
            'start': startuuid,
            }
    yield 'stop', stop


def _gen_event(path: str, descriptor):
    mtime = os.path.getmtime(path)
    fimg = fabio.open(path)
    event = {'data': {'image': fimg.data},
             'timestamps': {'image': mtime, },
             'time': mtime,
             'uid': str(uuid.uuid4()),
             'descriptor': descriptor,
             **fimg.header
             }
    return event


#
#
# def reflect(...):
#     ...
#
#
# handlers = []

from fabio import edfimage
from fabio import tifimage
from fabio import eigerimage
