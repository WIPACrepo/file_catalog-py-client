#!/bin/env python
import argparse

from wipac_fc.client import WFCClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dataset_id', metavar="Dataset", help="Dataset ID", type=int, nargs=1)
    parser.add_argument('run_id', metavar="Run", help="Run ID", type=int, nargs=1)
    parser.add_argument('event_id', metavar="Event", help="Event ID", type=int, nargs=1)
    args = parser.parse_args()

    client = WFCClient('http://128.104.5.129', 31135)

    query = {}
    for parm in ['dataset_id', 'run_id']:
        query['.'.join(['offline_processing_metadata', parm])] = getattr(args, parm)[0]

    query['offline_processing_metadata.first_event'] = {'$lte': args.event_id[0]}
    query['offline_processing_metadata.last_event'] = {'$gte': args.event_id[0]}

    files = client.get_list(query=query)['_embedded']['files']
    uuids = [f['uuid'] for f in files]
    for uuid in uuids:
        metadata = client.get(uuid)
        if metadata['content_status'] == 'good':
            path = [l['path'] for l in metadata['locations'] if l['site'] == 'Madison']
            if len(path) > 0:
                print path[0]


if __name__ == '__main__':
    main()
