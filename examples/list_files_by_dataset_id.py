#!/bin/env python
import argparse

from wipac_fc.client import WFCClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset_id', help="Dataset ID", type=int, default=False)
    parser.add_argument('--season', help="Season", type=int, default=False)
    parser.add_argument('--run_id', help="Run ID", type=int, default=False)
    args = parser.parse_args()

    client = WFCClient('http://128.104.5.129', 31135)

    # query = {offline_processing_metadata.season: 2010, offline_processing_metadata.run_id: 116120}
    query = {}
    for parm in ['dataset_id', 'season', 'run_id']:
        if getattr(args, parm):
            query['.'.join(['offline_processing_metadata', parm])] = getattr(args, parm)
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
