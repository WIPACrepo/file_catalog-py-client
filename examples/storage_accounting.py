#!/bin/env python
import argparse

from datetime import datetime
from wipac_fc.client import WFCClient


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--begin_date', help="Begin Date (YYYY-MM-DDTHH:MM:SS)",
                        type=lambda d: datetime.strptime(d, '%Y-%m-%dT%H:%M:%S'), default=False)
    parser.add_argument('--end_date', help="End Date (YYYY-MM-DDTHH:MM:SS)",
                        type=lambda d: datetime.strptime(d, '%Y-%m-%dT%H:%M:%S'), default=False)
    parser.add_argument('--logical_name', help="Mongo Regex", type=str, default=False)
    parser.add_argument('--data_type', help="Data Type", type=str, default='real',
                        choices=['real', 'simulation'])
    args = parser.parse_args()

    client = WFCClient('http://128.104.5.129', 31135)

    total_size = 0

    query = {'data_type': args.data_type}
    if args.begin_date or args.end_date:
        query['create_date'] = {}
        if args.begin_date:
            query['create_date']['$gte'] = '{}'.format(args.begin_date.isoformat())
        if args.end_date:
            query['create_date']['$lt'] = '{}'.format(args.end_date.isoformat())

    if args.logical_name:
        query['logical_name'] = {'$regex': args.logical_name}

    files = client.get_list(query=query)['_embedded']['files']
    uuids = [f['uuid'] for f in files]
    for uuid in uuids:
        metadata = client.get(uuid)
        total_size += metadata['file_size']

    print 'Total Files: {} Total Size: {}GBs'.format(len(uuids), total_size/(1024.0**3))


if __name__ == '__main__':
    main()
