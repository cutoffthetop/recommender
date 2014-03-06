# -*- coding: utf-8 -*-

"""
    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import date
from time import sleep
from json import dumps

from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch

dest = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
src = Elasticsearch(hosts=[{'host': '217.13.68.236', 'port': 9200}])
index = '%s-%s' % date.today().isocalendar()[:2]
start = 24000


def main(size=100):
    global start

    hits = src.search(
        from_=start,
        size=size,
        doc_type='user'
        )

    start += size
    users = hits['hits']['hits']

    get_action = lambda user: dumps({'index': {'_id': user['_id']}})
    body = [get_action(u) + '\n' + dumps(u['_source']) for u in users]

    status = dest.bulk(
        index=index,
        doc_type='user',
        refresh=True,
        body='\n'.join(body)
        )

    print start, status.get('created', False)


if __name__ == '__main__':
    body = {
        'user': {
            'properties': {
                'events': {'type': 'nested'},
                'rank': {'type': 'float'}
                },
            '_timestamp': {
                'enabled': True
                }
            }
        }

    ic = IndicesClient(dest)

    if not ic.exists(index):
        ic.create(index)

    if not ic.exists_type(index=index, doc_type='user'):
        ic.put_mapping(
            index=index,
            doc_type='user',
            body=body
            )

    while 1:
        try:
            main()
        except KeyboardInterrupt:
            raise SystemExit(0)
        except:
            continue
        finally:
            sleep(0.02)
