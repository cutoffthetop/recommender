# -*- coding: utf-8 -*-

"""
    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import date
from time import sleep

from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch

es = Elasticsearch()
index = '%s-%s' % date.today().isocalendar()[:2]
start = 0


def main():
    global start

    src = Elasticsearch(hosts=[{'host': '217.13.68.236', 'port': 9200}])

    hits = src.search(
        from_=start,
        size=1,
        doc_type='user'
        )

    user = hits['hits']['hits'][0]

    status = es.index(
        index=index,
        doc_type='user',
        id=user['_id'],
        body=user['_source'],
        )

    print user['_id'], status.get('ok', False)


if __name__ == '__main__':
    body = {
        'user': {
            'properties': {
                'events': {'type': 'nested'},
                'rank': {'type': 'float', 'store': 'yes'}
                },
            '_boost': {
                'name': 'rank',
                'null_value': 0.1
                },
            '_timestamp': {
                'enabled': True
                }
            }
        }
    IndicesClient(es).put_mapping(
        index=index,
        ignore_conflicts=True,
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
