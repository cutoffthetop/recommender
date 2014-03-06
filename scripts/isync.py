# -*- coding: utf-8 -*-

"""
    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import date
from datetime import datetime
from time import sleep
from urllib import urlencode
from urllib import urlopen
import json
import re

from elasticsearch.client import IndicesClient
from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
url = 'http://217.13.68.229:8983/solr/select'
index = '%s-%s' % date.today().isocalendar()[:2]
match = re.compile('seite-[0-9]|komplettansicht').match
start = 0


def main():
    global newest, start

    params = dict(
        wt='json',
        q='*:*',
        fl='href,release_date,title,body,teaser_text',
        sort='release_date desc',
        rows='1',
        start=start
        )

    start += 1
    raw = urlopen(url, urlencode(params))
    doc = json.loads(raw.read())['response']['docs'][0]

    args = doc['release_date'], '%Y-%m-%dT%H:%M:%SZ'
    ts = int(datetime.strptime(*args).strftime('%s000'))

    path = doc['href'][18:].rstrip('/')
    if match(path):
        path = path.rsplit('/', 1)[0]

    item = dict(
        path=path,
        title=doc.get('title'),
        body=doc.get('body'),
        teaser=doc.get('teaser_text'),
        timestamp=ts
        )

    print path, es.index(index, 'item', item).get('created', False)


if __name__ == '__main__':
    body = {
        'item': {
            'properties': {
                'path': {
                    'type': 'string',
                    'store': 'yes',
                    'index': 'not_analyzed'
                    },
                'title': {'type': 'string'},
                'body': {'type': 'string'},
                'teaser': {'type': 'string'},
                'timestamp': {'type': 'date'}
                },
            '_id': {'path': 'path'}
            }
        }

    ic = IndicesClient(es)

    if not ic.exists(index):
        ic.create(index)

    if not ic.exists_type(index=index, doc_type='item'):
        ic.put_mapping(
            index=index,
            ignore_conflicts=True,
            doc_type='item',
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
