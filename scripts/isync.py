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

es = Elasticsearch()
index = '%s-%s' % date.today().isocalendar()[:2]
match = re.compile('seite-[0-9]|komplettansicht').match
newest = datetime.now()
start = 0


def main():
    global newest, start
    url = 'http://217.13.68.229:8983/solr/select'

    date_range = '%s:00Z TO NOW' % newest.isoformat()[:-3]
    params = dict(
        wt='json',
        q='release_date:[%s]' % date_range,
        fl='href,release_date,title,body,teaser_text',
        sort='release_date asc',
        rows='1'
        )
    raw = urlopen(url, urlencode(params))
    docs = json.loads(raw.read())['response']['docs']

    if docs:
        args = docs[0]['release_date'], '%Y-%m-%dT%H:%M:%SZ'
        newest = datetime.strptime(*args)
    else:
        params['q'] = '*:*'
        params['start'] = start
        params['sort'] = 'release_date desc'
        start += 1
        raw = urlopen(url, urlencode(params))
        docs = json.loads(raw.read())['response']['docs']

    args = docs[0]['release_date'], '%Y-%m-%dT%H:%M:%SZ'
    ts = int(datetime.strptime(*args).strftime('%s000'))
    path = docs[0]['href'][18:].rstrip('/')
    if match(path):
        path = path.rsplit('/', 1)[0]

    item = dict(
        path=path,
        title=docs[0].get('title'),
        body=docs[0].get('body'),
        teaser=docs[0].get('teaser_text'),
        timestamp=ts
        )

    print path, es.index(index, 'item', item).get('ok', False)


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
