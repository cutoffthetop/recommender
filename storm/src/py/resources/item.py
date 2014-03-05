# -*- coding: utf-8 -*-

"""
    zeit.recommend.item
    ~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import date
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import ConnectionError
from storm import Bolt
from storm import emit
from storm import log
from storm import ack
from storm import fail
from urllib import urlencode
from urllib import urlopen
import json
import re


class ItemIndexBolt(Bolt):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 8983)
        self.url = 'http://%s:%s/solr/select' % (host, port)
        host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port}])
        self.match = re.compile('seite-[0-9]|komplettansicht').match
        self.index = '%s-%s' % date.today().isocalendar()[:2]
        ic = IndicesClient(self.es)

        try:
            if not ic.exists(self.index):
                ic.create(self.index)
        except ConnectionError, e:
            log('[ItemIndexBolt] ConnectionError, index unreachable: %s' % e)

        if not ic.exists_type(index=self.index, doc_type='item'):
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
            ic.put_mapping(
                index=self.index,
                ignore_conflicts=True,
                doc_type='item',
                body=body
                )

    def get_doc(self, path):
        params = dict(
            wt='json',
            q='href:*%s' % path,
            fl='title,teaser_text,release_date,body'
            )
        raw = urlopen(self.url, urlencode(params))
        data = raw.read()
        docs = json.loads(data)['response']['docs']
        return None if not len(docs) else docs[0]

    def process(self, tup):
        path = tup.values[0].rstrip('/')
        if self.match(path):
            path = path.rsplit('/', 1)[0]

        doc = self.get_doc(path)
        if doc:
            time = datetime.strptime(doc['release_date'], '%Y-%m-%dT%H:%M:%SZ')
            body = dict(
                path=path,
                title=doc['title'],
                body=doc['body'],
                teaser=doc['teaser_text'],
                timestamp=int(time.strftime("%s000"))
                )
            status = self.es.index(self.index, 'item', body)
            log(str(status))
            if status.get('ok', False):
                emit([path])
                ack(tup)
                return
        fail(tup)


if __name__ == '__main__':
    ItemIndexBolt().run()
