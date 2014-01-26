# -*- coding: utf-8 -*-

"""
    zeit.recommend.user
    ~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import TransportError
from elasticsearch.exceptions import ConnectionError
from storm import Bolt, log, emit
from datetime import date
import math
import re


class UserIndexBolt(Bolt):
    def initialize(self, conf, context):
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
            log('[UserIndexBolt] ConnectionError, index unreachable: %s' % e)

        if not ic.exists_type(index=self.index, doc_type='user'):
            # TODO: Map out properties of nested events.
            body = {
                'user': {
                    'properties': {
                        'events': {'type': 'nested'},
                        'rank': {'type': 'float', 'store': 'yes'}
                        },
                    '_boost': {
                        'name': 'rank',
                        'null_value': 0.1
                        }
                    }
                }
            ic.put_mapping(
                index=self.index,
                ignore_conflicts=True,
                doc_type='user',
                body=body
                )

    def process(self, tup):
        path = tup.values[1].rstrip('/')
        if self.match(path):
            path = path.rsplit('/', 1)[0]

        event = dict(
            timestamp=tup.values[0],
            path=path
            )

        kwargs = dict(
            id=tup.values[2]
            )

        try:
            events = self.es.get('_all', kwargs['id'], 'user')
            body = {'events': events['_source']['events'] + [event]}
        except NotFoundError:
            kwargs['op_type'] = 'create'
            body = {'events': [event]}
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[UserIndexBolt] TransportError, get failed: %s' % e)
            return

        events = list(i['path'] for i in body['events'])
        # Emitting   (user, events  )
        # Encoded as (user, (path*))
        emit([kwargs['id'], events])

        try:
            body['rank'] = math.log10(len(events)) / 2
            self.es.index(self.index, 'user', body, **kwargs)
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[UserIndexBolt] TransportError, index failed: %s' % e)

if __name__ == '__main__':
    UserIndexBolt().run()
