# -*- coding: utf-8 -*-

"""
    zeit.recommend.user
    ~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import date
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import ConnectionError
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import TransportError
from storm import ack
from storm import Bolt
from storm import emit
from storm import fail
from storm import log
import math
import re


class UserIndexBolt(Bolt):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port}])
        self.match = re.compile('seite-[0-9]|komplettansicht').match
        self.index = '%s-%s-test' % date.today().isocalendar()[:2]
        ic = IndicesClient(self.es)

        try:
            if not ic.exists(self.index):
                ic.create(self.index)
        except ConnectionError, e:
            log('[UserIndexBolt] ConnectionError, index unreachable: %s' % e)
            return

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
                        },
                    '_timestamp': {
                        'enabled': True
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
        segments = tup.values[1].rstrip('/').rsplit('/', 1)
        path = segments[0] if self.match(segments[-1]) else '/'.join(segments)

        event = dict(
            timestamp=tup.values[0],
            path=path
            )

        kwargs = dict(
            id=tup.values[2]
            )

        try:
            # TODO: Retrieve users from all indicies.
            events = self.es.get(self.index, kwargs['id'], 'user',
                                 preference='_primary')
            kwargs['version'] = events['_version']
            body = {'events': events['_source']['events'] + [event]}
        except NotFoundError:
            kwargs['op_type'] = 'create'
            body = {'events': [event]}

        try:
            body['rank'] = math.log10(len(body['events'])) / 2
            self.es.index(self.index, 'user', body, **kwargs)
            paths = list(event['path'] for event in body['events'])
            emit([kwargs['id'], paths])
            ack(tup)
        except TransportError:
            fail(tup)


if __name__ == '__main__':
    UserIndexBolt().run()
