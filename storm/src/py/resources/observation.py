# -*- coding: utf-8 -*-

"""
    zeit.recommend.observation
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import TransportError
from storm import Bolt, log, emit
import math
import re


class ObservationBolt(Bolt):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port}])
        self.match = re.compile('seite-[0-9]|komplettansicht').match
        ic = IndicesClient(self.es)

        if not ic.exists('observations'):
            ic.create('observations')

        if not ic.exists_type(index='observations', doc_type='user'):
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
                index='observations',
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
            events = self.es.get('observations', kwargs['id'], 'user')
            body = {'events': events['_source']['events'] + [event]}
        except NotFoundError:
            kwargs['op_type'] = 'create'
            body = {'events': [event]}
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[ObservationBolt] TransportError, get failed: %s' % e)
            return

        events = list(i['path'] for i in body['events'])
        # Emitting   (user, events  )
        # Encoded as (user, (path*))
        emit([kwargs['id'], events])

        try:
            body['rank'] = math.log10(len(events)) / 2
            self.es.index('observations', 'user', body, **kwargs)
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[ObservationBolt] TransportError, index failed: %s' % e)

if __name__ == '__main__':
    ObservationBolt().run()
