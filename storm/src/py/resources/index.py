# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import TransportError
from storm import Bolt, log


class ESIndexBolt(Bolt):
    def initialize(self, conf, context):
        log('Bolt id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        ic = IndicesClient(self.es)

        try:
            ic.get_mapping(
                index='observations',
                doc_type='user'
                )
        except NotFoundError:
            doc_type = {
                'user': {
                    'properties': {
                        'events': {
                            'type': 'nested'
                            }
                        }
                    }
                }
            ic.put_mapping(
                index='observations',
                ignore_conflicts=True,
                doc_type='user',
                body=doc_type
                )

    def process(self, tup):
        event = dict(
            timestamp=tup.values[0],
            path=tup.values[1]
            )

        kwargs = dict(
            id=tup.values[2]
            )

        try:
            events = self.es.get('observations', tup.values[2], 'user')
            body = {'events': events['_source']['events'] + [event]}
        except NotFoundError:
            kwargs['op_type'] = 'create'
            body = {'events': [event]}
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[TransportError] Get failed: %s' % e)
            return

        try:
            self.es.index('observations', 'user', body, **kwargs)
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[TransportError] Index failed: %s' % e)

ESIndexBolt().run()
