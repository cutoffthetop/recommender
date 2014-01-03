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

        params = dict(
            index='observations',
            id=tup.values[2],
            doc_type='user'
            )

        try:
            events = self.es.get(**params)['_source']['events']
            params['body'] = {'events': events + [event]}
        except NotFoundError:
            params['body'] = {'events': [event]}
            params['op_type'] = 'create'
        except TransportError, e:
            # TODO: What is going wrong here?
            log('[TransportError] %s' % e)

        self.es.index(**params)

ESIndexBolt().run()
