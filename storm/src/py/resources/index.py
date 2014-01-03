# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from storm import Bolt, log


class ESIndexBolt(Bolt):
    def initialize(self, conf, context):
        log('bolt initializing')
        self.es = Elasticsearch()

    def process(self, tup):
        event = dict(
            timestamp=int(tup.values[0]) * 1000,
            path=tup.values[1]
            )

        params = dict(
            index='observations',
            id=tup.values[2],
            doc_type='user'
            )

        try:
            events = self.es.get(**params)['_source']['history']
            params['body'] = {'history': events + [event]}
        except NotFoundError:
            params['body'] = {'history': [event]}
            params['op_type'] = 'create'

        self.es.index(**params)

ESIndexBolt().run()
