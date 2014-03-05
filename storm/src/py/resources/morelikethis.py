# -*- coding: utf-8 -*-

"""
    zeit.recommend.morelikethis
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from storm import Bolt
from storm import log
from storm import emit


import time


from elasticsearch import Elasticsearch


class MorelikethisBolt(Bolt):

    connections = {}

    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port}])
        raw = open('./stopwords.txt', 'r').read()
        self.stopwords = raw.decode('utf-8').split('\n')[3:]

    def recommend(self, paths, top_n=10):
        b = {
            'query': {
                'bool': {
                    'should': [
                        {'ids': {'values': paths, 'boost': 1}},
                        {'match_all': {'boost': 0}}
                        ]
                    }
                }
            }
        items = self.es.search(doc_type='item', body=b, size=len(paths) or 3)
        legacy_get = lambda i: i['_source'].get('body', i['_source']['teaser'])
        hits = [legacy_get(i) for i in items['hits']['hits']]

        b = {
            'query': {
                'more_like_this': {
                    'like_text': ' '.join(hits),
                    'stop_words': self.stopwords
                    }
                }
            }
        items = self.es.search(doc_type='item', body=b, fields='', size=top_n)
        return list(i['_id'] for i in items['hits']['hits'])

    def process(self, tup):
        if tup.stream == 'control':
            action, user = tup.values
            if action == 'connect':
                self.connections[user] = int(time.time())
            elif action == 'disconnect':
                del self.connections[user]

        elif tup.stream == 'default':
            user, paths = tup.values
            if user in self.connections:
                log('[MorelikethisBolt] Incoming: %s' % user)

                recommendations = self.recommend(paths)
                paths = list(set(paths))[:10]

                emit([user, paths, recommendations])


if __name__ == '__main__':
    MorelikethisBolt().run()
