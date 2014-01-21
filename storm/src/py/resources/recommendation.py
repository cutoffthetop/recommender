# -*- coding: utf-8 -*-

"""
    zeit.recommend.recommendation
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from elasticsearch import Elasticsearch
from storm import Bolt, log, emit
import numpy as np
import scipy.spatial
import scipy.linalg


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
        self.host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        self.port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        base = conf.get('zeit.recommend.svd.base', 18)
        k = conf.get('zeit.recommend.svd.rank', 3)

        seed = dict(self.generate_seed(size=base))

        self.cols = sorted(set([i for j in seed.values() for i in j]))
        self.rows = sorted(seed.keys())
        self.A = np.empty([len(self.rows), len(self.cols)])

        # expand()
        for r in range(self.A.shape[0]):
            for c in range(self.A.shape[1]):
                self.A[r, c] = float(self.cols[c] in seed[self.rows[r]])

        U, S, V_t = np.linalg.svd(self.A, full_matrices=False)

        self.U_k = U[:, :k]
        self.S_k = np.diag(S)[:k, :k]
        self.V_t_k = V_t[:k, :]

    def generate_seed(self, from_=1000, size=1000, threshold=1):
        # TODO: Implement threshold via DSL script filter.
        es = Elasticsearch(hosts=[{'host': self.host, 'port': self.port}])
        for hit in es.search(from_=from_, size=size)['hits']['hits']:
            if 'events' not in hit['_source']:
                continue
            events = set([i['path'] for i in hit['_source']['events']])
            if len(events) >= threshold:
                yield hit['_id'], events

    def neighborhood(self, vector):
        U_k_S_k_sqrt = np.dot(self.U_k, scipy.linalg.sqrtm(self.S_k))
    
        query = np.dot(np.linalg.inv(self.S_k), np.dot(self.V_t_k, vector))

    def expand(self, source):
        value_range = range(len(self.cols))
        for row in source.keys():
            column = np.zeros(len(self.cols))
            for i in value_range:
                if self.cols[i] in source[row]:
                    column[i] = 1
            yield column

    def fold_in(self, vector):
        # TODO: Fold into which axis?
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )
        item = np.dot(np.linalg.inv(self.S_k), np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([item]).T))

    def recommend(self, vector, size=10):
        # TODO: Isn't np.linalg.inv(self.S_k) cacheable?
        query = np.dot(np.linalg.inv(self.S_k), np.dot(self.V_t_k, vector))

        distances = list()
        for row in range(0, self.V_t_k.shape[1]):
            dist = scipy.spatial.distance.cosine(query, self.V_t_k[:, row])
            if dist >= 0:
                try:
                    distances.append((row, dist))
                except:
                    print 'continue'

        return sorted(distances, key=lambda tup: tup[1])#[-size:]

    def process(self, tup):
        try:
            user, events = tup.values
        except TypeError, e:
            # TODO: What happens here?
            log('[RecommendationBolt] TypeError, process failed: %s' % e)
            return

        vector = np.array(self.expand({user: events}).next())
        self.fold_in(vector)
        recommendations = self.recommend(vector)
        # Emitting   (user, events,   recommendations  )
        # Encoded as (user, (event*), (recommendation*))
        emit([user, events, recommendations])

if __name__ == '__main__':
    RecommendationBolt().run()
