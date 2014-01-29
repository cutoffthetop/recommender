# -*- coding: utf-8 -*-

"""
    zeit.recommend.recommendation
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from elasticsearch import Elasticsearch
from storm import Bolt
from storm import emit
from storm import log
import numpy as np
import scipy.linalg
import scipy.spatial


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
        log('[DEBUG] Initializing SVD: Start')
        self.host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        self.port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        base = conf.get('zeit.recommend.svd.base', 18)
        k = conf.get('zeit.recommend.svd.rank', 3)

        # TODO: Make from_ and threshold configurable.
        seed = dict(self.generate_seed(size=base, threshold=0.3))

        self.cols = sorted(set([i for j in seed.values() for i in j]))
        self.rows = sorted(seed.keys())
        self.A = np.empty([len(self.rows), len(self.cols)])

        for r in range(self.A.shape[0]):
            for c in range(self.A.shape[1]):
                self.A[r, c] = float(self.cols[c] in seed[self.rows[r]])

        U, S, V_t = np.linalg.svd(self.A, full_matrices=False)

        self.U_k = U[:, :k]
        self.S_k_inv = np.linalg.inv(np.diag(S)[:k, :k])
        self.V_t_k = V_t[:k, :]
        log('[DEBUG] Initializing SVD: Finish')

    def generate_seed(self, from_=0, size=1000, threshold=0.0):
        body = {
            'query': {
                'filtered': {
                    'query': {
                        'match_all': {}
                    },
                    'filter': {
                        'range': {
                            'user.rank': {
                                'from': threshold
                            }
                        }
                    }
                }
            }
        }
        es = Elasticsearch(hosts=[{'host': self.host, 'port': self.port}])
        results = es.search(body=body, from_=from_, size=size, doc_type='user')
        for h in results['hits']['hits']:
            yield h['_id'], set([e['path'] for e in h['_source']['events']])

    def expand(self, source):
        value_range = range(len(self.cols))
        for row in source.keys():
            column = np.zeros(len(self.cols))
            for i in value_range:
                if self.cols[i] in source[row]:
                    column[i] = 1
            yield column

    def fold_in_item(self, vector):
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )
        item = np.dot(self.S_k_inv, np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([item]).T))

    def fold_in_user(self, vector):
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )
        user = np.dot(self.S_k_inv, np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([user]).T))

    def recommend(self, vector, proximity=1.0):
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )
        query = np.dot(self.S_k_inv, np.dot(self.V_t_k, vector))
        # for row in range(0, self.V_t_k.shape[1]):
        #     distance = scipy.spatial.distance.cosine(query, self.V_t_k[:, row])
        #     if distance >= proximity:
        #         for col in range(len(self.A[row, :])):
        #             if self.A[row, col] > vector[col]:
        #                 yield self.cols[col]

        for col in range(0, self.V_t_k.shape[1]):
            dist = scipy.spatial.distance.cosine(query, self.V_t_k[:, col])
            if dist >= proximity:
                for row in range(len(self.A[:, col])):
                    if self.A[row, col] > vector[row]:
                        yield self.cols[col]

    def process(self, tup):
        try:
            user, events = tup.values
        except TypeError, e:
            # TODO: What happens here?
            log('[RecommendationBolt] TypeError, process failed: %s' % e)
            return

        vector = np.array(self.expand({user: events}).next())
        # TODO: Folding in seems to be broken.
        # self.fold_in_user(vector)

        # TODO: Make proximity configurable.
        recommendations = list(set(self.recommend(vector, proximity=1.1)))[:10]
        events = list(set(events))[:100]

        emit([user, events, recommendations])

if __name__ == '__main__':
    RecommendationBolt().run()
