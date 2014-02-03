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
import numpy as np
import scipy.linalg
import scipy.spatial


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
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
            paths = seed[self.rows[r]]
            for c in range(self.A.shape[1]):
                self.A[r, c] = float(self.cols[c] in paths)

        self.U, self.S, self.V_t = np.linalg.svd(self.A, full_matrices=False)

        self.U_k = self.U[:, :k]
        self.S_k = np.diagflat(self.S)[:k, :k]
        self.V_t_k = self.V_t[:k, :]

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
            yield h['_id'], {e['path'] for e in h['_source']['events']}

    def expand(self, paths):
        assert isinstance(paths, list) or isinstance(paths, set)
        for i in paths:
            assert isinstance(i, unicode)
        return np.array([float(p in paths) for p in self.cols])

    def fold_in_item(self, item, vector):
        item = np.dot(self.S_k, np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([item]).T))
        self.cols.append(item)

    def fold_in_user(self, user, vector):
        projection = np.dot(self.S_k, np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([projection]).T))
        self.rows.append(user)

    def predict(self, vector, metric='cosine', neighbors=100):
        query = np.array([np.dot(self.S_k, np.dot(self.V_t_k, vector))])
        dist = scipy.spatial.distance.cdist(query, self.U_k, metric)[0, :]
        key = lambda x: 0 if np.isnan(x[1]) else x[1]
        hood = sorted(zip(range(dist.shape[0]), dist), key=key)[-neighbors:]
        aggregate = np.array([self.A[i[0], :] * i[1] for i in hood]).sum(0)
        aggregate = np.nan_to_num(aggregate)
        prefs = aggregate ** -1
        prefs[np.isinf(prefs)] = 0.0
        return np.where(vector.astype(bool), vector, prefs)

    def recommend(self, vector, top_n=100, proximity=1.0, **kwargs):
        dist = self.predict(vector, **kwargs)
        tups = [(i, dist[i]) for i in range(len(dist)) if dist[i] > proximity]
        indicies = dict(sorted(tups, key=lambda x: x[1])[-top_n:]).keys()
        return np.take(np.array(self.cols), indicies)

    def process(self, tup):
        user, paths = tup.values
        vector = self.expand(paths)
        # self.fold_in_user(user, vector)

        recommendations = self.recommend(vector)[:10]
        paths = list(set(paths))[:10]

        emit([user, paths, recommendations])

if __name__ == '__main__':
    RecommendationBolt().run()
