# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from storm import Bolt, log
import numpy as np
import scipy.spatial


class RecommendationBolt(Bolt):
    def initialize(self, conf, context, k=10, size=2500):
        log('Bolt id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

        self.source = dict(self._stream(size=size))
        self.values = list(set([i for j in self.source.values() for i in j]))

        self.A = np.array(list(self._expand(self.source, self.values)))
        U, S, V_t = np.linalg.svd(self.A, full_matrices=False)

        self.U_k = U[:, :k]
        self.S_k = np.diag(S)[:k, :k]
        self.V_t_k = V_t[:k, :]

    def _expand(self):
        default_column = [0] * len(self.values)
        value_range = range(len(self.values))
        for row in self.source.keys():
            column = list(default_column)
            for i in value_range:
                if self.values[i] in self.source[row]:
                    column[i] = 1
            yield column

    def _stream(self, size=100, from_=10000, threshold=1):
        pool = self.es.search(from_=from_, size=size)['hits']['hits']
        for p in range(len(pool)):
            if 'events' not in pool[p]['_source']:
                continue
            events = set([i['path'] for i in pool[p]['_source']['events']])
            if len(events) > threshold:
                yield pool[p]['_id'], list(events)

    def _fold_in(self, vector=None):
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )
        item = np.dot(np.linalg.inv(self.S_k), np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([item]).T))

    def _recommend(self, size=10, vector=0):
        if vector > self.A.shape[1]:
            return None

        query = np.dot(np.linalg.inv(self.S_k),
                       np.dot(self.U_k.T, self.A[:, vector]))

        distances = dict()
        for row in range(0, self.V_t_k.shape[1]):
            dist = scipy.spatial.distance.cosine(query, self.V_t_k[:, row])
            if dist >= 0:
                distances[row] = dist

        if not len(distances):
            return None

        threshold = sorted(distances.values())[-min(size, len(distances))]
        rec = dict([(self.values[k], v) for k, v in distances.items()
                    if v > threshold])

        return dict(
            observations=self.source.items()[vector][1],
            recommendations=rec,
            vector=vector
            )

    def process(self, tup):
        if tup[0] == 0:
            log('[Process] Folding in: %s' % tup)
            # TODO: Expand tuple!
            print self._fold_in(vector=tup[1:])
        elif tup[0] == 1:
            log('[Process] Recommending: %s' % tup)
            # TODO: Emit returned values.
            print self._recommend(vector=tup[1:])

RecommendationBolt().run()
