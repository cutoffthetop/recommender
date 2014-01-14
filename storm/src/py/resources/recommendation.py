# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from storm import Bolt, log, emit
import numpy as np
import scipy.spatial


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.elasticsearch.host', 'localhost')
        port = conf.get('zeit.recommend.elasticsearch.port', 9200)
        self.es = Elasticsearch(hosts=[{'host': host, 'port': port}])

        base = conf.get('zeit.recommend.svd.base', 1000)
        self.source = dict(self._stream(base))
        self.values = list(set([i for j in self.source.values() for i in j]))

        self.A = np.array(list(self._expand(self.source)))
        U, S, V_t = np.linalg.svd(self.A, full_matrices=False)

        k = conf.get('zeit.recommend.svd.rank', 100)
        self.U_k = U[:, :k]
        self.S_k = np.diag(S)[:k, :k]
        self.V_t_k = V_t[:k, :]

    def _expand(self, source):
        default_column = [0] * len(self.values)
        value_range = range(len(self.values))
        for row in source.keys():
            column = list(default_column)
            for i in value_range:
                if self.values[i] in source[row]:
                    column[i] = 1
            yield column

    def _stream(self, size, from_=10000, threshold=1):
        # TODO: Replace 'from_' param with a more sensible time range.
        # TODO: Is threshold=1 justified?
        pool = self.es.search(from_=from_, size=size)['hits']['hits']
        for p in range(len(pool)):
            if 'events' not in pool[p]['_source']:
                continue
            events = set([i['path'] for i in pool[p]['_source']['events']])
            if len(events) > threshold:
                yield pool[p]['_id'], list(events)

    def _fold_in(self, vector):
        # TODO: Fold into which axis?
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )
        item = np.dot(np.linalg.inv(self.S_k), np.dot(self.V_t_k, vector))
        self.V_t_k = np.hstack((self.V_t_k, np.array([item]).T))

    def _recommend(self, vector, size=10):
        vector = np.append(
            vector,
            (self.V_t_k.shape[1] - vector.shape[0]) * [0]
            )

        query = np.dot(np.linalg.inv(self.S_k), np.dot(self.V_t_k, vector))

        distances = list()
        for row in range(0, self.V_t_k.shape[1]):
            dist = scipy.spatial.distance.cosine(query, self.V_t_k[:, row])
            if dist >= 0:
                try:
                    distances.append((self.values[row], dist))
                except:
                    distances.append((row, dist))

        return sorted(distances, key=lambda tup: tup[1])[-size:]

    def process(self, tup):
        try:
            user, events = tup.values
        except TypeError, e:
            # TODO: What happens here?
            log('[RecommendationBolt] TypeError, process failed: %s' % e)
            return

        vector = np.array(self._expand({user: events}).next())
        self._fold_in(vector)
        recommendations = self._recommend(vector)
        # Emitting   (user, events,   recommendations  )
        # Encoded as (user, (event*), (recommendation*))
        emit([user, events, recommendations])

RecommendationBolt().run()
