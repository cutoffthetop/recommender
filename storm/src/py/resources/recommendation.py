# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from storm import Bolt, log
import numpy as np
import random
import scipy.spatial
import time
import timeit


def stream(size=100, from_=10000, threshold=1):
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    pool = es.search(from_=from_, size=size)['hits']['hits']
    for p in range(len(pool)):
        if 'events' not in pool[p]['_source']:
            continue
        events = list(set([i['path'] for i in pool[p]['_source']['events']]))
        if len(events) > threshold:
            yield pool[p]['_id'], events


def expand(source, values):
    default_column = [0] * len(values)
    value_range = range(len(values))
    for row in source.keys():
        column = list(default_column)
        for i in value_range:
            if values[i] in source[row]:
                column[i] = 1
            #column.append(1 if cell in source.get(row) else 0)
        yield column

source = dict(stream(size=2500))
values = list(set([cell for sublist in source.values() for cell in sublist]))

A_base = np.array(list(expand(source, values)))
A_train = A_base[-100:, :]
A = A_base[:-100, :]
U, S, V_t = np.linalg.svd(A, full_matrices=False)
k = 10

U_k = U[:, :k]
S_k = np.diag(S)[:k, :k]
V_t_k = V_t[:k, :]


def recommend(size=100, user=0):
    if user > A.shape[1]:
        return None

    query = np.dot(np.linalg.inv(S_k), np.dot(U_k.T, A[:, user]))

    distances = dict()
    for row in range(0, V_t_k.shape[1]):
        dist = scipy.spatial.distance.cosine(query, V_t_k[:, row])
        if dist >= 0:
            distances[row] = dist

    if not len(distances):
        return None

    threshold = sorted(distances.values())[-min(size, len(distances))]
    rec = dict([(values[k], v) for k, v in distances.items() if v > threshold])

    return dict(
        observations=source.items()[user][1],
        recommendations=rec,
        user=user
        )


def bench():
    start = time.clock()
    recommend(user=random.randint(10, 200))
    print '%sms' % int((time.clock() - start) * 1000)


def folding_in(vector=None):
    global V_t_k
    vector = np.append(vector, (V_t_k.shape[1] - vector.shape[0]) * [0])
    item = np.dot(np.linalg.inv(S_k), np.dot(V_t_k, vector))
    V_t_k = np.hstack((V_t_k, np.array([item]).T))


ms = int(timeit.timeit('__import__("__main__").bench()', number=10) * 1000)
print 'average:', ms / 10, 'ms'

[folding_in(vector=A_train[i, :]) for i in range(A_train.shape[0])]


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
        log('Bolt id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def process(self, tup):
        log('[Process] Tuple received: %s' % tup)

#RecommendationBolt().run()
