# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from storm import Bolt, log
import numpy as np


def stream(amount=100, threshold=1):
    es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
    pool = es.search(from_=1000, size=amount)['hits']['hits']
    for p in range(len(pool)):
        if 'events' not in pool[p]['_source']:
            continue
        events = list(set([i['path'] for i in pool[p]['_source']['events']]))
        if len(events) > threshold:
            yield pool[p]['_id'], events


def expand(src):
    values = set([cell for sublist in src.values() for cell in sublist])
    for row in src.keys():
        column = list()
        for cell in values:
            column.append(1 if cell in src.get(row) else 0)
        yield column

model = list(expand(dict(stream(amount=3000))))
training = np.array(model[:2000])

full_user_space, full_singular_vector, full_item_space = np.linalg.svd(
    training, full_matrices=False)

full_singular_matrix = np.diag(full_singular_vector)

r = full_singular_matrix.shape[0]
k = 100

# TODO: Implement this with views?
reduced_user_space = full_user_space[:-(r-k), :]
reduced_singular_matrix = np.diag(full_singular_vector[:k])
reduced_item_space = full_item_space[:, :-(r-k)]

#testing = np.array(model[-1])
#projection = user * user.transpose() * testing
#import pdb; pdb.set_trace()


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
        log('Bolt id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def process(self, tup):
        log('[Process] Tuple received: %s' % tup)

#RecommendationBolt().run()
