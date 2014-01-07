# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from scikits.crab.metrics import loglikehood_coefficient
from scikits.crab.models import MatrixBooleanPrefDataModel
#from scikits.crab.recommenders.knn import UserBasedRecommender
from scikits.crab.recommenders.svd.classes import MatrixFactorBasedRecommender
from scikits.crab.similarities import UserSimilarity
from scikits.crab.models.base import BaseDataModel
from storm import Bolt, log
from numpy import ndarray
from hashlib import md5


__import__('logging').getLogger('crab').setLevel('DEBUG')

# Transform elasticsearch output to work with crab
es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
pool = es.search(from_=2000, size=2000)['hits']['hits']
compressed, uncompressed = dict(), dict()

for i in 200:
    if 'events' not in pool[i]['_source']:
        continue
    events = list(set([j['path'] for j in pool[i]['_source']['events']]))
    uncompressed[pool[i]['_id']] = events
    if len(events) > 1:
        compressed[md5(''.join(sorted(events))).hexdigest()] = events

# How well is the user-base compressable?
print 'ratio', len(uncompressed)/float(len(compressed))
print 'saving', 1 - (float(len(compressed))/len(uncompressed))

source = compressed


# Initialize crab objects
model = MatrixBooleanPrefDataModel(source)
similarity = UserSimilarity(model, loglikehood_coefficient)
#recommender = UserBasedRecommender(model, similarity, with_preference=False)

# Randomly select a user and calculate a recommendation
user = source.keys()[__import__('random').randint(0, len(source) - 1)]

recommender = MatrixFactorBasedRecommender(model=model, n_features=2)




result = recommender.recommend(user, how_many=10)



















__import__('pprint').pprint(source[user])
__import__('pprint').pprint(result)



# Could a custom data model speed up transformation?
class ElasticsearchDataModel(BaseDataModel):
    def __init__(self):
        super(BaseDataModel, self).__init__()

    def user_ids(self):
        return ndarray()

    def preference_values_from_user(self, user_id):
        return [0]

    def preferences_from_user(self, user_id, order_by_id=True):
        return [(0, 1.0)]

    def has_preference_values(self):
        return False

    def maximum_preference_value(self):
        return 1.0


class RecommendationBolt(Bolt):
    def initialize(self, conf, context):
        log('Bolt id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def process(self, tup):
        log('[Process] Tuple received: %s' % tup)

#RecommendationBolt().run()
