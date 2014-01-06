# -*- coding: utf-8 -*-

from elasticsearch import Elasticsearch
from scikits.crab.metrics import loglikehood_coefficient
from scikits.crab.models import MatrixBooleanPrefDataModel
from scikits.crab.recommenders.knn import UserBasedRecommender
from scikits.crab.similarities import UserSimilarity
from scikits.crab.models.base import BaseDataModel
from storm import Bolt, log
from numpy import ndarray

# Transform elasticsearch output to work with crab
es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
pool = es.search(from_=500, size=40000)['hits']['hits']
indict = dict()

for i in range(len(pool)):
    if 'events' in pool[i]['_source']:
        indict[pool[i]['_id']] = list([j['path'] for j in pool[i]['_source']['events']])

# Initialize crab objects
model = MatrixBooleanPrefDataModel(indict)
similarity = UserSimilarity(model, loglikehood_coefficient)
recommender = UserBasedRecommender(model, similarity, with_preference=False)

# How well is the user-base compressable?
compressed = set([','.join(set(model.preferences_from_user(user))) for user in model.user_ids()])
ratio = len(model.user_ids())/float(len(compressed))
saving = 1 - (float(len(compressed))/len(model.user_ids()))
print ratio

# Randomly select a user and calculate a recommendation
user_ids = model.user_ids()
user = user_ids[__import__('random').randint(0, user_ids.size - 1)]
__import__('pprint').pprint(indict[user])
__import__('pprint').pprint(recommender.recommend(user, how_many=5))


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


class RecommenderBolt(Bolt):
    def initialize(self, conf, context):
        log('Bolt id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])

    def process(self, tup):
        log('[Process] Tuple received: %s' % tup)

#RecommenderBolt().run()
