# -*- coding: utf-8 -*-

"""
    zeit.recommend.morelikethis
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from elasticsearch import Elasticsearch
from storm import Bolt
from storm import log
from storm import emit
import urllib2
import time


class MorelikethisBolt(Bolt):

    connections = {}

    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 8983)
        self.url = 'http://%s:%s/solr/mlt?fl=href' % (host, port)

    def recommend(self, paths):
        aggregate = []
        for path in paths:
            # TODO: Retrieve article bodies here.
            aggregate.append('')

        body = ' '.join(aggregate)
        req = urllib2.Request(url, body, {'Content-Type':'text/plain'})
        raw = urllib2.urlopen(req)
        data = raw.read()
        response = json.loads(data)['response']
        return list(i['href'][18:] for i in response['docs'])

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
