# -*- coding: utf-8 -*-

"""
    zeit.recommend.morelikethis
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from storm import Bolt
from storm import log
from storm import emit
import urllib2
import time
import json


class MorelikethisBolt(Bolt):

    connections = {}

    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 8983)
        self.url = 'http://%s:%s/solr/' % (host, port)

    def recommend(self, paths):
        q = 'q=' + '%20'.join(['*' + p for p in paths])
        raw = urllib2.urlopen(self.url + 'select?fl=body&wt=json&df=href&' + q)
        data = raw.read()
        response = json.loads(data)['response']
        bodies = [d['body'] for d in response['docs']]
        body = ' '.join(bodies).encode('ascii', 'ignore')
        header = {'Content-Type': 'text/plain; charset=utf-8'}
        req = urllib2.Request(self.url + 'mlt?fl=href', body, header)
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
