# -*- coding: utf-8 -*-

"""
    zeit.recommend.zonapi
    ~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import datetime
from storm import emit
from storm import log
from storm import Spout
from time import sleep
from urllib import urlencode
from urllib import urlopen
import json


class ZonAPISpout(Spout):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 8983)
        self.url = 'http://%s:%s/solr/select' % (host, port)
        self.newest = datetime.now()
        self.start = 0
        self.buffer = {}

    def ack(self, cnt_id):
        tup, retries = self.buffer[cnt_id]
        del self.buffer[cnt_id]
        log('[ZonAPISpout] Acknowledging content id-%s.' % cnt_id)

    def fail(self, cnt_id):
        tup, retries = self.buffer[cnt_id]
        if retries >= 5:
            del self.buffer[cnt_id]
            log('[ZonAPISpout] Message %s failed for good.' % cnt_id)
        else:
            self.buffer[cnt_id] = (tup, retries + 1)
            emit(tup, id=cnt_id)

    def get_docs(self):
        date_range = '%s:00Z TO NOW' % self.newest.isoformat()[:-3]
        params = dict(
            wt='json',
            q='release_date:[%s]' % date_range,
            fl='uuid,href,release_date,title',
            sort='release_date asc',
            rows='1'
            )
        raw = urlopen(self.url, urlencode(params))
        docs = json.loads(raw.read())['response']['docs']
        if docs:
            args = docs[0]['release_date'], '%Y-%m-%dT%H:%M:%SZ'
            self.newest = datetime.strptime(*args)
        else:
            params['q'] = '*:*'
            params['start'] = self.start
            params['sort'] = 'release_date asc'
            self.start += 1
            raw = urlopen(self.url, urlencode(params))
            docs = json.loads(raw.read())['response']['docs']
        return docs

    def nextTuple(self):
        docs = self.get_docs()
        uuid = docs[0]['uuid']
        tup = [docs[0]['href'][18:]]
        self.buffer[uuid] = (tup, 0)
        emit(tup, id=uuid)
        sleep(1.0)

if __name__ == '__main__':
    ZonAPISpout().run()
