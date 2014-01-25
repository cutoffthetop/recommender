# -*- coding: utf-8 -*-

"""
    zeit.recommend.zonapi
    ~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from datetime import datetime
from storm import Spout, emit, log
from time import sleep
from urllib import urlopen, urlencode
from uuid import uuid4
import json


class ZonAPISpout(Spout):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 8983)
        self.url = 'http://%s:%s/solr/select' % (host, port)
        self.time_range = [datetime.now(), ] * 2

    def ack(self, cnt_id):
        log('[ZonAPISpout] Acknowledging content id-%s.' % cnt_id)

    def fail(self, cnt_id):
        log('[ZonAPISpout] Content id-%s is failing.' % cnt_id)

    def get_docs(self, from_, to, sort):
        # TODO: This is not parallelizable.
        date_range = (from_.isoformat()[:-3], to.isoformat()[:-3])
        params = dict(
            wt='json',
            q='release_date:[%s:00Z TO %s:00Z]' % date_range,
            fl='uuid,href,release_date,title',
            sort='release_date %s' % sort,
            rows='1'
            )
        raw = urlopen(self.url, urlencode(params))
        data = raw.read()
        return json.loads(data)['response']['docs']

    def nextTuple(self):
        docs = self.get_docs(self.time_range[1], datetime.now(), 'asc')
        if docs:
            self.time_range[1] = datetime.now()
        else:
            docs = self.get_docs(datetime(2000, 1, 1), self.time_range[0],
                                 'desc')
            if docs:
                self.time_range[0] = datetime.strptime(docs[0]['release_date'],
                                                       '%Y-%m-%dT%H:%M:%SZ')
        if docs:
            # Emitting   (path)
            # Encoded as (path)
            emit([docs[0]['href'][18:]], id=str(uuid4()))
        sleep(1.0)

if __name__ == '__main__':
    ZonAPISpout().run()
