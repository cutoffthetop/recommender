# -*- coding: utf-8 -*-

from storm import Spout, emit, log
from uuid import uuid4
from time import mktime, strptime
import json
import urllib


class ZonAPISpout(Spout):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 8983)
        self.url = 'http://%s:%s/solr/select' % (host, port)

    def ack(self, msg_id):
        # TODO: Properly implement AMQP acking.
        log('[ZonAPISpout] Acknowledging message id-%s.' % msg_id)

    def fail(self, msg_id):
        log('[ZonAPISpout] Message id-%s is failing.' % msg_id)

    def nextTuple(self):
        raw = self.channel.basic_get(queue=self.queue, no_ack=True)[2]
        # TODO: Investigate (None, None, None) issue!
        if raw:
            parsed = json.loads(raw)
            user = urllib.unquote(parsed['user']).strip('";').replace('|', '')
            ts = mktime(strptime(parsed['timestamp'], '%Y-%m-%d %H:%M:%S %Z'))
            emit([int(ts * 1000), parsed['path'], user], id=str(uuid4()))

ZonAPISpout().run()
