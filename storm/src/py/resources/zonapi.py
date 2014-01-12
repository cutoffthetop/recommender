# -*- coding: utf-8 -*-

from storm import Spout, emit, log
from uuid import uuid4
from time import mktime, strptime
import json
import urllib


class ZonAPISpout(Spout):
    def initialize(self, conf, context):
        log('Spout id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        self.url = 'http://217.13.68.229:8983/solr/select'

    def ack(self, msg_id):
        # TODO: Properly implement amqp acking.
        log('Acknowledging message id-%s.' % msg_id)

    def fail(self, msg_id):
        log('Message id-%s is failing.' % msg_id)

    def nextTuple(self):
        raw = self.channel.basic_get(queue=self.queue, no_ack=True)[2]
        # TODO: Investigate (None, None, None) issue!
        if raw:
            parsed = json.loads(raw)
            user = urllib.unquote(parsed['user']).strip('";').replace('|', '')
            ts = mktime(strptime(parsed['timestamp'], '%Y-%m-%d %H:%M:%S %Z'))
            emit([int(ts * 1000), parsed['path'], user], id=str(uuid4()))

ZonAPISpout().run()
