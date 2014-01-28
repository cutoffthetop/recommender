# -*- coding: utf-8 -*-

"""
    zeit.recommend.rabbitmq
    ~~~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from storm import emit
from storm import log
from storm import Spout
from time import mktime
from time import strptime
from uuid import uuid4
import json
import pika
import random
import urllib


class RabbitMQSpout(Spout):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.rabbitmq.host', 'localhost')
        port = conf.get('zeit.recommend.rabbitmq.port', 5672)
        exchange = conf.get('zeit.recommend.rabbitmq.exchange', '')
        type_ = conf.get('zeit.recommend.rabbitmq.type', 'direct')
        key = conf.get('zeit.recommend.rabbitmq.key', '')
        self.throughput = conf.get('zeit.recommend.rabbitmq.throughput', 1.0)
        self.buffer = {}

        parameters = pika.ConnectionParameters(host=host, port=port)
        connection = pika.BlockingConnection(parameters=parameters)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=exchange, type=type_)
        self.queue = self.channel.queue_declare(exclusive=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=self.queue,
                                routing_key=key)

    def ack(self, msg_id):
        # TODO: Properly implement amqp acking.
        tup, retries = self.buffer[msg_id]
        del self.buffer[msg_id]

    def fail(self, msg_id):
        tup, retries = self.buffer[msg_id]
        if retries >= 5:
            del self.buffer[msg_id]
            log('[RabbitMQSpout] Message %s failed for good.' % msg_id)
        else:
            self.buffer[msg_id] = (tup, retries + 1)
            emit(tup, id=msg_id)

    def nextTuple(self):
        raw = self.channel.basic_get(queue=self.queue, no_ack=True)[2]
        # TODO: Investigate (None, None, None) issue!
        if raw and random.random() < self.throughput:
            parsed = json.loads(raw)
            user = urllib.unquote(parsed['user']).strip('";').replace('|', '')
            if not len(user):
                return
            ts = mktime(strptime(parsed['timestamp'], '%Y-%m-%d %H:%M:%S %Z'))
            msg_id = str(uuid4())
            tup = [int(ts * 1000), parsed['path'], user]
            self.buffer[msg_id] = (tup, 0)
            emit(tup, id=msg_id)

if __name__ == '__main__':
    RabbitMQSpout().run()
