# -*- coding: utf-8 -*-

from storm import Spout, emit, log
from uuid import uuid4
from time import mktime, strptime
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
        self.throughput = conf.get('zeit.recommend.rabbitmq.throughput', 100)

        parameters = pika.ConnectionParameters(host=host, port=port)
        connection = pika.BlockingConnection(parameters=parameters)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange=exchange, type=type_)
        self.queue = self.channel.queue_declare(exclusive=True).method.queue
        self.channel.queue_bind(exchange=exchange, queue=self.queue,
                                routing_key=key)

    def ack(self, msg_id):
        # TODO: Properly implement amqp acking.
        log('[RabbitMQSpout] Acknowledging message id-%s.' % msg_id)

    def fail(self, msg_id):
        log('[RabbitMQSpout] Message id-%s is failing.' % msg_id)

    def nextTuple(self):
        raw = self.channel.basic_get(queue=self.queue, no_ack=True)[2]
        # TODO: Investigate (None, None, None) issue!
        if raw and random.randint(0, 100000 / self.throughput) < 1000:
            parsed = json.loads(raw)
            user = urllib.unquote(parsed['user']).strip('";').replace('|', '')
            ts = mktime(strptime(parsed['timestamp'], '%Y-%m-%d %H:%M:%S %Z'))
            emit([int(ts * 1000), parsed['path'], user], id=str(uuid4()))

RabbitMQSpout().run()
