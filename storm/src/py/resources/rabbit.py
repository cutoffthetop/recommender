# -*- coding: utf-8 -*-

from storm import Spout, emit, log
from uuid import uuid4
from time import mktime, strptime
import json
import pika
import urllib


class RabbitMQSpout(Spout):
    def initialize(self, conf, context):
        log('Spout id-%s is initializing.' % id(self))
        # TODO: Make connection params configurable.
        parameters = pika.ConnectionParameters(host='217.13.68.236', port=5672)
        connection = pika.BlockingConnection(parameters=parameters)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='zr_spout', type='direct')
        self.queue = self.channel.queue_declare(exclusive=True).method.queue
        self.channel.queue_bind(exchange='zr_spout', queue=self.queue,
                                routing_key='logstash')

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
            user_str = urllib.unquote(parsed['user'])
            user_id = user_str.strip('"').strip(';').replace('|', '')
            ts = mktime(strptime(parsed['timestamp'], '%Y-%m-%d %H:%M:%S %Z'))
            message = [str(int(ts)), parsed['path'], user_id]
            emit(message, id=str(uuid4()))

RabbitMQSpout().run()
