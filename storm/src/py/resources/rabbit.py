# -*- coding: utf-8 -*-

from storm import Spout, emit, log
from uuid import uuid4

import pika


class RabbitMQSpout(Spout):
    def initialize(self, conf, context):
        emit(['spout initializing'])
        self.pending = {}

        host = 'ec2-54-220-14-229.eu-west-1.compute.amazonaws.com'
        parameters = pika.ConnectionParameters(host=host, port=5672)
        connection = pika.BlockingConnection(parameters=parameters)
        self.channel = connection.channel()
        self.channel.exchange_declare(exchange='zr_spout', type='direct')
        self.queue = self.channel.queue_declare(exclusive=True).method.queue
        self.channel.queue_bind(exchange='zr_spout', queue=self.queue,
                                routing_key='logstash')

    def ack(self, id):
        del self.pending[id]

    def fail(self, id):
        log('emitting ' + self.pending[id] + ' on fail')
        emit([self.pending[id]], id=id)

    def nextTuple(self):
        message = self.channel.basic_get(queue=self.queue, no_ack=True)[2]
        id = str(uuid4())
        self.pending[id] = message
        emit([message], id=id)

RabbitMQSpout().run()