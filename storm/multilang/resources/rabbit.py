# -*- coding: utf-8 -*-

from storm import Spout, emit, log
from random import choice
from time import sleep
from uuid import uuid4


class RabbitMQSpout(Spout):
    def initialize(self, conf, context):
        emit(['spout initializing'])
        self.pending = {}

    def ack(self, id):
        del self.pending[id]

    def fail(self, id):
        log('emitting ' + self.pending[id] + ' on fail')
        emit([self.pending[id]], id=id)

    def nextTuple(self):
        sleep(1)
        word = choice(['Hot Chip', 'Kavinsky' 'London Grammar', 'MS MR'])
        id = str(uuid4())
        self.pending[id] = word
        emit([word], id=id)

RabbitMQSpout().run()
