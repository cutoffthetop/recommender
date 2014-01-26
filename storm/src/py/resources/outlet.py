# -*- coding: utf-8 -*-

"""
    zeit.recommend.outlet
    ~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from storm import Bolt
from threading import Thread
from ws4py.server.wsgirefserver import WebSocketWSGIRequestHandler
from ws4py.server.wsgirefserver import WSGIServer
from ws4py.server.wsgiutils import WebSocketWSGIApplication
from ws4py.websocket import WebSocket
from wsgiref.simple_server import make_server

_clients = {}


class OutletWebSocket(WebSocket):
    def received_message(self, message):
        _clients.append(message)


class OutletBolt(Bolt):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.socket.host', 'localhost')
        port = conf.get('zeit.recommend.socket.port', 9000)
        self.server = make_server(
            host,
            port,
            server_class=WSGIServer,
            handler_class=WebSocketWSGIRequestHandler,
            app=WebSocketWSGIApplication(handler_cls=OutletWebSocket)
            )
        self.server.initialize_websockets_manager()
        thread = Thread(target=self.server.serve_forever)
        thread.start()

    def process(self, tup):
        for sock in self.server.manager.websockets.itervalues():
            pass

if __name__ == '__main__':
    OutletBolt().run()
