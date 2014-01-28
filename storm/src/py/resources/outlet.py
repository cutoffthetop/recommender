# -*- coding: utf-8 -*-

"""
    zeit.recommend.outlet
    ~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from storm import Bolt
from storm import log
from threading import Thread
from urllib import urlencode
from urllib import urlopen
from ws4py.server.wsgirefserver import WebSocketWSGIRequestHandler
from ws4py.server.wsgirefserver import WSGIServer
from ws4py.server.wsgiutils import WebSocketWSGIApplication
from ws4py.websocket import WebSocket
from wsgiref.simple_server import make_server
import json

# TODO: Global variables shouldn't be the solution.
_clients = {}
_server = None

LOREM = (
    'Lorem ipsum dolor sit amet, consetetur sadipscing elitr, sed diam '
    'nonumy tempor invidunt ut labore et dolore magna aliquyam erat sed.'
    )

class OutletWebSocket(WebSocket):
    def received_message(self, message):
        uid = str(message)
        if len(uid) and _server:
            log('[OutletWebSocket] Client connected: %s' % str(uid))
            index = _server.manager.websockets.values().index(self)
            _clients[uid] = _server.manager.websockets.keys()[index]


class OutletBolt(Bolt):
    def initialize(self, conf, context):
        host = conf.get('zeit.recommend.zonapi.host', 'localhost')
        port = conf.get('zeit.recommend.zonapi.port', 9200)
        self.url = 'http://%s:%s/solr/select' % (host, port)

    def resolve_paths(self, paths):
        for path in paths:
            params = dict(
                wt='json',
                q='href:*%s' % path,
                fl='title,teaser_text'
                )
            data = urlopen(self.url, urlencode(params)).read()
            try:
                result = json.loads(data)['response']['docs'][0]
            except:
                continue

            yield dict(
                teaser=result.get('teaser_text', LOREM),
                title=result.get('title', LOREM[:21]),
                path=path
                )

    def process(self, tup):
        user, requested, recommended = tup.values

        message = dict(
            recommended=list(self.resolve_paths(recommended)),
            requested=list(self.resolve_paths(requested))
            )

        # TODO: Disable broadcasting and whitelist user IDs.
        # if user in _clients:
        #     ws = _server.manager.websockets[_clients[user]]
        for ws in _server.manager.websockets.values():
            ws.send(json.dumps(message))


if __name__ == '__main__':
    _server = make_server(
        'localhost',
        9000,
        server_class=WSGIServer,
        handler_class=WebSocketWSGIRequestHandler,
        app=WebSocketWSGIApplication(handler_cls=OutletWebSocket)
        )
    _server.initialize_websockets_manager()
    thread = Thread(target=_server.serve_forever)
    thread.daemon = True
    thread.start()
    OutletBolt().run()
