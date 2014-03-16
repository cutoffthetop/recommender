# -*- coding: utf-8 -*-

"""
    zeit.recommend.outlet
    ~~~~~~~~~~~~~~~~~~~~~

    This module has no description.

    Copyright: (c) 2013 by Nicolas Drebenstedt.
    License: BSD, see LICENSE for more details.
"""

from storm import Bolt
from storm import emitBolt
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

_clients = {}
_server = None


class OutletWebSocket(WebSocket):
    def received_message(self, message):
        uid = str(message)
        if len(uid) and _server:
            log('[OutletWebSocket] Client connected: %s' % uid)
            index = _server.manager.websockets.values().index(self)
            _clients[uid] = _server.manager.websockets.keys()[index]
            emitBolt(['connect', uid], stream='control')


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
                fl=('title,teaser_text,keyword,release_date,author,department,'
                    'sub_department,href')
                )
            try:
                data = urlopen(self.url, urlencode(params)).read()
                yield json.loads(data)['response']['docs'][0]
            except Exception:
                yield dict(
                    title='zeit.de' + path,
                    teaser_text=('Lorem ipsum dolor sit amet, consetetur sadip'
                                 'scing elitr, sed diam nonumy eirmod tempor i'
                                 'nvidunt ut labore et dolore magna sed est.'),
                    href='http://www.zeit.de' + path
                    )

    def process(self, tup):
        user, requested, recommended = tup.values

        message = dict(
            recommended=list(self.resolve_paths(recommended)),
            requested=list(self.resolve_paths(requested))
            )

        if user in _clients:
            ws = _server.manager.websockets[_clients[user]]
            ws.send(json.dumps(message))


if __name__ == '__main__':
    _server = make_server(
        '0.0.0.0',
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
