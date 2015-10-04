"""
Vanilla Python Consul(.io) Client
"""

from __future__ import absolute_import

from functools import partial

from consul import base


def __plugin__(hub):
    class Consul(base.Consul):
        def connect(self, host, port, scheme, verify):
            return HTTPClient(hub, host, port, scheme, verify)
    return Consul


class HTTPClient(object):
    def __init__(
            self,
            hub,
            host='127.0.0.1',
            port=8500,
            scheme='http',
            verify=True):
        self.hub = hub
        self.host = host
        self.port = port
        self.scheme = scheme
        self.base_uri = '%s://%s:%s' % (self.scheme, self.host, self.port)

    def _response(self, conn, callback, response):
        response = base.Response(
            response.status.code, response.headers, response.consume())
        response = callback(response)
        conn.close()
        return response

    def _map(self, conn, callback):
        return conn.map(partial(self._response, conn, callback))

    def get(self, callback, path, params=None):
        conn = self.hub.http.connect(self.base_uri).get(path, params=params)
        return self._map(conn, callback)

    def put(self, callback, path, params=None, data=''):
        conn = self.hub.http.connect(
            self.base_uri).put(path, params=params, data=data)
        return self._map(conn, callback)

    def delete(self, callback, path, params=None):
        conn = self.hub.http.connect(
            self.base_uri).delete(path, params=params)
        return self._map(conn, callback)
