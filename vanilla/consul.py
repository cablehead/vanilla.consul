"""
Vanilla Python Consul(.io) Client
"""

from __future__ import absolute_import


from consul import base


def __plugin__(hub):
    class Consul(base.Consul):
        def connect(self, host, port, scheme):
            return HTTPClient(hub, host, port, scheme)
    return Consul


class HTTPClient(object):
    def __init__(self, hub, host='127.0.0.1', port=8500, scheme='http'):
        self.hub = hub
        self.host = host
        self.port = port
        self.scheme = scheme
        self.base_uri = '%s://%s:%s' % (self.scheme, self.host, self.port)

    def response(self, response):
        return base.Response(
            response.status.code, response.headers, response.consume())

    def get(self, callback, path, params=None):
        r = self.hub.http.connect(self.base_uri).get(path, params=params)
        return r.map(self.response).map(callback)

    def put(self, callback, path, params=None, data=''):
        r = self.hub.http.connect(
            self.base_uri).put(path, params=params, data=data)
        return r.map(self.response).map(callback)

    def delete(self, callback, path, params=None):
        r = self.hub.http.connect(
            self.base_uri).delete(path, params=params)
        return r.map(self.response).map(callback)
