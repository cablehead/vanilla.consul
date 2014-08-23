"""
Vanilla Consul(.io) Client
"""


import base64
import json


def __plugin__(hub):
    def _():
        return Consul(hub)
    return _


class Consul(object):
    def __init__(self, hub):
        self.hub = hub
        self.conn = hub.http.connect('http://localhost:8500')
        self.dumps, self.loads = json.dumps, json.loads

        self.agent = Consul.Agent(self.hub, self.conn)
        self.health = Consul.Health(self.hub, self.conn)
        self.kv = Consul.KV(self)

    class KV(object):
        def __init__(self, agent):
            self.agent = agent

        def get(self, key, recurse=False):
            assert not key.startswith('/')

            params = {}
            if recurse:
                params['recurse'] = '1'
            ch = self.agent.conn.get('/v1/kv/%s' % key, params=params)

            @ch.map
            def _(response):
                data = response.consume()
                if response.status.code == 404:
                    # TODO: pretty sure this should raise
                    return None
                data = json.loads(data)
                for item in data:
                    item['Value'] = self.agent.loads(
                        base64.b64decode(item['Value']))
                if not recurse:
                    data = data[0]
                return response.headers['X-Consul-Index'], data
            return _

        def put(self, key, value):
            assert not key.startswith('/')

            value = self.agent.dumps(value)
            ch = self.agent.conn.put('/v1/kv/%s' % key, data=value)

            @ch.map
            def _(response):
                return json.loads(response.consume())
            return _

        def delete(self, key, recurse=False):
            assert not key.startswith('/')

            params = {}
            if recurse:
                params['recurse'] = '1'
            ch = self.agent.conn.delete('/v1/kv/%s' % key, params=params)

            @ch.map
            def _(response):
                response.consume()
                return (response.status.code == 200)
            return _

    class Agent(object):
        def __init__(self, hub, conn):
            self.hub = hub
            self.conn = conn
            self.service = Consul.Agent.Service(self.hub, self.conn)
            self.check = Consul.Agent.Check(self.hub, self.conn)

        def services(self):
            return self.conn.get('/v1/agent/services')

        class Service(object):
            def __init__(self, hub, conn):
                self.hub = hub
                self.conn = conn

            def register(
                self, name, service_id=None, port=None,
                    tags=None, check=None, interval=None, ttl=None):

                payload = {
                    'id': service_id,
                    'name': name,
                    'port': port,
                    'tags': tags,
                    'check': {
                        'script': check,
                        'interval': interval,
                        'ttl': ttl, }}

                return self.conn.put(
                    '/v1/agent/service/register', data=json.dumps(payload))

            def deregister(self, service_id):
                return self.conn.get(
                    '/v1/agent/service/deregister/%s' % service_id)

        class Check(object):
            def __init__(self, hub, conn):
                self.hub = hub
                self.conn = conn

            def ttl_pass(self, check_id):
                return self.conn.get('/v1/agent/check/pass/%s' % check_id)

    class Health(object):
        def __init__(self, hub, conn):
            self.hub = hub
            self.conn = conn

        def service(self, service):
            return self.conn.get('/v1/health/service/%s' % service)

        def checks(self, service, index=None):
            params = {}
            if index:
                params['index'] = index
            return self.conn.get(
                '/v1/health/checks/%s' % service, params=params)
