import vanilla

import json
import uuid

# from pprint import pprint as p

import logging
logging.basicConfig()


class TestConsul(object):
    def test_kv(self):
        h = vanilla.Hub()
        c = h.consul()

        key = uuid.uuid4().hex
        index, data = c.kv.get(key).recv()
        assert data is None

        assert c.kv.put(key, 'bar').recv()
        index, data = c.kv.get(key).recv()
        assert data['Value'] == 'bar'
        index, data = c.kv.get(key, recurse=True).recv()
        assert [x['Value'] for x in data] == ['bar']

        assert c.kv.put(key+'1', 'bar1').recv()
        assert c.kv.put(key+'2', 'bar2').recv()
        index, data = c.kv.get(key, recurse=True).recv()
        assert sorted([x['Value'] for x in data]) == ['bar', 'bar1', 'bar2']

        assert c.kv.delete(key).recv()
        index, data = c.kv.get(key).recv()
        assert data is None
        index, data = c.kv.get(key, recurse=True).recv()
        assert sorted([x['Value'] for x in data]) == ['bar1', 'bar2']

        assert c.kv.delete(key, recurse=True).recv()
        index, data = c.kv.get(key, recurse=True).recv()
        assert data is None

    def test_kv_subscribe(self):
        h = vanilla.Hub()

        key = uuid.uuid4().hex

        @h.spawn
        def _():
            c = h.consul()
            for i in xrange(3):
                h.sleep(10)
                c.kv.put(key, i).recv()

        c = h.consul()
        watch = c.kv.subscribe(key)
        assert watch.recv() is None
        assert watch.recv()['Value'] == 0
        assert watch.recv()['Value'] == 1
        assert watch.recv()['Value'] == 2

    def test_connect(self):
        print
        print

        h = vanilla.Hub()

        """
        response = c.agent.service.deregister('zhub').recv()
        print response.status
        print response.headers
        print repr(response.consume())
        print
        """

        c = h.consul()
        response = c.agent.service.register(
            'zhub', service_id='n2', ttl='10s').recv()
        print response.status
        print response.headers
        print repr(response.consume())
        print

        """
        response = c.agent.services().recv()
        print response.status
        print response.headers
        p(json.loads(response.consume()))
        print
        """

        @h.spawn
        def _():
            c = h.consul()
            index = None
            while True:
                response = c.health.checks('zhub', index=index).recv()
                index = response.headers['X-Consul-Index']
                data = json.loads(response.consume())
                print data

        @h.spawn
        def _():
            c = h.consul()
            index = None
            while True:
                response = c.health.service(
                    'zhub', index=index, passing=True).recv()
                index = response.headers['X-Consul-Index']
                data = json.loads(response.consume())
                print data

        @h.spawn
        def _():
            c = h.consul()
            index = None
            while True:
                response = c.catalog.service('zhub', index=index).recv()
                index = response.headers['X-Consul-Index']
                data = json.loads(response.consume())
                print data

        h.sleep(1000)

        c = h.consul()
        c.agent.check.ttl_pass('service:n2').recv()

        h.sleep(15000)
