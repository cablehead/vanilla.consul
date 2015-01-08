import time

import pytest
import consul

import vanilla


def test_plugin():
    h = vanilla.Hub()
    h.consul()


class TestConsul(object):
    def test_kv(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)
        index, data = c.kv.get('foo').recv()
        assert data is None
        assert c.kv.put('foo', 'bar').recv() is True
        index, data = c.kv.get('foo').recv()
        assert data['Value'] == 'bar'

    def test_kv_delete(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)
        c.kv.put('foo1', '1').recv()
        c.kv.put('foo2', '2').recv()
        c.kv.put('foo3', '3').recv()
        index, data = c.kv.get('foo', recurse=True).recv()
        assert [x['Key'] for x in data] == ['foo1', 'foo2', 'foo3']

        assert c.kv.delete('foo2').recv() is True
        index, data = c.kv.get('foo', recurse=True).recv()
        assert [x['Key'] for x in data] == ['foo1', 'foo3']
        assert c.kv.delete('foo', recurse=True).recv() is True
        index, data = c.kv.get('foo', recurse=True).recv()
        assert data is None

    def test_kv_subscribe(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)

        def put():
            c.kv.put('foo', 'bar').recv()
        h.spawn_later(100, put)

        index, data = c.kv.get('foo').recv()
        assert data is None
        index, data = c.kv.get('foo', index=index).recv()
        assert data['Value'] == 'bar'

    def test_agent_self(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)
        assert set(c.agent.self().recv().keys()) == set(['Member', 'Config'])

    def test_agent_services(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)
        assert c.agent.services().recv().keys() == ['consul']
        assert c.agent.service.register('foo').recv() is True
        assert set(c.agent.services().recv().keys()) == set(['consul', 'foo'])
        assert c.agent.service.deregister('foo').recv() is True
        assert c.agent.services().recv().keys() == ['consul']

    def test_health_service(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)

        # check there are no nodes for the service 'foo'
        index, nodes = c.health.service('foo').recv()
        assert nodes == []

        # register two nodes, one with a long ttl, the other shorter
        c.agent.service.register('foo', service_id='foo:1', ttl='10s').recv()
        c.agent.service.register('foo', service_id='foo:2', ttl='100ms').recv()

        time.sleep(10/1000.0)

        # check the nodes show for the /health/service endpoint
        index, nodes = c.health.service('foo').recv()
        assert [node['Service']['ID'] for node in nodes] == ['foo:1', 'foo:2']

        # but that they aren't passing their health check
        index, nodes = c.health.service('foo', passing=True).recv()
        assert nodes == []

        # ping the two node's health check
        c.agent.check.ttl_pass('service:foo:1').recv()
        c.agent.check.ttl_pass('service:foo:2').recv()

        time.sleep(10/1000.0)

        # both nodes are now available
        index, nodes = c.health.service('foo', passing=True).recv()
        assert [node['Service']['ID'] for node in nodes] == ['foo:1', 'foo:2']

        # wait until the short ttl node fails
        time.sleep(120/1000.0)

        # only one node available
        index, nodes = c.health.service('foo', passing=True).recv()
        assert [node['Service']['ID'] for node in nodes] == ['foo:1']

        # ping the failed node's health check
        c.agent.check.ttl_pass('service:foo:2').recv()

        time.sleep(10/1000.0)

        # check both nodes are available
        index, nodes = c.health.service('foo', passing=True).recv()
        assert [node['Service']['ID'] for node in nodes] == ['foo:1', 'foo:2']

        # deregister the nodes
        c.agent.service.deregister('foo:1').recv()
        c.agent.service.deregister('foo:2').recv()

        time.sleep(10/1000.0)

        index, nodes = c.health.service('foo').recv()
        assert nodes == []

    def test_health_service_subscribe(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)

        class Config(object):
            pass

        config = Config()

        @h.spawn
        def monitor():
            c.agent.service.register(
                'foo', service_id='foo:1', ttl='20ms').recv()

            index = None
            while True:
                index, nodes = c.health.service(
                    'foo', index=index, passing=True).recv()
                config.nodes = [node['Service']['ID'] for node in nodes]

        # give the monitor a chance to register the service
        h.sleep(50)
        assert config.nodes == []

        # ping the service's health check
        c.agent.check.ttl_pass('service:foo:1').recv()
        h.sleep(10)
        assert config.nodes == ['foo:1']

        # the service should fail
        h.sleep(20)
        assert config.nodes == []

        c.agent.service.deregister('foo:1').recv()

    def test_acl(self, acl_consul):
        h = vanilla.Hub()
        c = h.consul(port=acl_consul.port, token=acl_consul.token)
        rules = """
            key "" {
                policy = "read"
            }
            key "private/" {
                policy = "deny"
            }
        """
        token = c.acl.create(rules=rules).recv()
        pytest.raises(consul.ACLPermissionDenied, c.acl.list(token=token).recv)
        c.acl.destroy(token).recv() is True


class TestIndex(object):
    """
    Investigate the interplay of Consul's blocking queries
    """
    def test_index(self, consul_port):
        h = vanilla.Hub()
        c = h.consul(port=consul_port)

        check = h.router().pipe(h.queue(10))

        # blocking calls:
        # kv.get

        # catalog.nodes
        # catalog.services
        # catalog.node
        # catalog.service

        # health.service

        # session.list
        # session.node
        # session.info

        def who(name):
            def f(x):
                return name, x
            return f

        c.agent.service.register('s1', ttl='60s').recv()
        c.agent.service.register('s2', ttl='60s').recv()

        index = None

        # wait for index to stabilize
        while True:
            try:
                index, _ = c.health.service('s2', index=index).recv(timeout=50)
            except vanilla.Timeout:
                break

        c.health.service('s1', passing=True, index=index
            ).map(who('s1')).pipe(check)
        c.health.service('s2', passing=True, index=index
            ).map(who('s2')).pipe(check)

        # check noone is ready to fire
        pytest.raises(vanilla.Timeout, check.recv, timeout=50)

        c.agent.check.ttl_pass('service:s1').recv()

        service, got = check.recv()
        assert service == 's1'

        # NOTE: s2's long poll fires
        service, got = check.recv()
        assert service == 's2'

        index, _ = got

        c.health.service('s1', passing=True, index=index
            ).map(who('s1')).pipe(check)
        c.health.service('s2', passing=True, index=index
            ).map(who('s2')).pipe(check)

        # check noone is ready to fire
        pytest.raises(vanilla.Timeout, check.recv, timeout=50)

        c.kv.put('foo/1', '1').recv()
        h.sleep(50)

        # interesting, putting to the KV doesn't bump the health.service index
        index2, _ = c.health.service('s1', passing=True).recv()
        assert index == index2
        index2, _ = c.kv.get('foo/1').recv()
        assert index != index2

        # service long polls weren't triggered by the KV put
        pytest.raises(vanilla.Timeout, check.recv, timeout=50)
