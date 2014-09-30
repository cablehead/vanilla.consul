import time

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
        assert c.agent.services().recv() == {}
        assert c.agent.service.register('foo').recv() is True
        assert c.agent.services().recv() == {
            'foo': {'Port': 0, 'ID': 'foo', 'Service': 'foo', 'Tags': None}}
        assert c.agent.service.deregister('foo').recv() is True
        assert c.agent.services().recv() == {}

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
        c.health.check.ttl_pass('service:foo:1').recv()
        c.health.check.ttl_pass('service:foo:2').recv()

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
        c.health.check.ttl_pass('service:foo:2').recv()

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
        c.health.check.ttl_pass('service:foo:1').recv()
        h.sleep(10)
        assert config.nodes == ['foo:1']

        # the service should fail
        h.sleep(20)
        assert config.nodes == []

        c.agent.service.deregister('foo:1').recv()
