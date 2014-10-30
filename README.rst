`Vanilla`_ client for `Consul.io`_
==================================

This is an adaptor/plugin for `Vanilla`_ based on the `python-consul`_ library.

Usage is the same as the `standard API`_ except that all API calls return a
Vanilla pipe, which can be *recv*'ed on to receive the Consul response.

Example
-------

.. code:: python

    h = vanilla.Hub()
    c = h.consul()

    class Config(object):
        pass

    config = Config()

    @h.spawn
    def monitor():
        # register our service
        c.agent.service.register(
            'foo', service_id='foo:1', ttl='10s').recv()

        @h.spawn
        def keepalive():
            while True:
                # ping our service's health check every 5s
                c.health.check.ttl_pass('service:foo:1').recv()
                h.sleep(5000)

        # maintain our internal configuration state with all available nodes
        # providing the foo service
        index = None
        while True:
            index, nodes = c.health.service(
                'foo', index=index, passing=True).recv()
            config.nodes = [node['Service']['ID'] for node in nodes]

    # make use of config.nodes

Installation
------------

::

        pip install vanilla.consul


.. _Consul.io: http://www.consul.io/
.. _Vanilla: https://github.com/cablehead/vanilla
.. _python-consul: http://python-consul.readthedocs.org
.. _standard API:
    http://python-consul.readthedocs.org/en/latest/#api-documentation
