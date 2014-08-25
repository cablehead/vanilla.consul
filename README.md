
```python

    >>> h = vanilla.Hub()

    >>> @h.spawn
    >>> def writer():
    >>>     c = h.consul()
    >>>     for i in xrange(3):
    >>>         h.sleep(10)
    >>>         c.kv.put(key, i).recv()

    >>> c = h.consul()
    >>> watch = c.kv.subscribe(key)
    >>> watch.recv()['Value']
    None
    >>> watch.recv()['Value']
    0
    >>> watch.recv()['Value']
    1
```
