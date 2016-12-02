RedisSub
=====

This consumer uses the redis messagingsystem to consume messages which are published to a specific channel.
Internally a redis PSubscribe is used which means that you can also use wildcard channels.


Parameters
----------

**Enable**
  Enable switches the consumer on or off.
  By default this value is set to true.

**ID**
  ID allows this consumer to be found by other plugins by name.
  By default this is set to "" which does not register this consumer.

**Channel**
  Channel is the PSub pattern of the channels to subscribe to.
  E.g. my_channel_* or just my_channel.

**Address**
  Address stores the identifier to connect to.
  This can either be any ip address and port like "localhost:6379" or a file like "unix:///var/redis.socket".
  By default this is set to ":6379".


Example
-------

.. code-block:: yaml

  - "consumer.RedisSub":
      Enable: true
      ID: ""
      Stream: "data"
      Address: "127.0.0.1:6379"
      Channel: "my_channel*"
