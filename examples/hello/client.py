import os

import avro.ipc
import avro.protocol
import tornavro.transceiver

proto = open(os.path.join(os.path.dirname(__file__), 'hello.avpr')).read()
proto = avro.protocol.parse(proto)

# Blocking client
client = tornavro.transceiver.SocketTransceiver('localhost', 8888)
requestor = avro.ipc.Requestor(proto, client)

print requestor.request(u'hello', dict(name=u'rich'))

client.close()
