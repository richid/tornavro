import avro.protocol
import tornado.web
import tornado.ioloop

import tornavro.server
import tornavro.responder


class HelloResponder(tornavro.responder.Responder):
    def hello(self, name):
        return 'Hello, %s' % name


proto = open(os.path.join(os.path.dirname(__file__), 'hello.avpr')).read()
proto = avro.protocol.parse(proto)
responder = HelloResponder(proto)

server = tornavro.server.AvroServer(responder)
server.listen(8888)
tornado.ioloop.IOLoop.instance().start()
