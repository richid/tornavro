import os

import avro.protocol
import tornado.web
import tornado.ioloop
from tornado.options import define, options

import tornavro.server
import tornavro.responder


define('port', default=8888, help='Listen on this port')


class HelloResponder(tornavro.responder.Responder):
    def hello(self, name):
        return 'Hello, %s' % name


tornado.options.parse_command_line()

proto = open(os.path.join(os.path.dirname(__file__), 'hello.avpr')).read()
proto = avro.protocol.parse(proto)
responder = HelloResponder(proto)

server = tornavro.server.AvroServer(responder)
server.listen(options.port)
tornado.ioloop.IOLoop.instance().start()
