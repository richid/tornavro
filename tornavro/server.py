"""Simple Avro server based on Tornado's TCPServer."""

import socket
import multiprocessing.pool

import avro.ipc
import tornado.ioloop
import tornado.netutil
import tornado.stack_context

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class AvroServer(tornado.netutil.TCPServer):
    """Simple Avro server based on Tornado's TCPServer."""

    def __init__(self, responder, **kwargs):
        """Create the server, taking a responder instance as an argument.

        Optional kwargs (io_loop, ssl_options) from parent constructor are
        preserved.
        """

        # XXX: cheat(!) and use a pool of worker threads
        self.pool = multiprocessing.pool.ThreadPool(10)
        self.responder = responder

        super(AvroServer, self).__init__(**kwargs)

    def handle_stream(self, stream, address):
        """Read the Avro framed messaged by each buffer."""

        # Should we limit stream.max_buffer_size? Default is 100MB
        AvroConnection(stream, address, self.responder.respond, self.pool)


class AvroConnection(object):
    """Handles an Avro connection. Read each Avro buffer until we've got a
    complete message and then execute the callback (provided by the Responder
    class) on that message. Then write the response out on the wire.
    """

    def __init__(self, stream, address, request_callback, worker_pool):
        """Set us up the bomb."""

        self.stream = stream
        self.address = address
        self.request_callback = request_callback
        self.worker_pool = worker_pool
        self.message = StringIO()
        self.read_new_buffer()

    def read_new_buffer(self):
        """Start reading a new Avro buffer by reading the first four bytes,
        which indicate how many bytes this buffer is.
        """

        if self.stream.closed():
            return

        callback = tornado.stack_context.wrap(self._on_new_buffer)
        self.stream.read_bytes(avro.ipc.BUFFER_HEADER_LENGTH, callback)

    def write(self, chunk):
        """Writes a chunk of output to the stream."""

        if self.stream.closed():
            return

        try:
            self.stream.write(chunk)
        except socket.error:
            pass

    def _on_new_buffer(self, buffer_header):
        """Callback fired after we've read the buffer length off the wire and
        are ready to read the payload.
        """

        buffer_length = avro.ipc.BIG_ENDIAN_INT_STRUCT.unpack(buffer_header)[0]

        self.message.write(buffer_header)

        if buffer_length == "":
            raise avro.ipc.ConnectionClosedException("Reader read 0 bytes.")
        elif buffer_length == 0:
            # A zero length buffer indicates the end of the incomine message
            self._fetch_response()
        else:
            self.stream.read_bytes(buffer_length, self._on_buffer)

    def _on_buffer(self, full_buffer):
        """Callback fired after we've finished reading an entire buffer."""

        self.message.write(full_buffer)
        self.read_new_buffer()

    def _on_response(self, response):
        #tornado.ioloop.IOLoop.instance().add_handler(self.stream.fileno(), self.
        #response = self.request_callback(request)

        writer = avro.ipc.FramedWriter(StringIO())
        writer.write_framed_message(response)

        # Should this notify _someone_ when the write has been flushed?
        self.write(writer.writer.getvalue())

    def _fetch_response(self):
        """Glue the buffers back into a single message and call the specified
        method on the responder.
        """

        self.message.reset()

        reader = avro.ipc.FramedReader(self.message)
        request = reader.read_framed_message()

        # Delegate invocation of the Responder to the thread pool
        self.worker_pool.apply_async(self.request_callback, args=(request,),
            callback=self._on_response)