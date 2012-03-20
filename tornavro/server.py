"""Simple Avro server based on Tornado's TCPServer."""

import multiprocessing.pool

import avro.ipc
import tornado.ioloop
import tornado.netutil
import tornado.stack_context

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class Server(tornado.netutil.TCPServer):
    """Single-threaded Avro server based on Tornado's TCPServer."""

    def __init__(self, responder, **kwargs):
        """Create the server, taking a responder instance as an argument.

        Optional kwargs (io_loop, ssl_options) from parent constructor are
        preserved.
        """

        self.responder = responder

        super(Server, self).__init__(**kwargs)

    def handle_stream(self, stream, address):
        """Read the Avro framed messaged by each buffer."""

        # Should we limit stream.max_buffer_size? Default is 100MB
        AvroConnection(stream, address, self.handle_request)

    def handle_request(self, request, callback):
        """Send the request off to the Responder instance to perform the
        actual work.
        """

        self.responder.respond(request, callback)


class ThreadPoolServer(Server):
    """Server that utilizes a pool of worker threads to call the Responder
    endpoints.
    """

    def __init__(self, responder, num_threads=15, **kwargs):
        """Create the server, taking a responder instance as an argument and
        num_threads as kwarg.

        Optional kwargs (io_loop, ssl_options) from parent constructor are
        preserved.
        """

        self.thread_pool = multiprocessing.pool.ThreadPool(num_threads)

        super(ThreadPoolServer, self).__init__(responder, **kwargs)

    def handle_request(self, request, callback):
        """Use the thread pool to handle the request."""

        func = self.responder.respond
        self.thread_pool.apply_async(func, args=(request,), callback=callback)


class AvroConnection(object):
    """Handles an Avro connection. Read each Avro buffer until we've got a
    complete message and then execute the callback (provided by the Responder
    class) on that message. Then write the response out on the wire.
    """

    def __init__(self, stream, address, request_handler):
        """Set us up the bomb."""

        self.stream = stream
        self.address = address
        self.request_handler = request_handler
        self.message = StringIO()
        self.read_new_buffer()

    def read_new_buffer(self):
        """Start reading a new Avro buffer by reading the first four bytes,
        which indicate how many bytes this buffer is.
        """

        if self.stream.closed():
            return

        self.stream.read_bytes(avro.ipc.BUFFER_HEADER_LENGTH,
            self._on_new_buffer)

    def write(self, chunk):
        """Writes a chunk of output to the stream."""

        if self.stream.closed():
            return

        self.stream.write(chunk, self._on_write)

    def _on_write(self):
        """Callback fired after previous write has been flushed. Ready to read
        additional request from the sream.
        """

        self.read_new_buffer()

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
        """Grab the response, frame it, and write it out."""

        writer = avro.ipc.FramedWriter(StringIO())
        writer.write_framed_message(response)

        self.write(writer.writer.getvalue())

    def _fetch_response(self):
        """Glue the buffers back into a single message and call the specified
        method on the responder.
        """

        self.message.reset()

        reader = avro.ipc.FramedReader(self.message)
        request = reader.read_framed_message()

        self.request_handler(request, self._on_response)
