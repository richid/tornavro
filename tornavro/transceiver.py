"""Simple, blocking socket-based transceiver."""

import struct
import socket

import avro.ipc

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class SocketTransceiver(object):
    """Blocking, Socket-based transceiver."""

    def __init__(self, host, port):
        """Create and connect to the remote service."""

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((host, port))

    remote_name = property(lambda self: self.sock.getsockname())

    def transceive(self, request):
        """Write the request and read the response back."""

        self.write_framed_message(request)
        return self.read_framed_message()

    def write_framed_message(self, message):
        """Frame the message and send it over the wire."""

        req_buffer = avro.ipc.FramedWriter(StringIO())
        req_buffer.write_framed_message(message)
        req = req_buffer.writer.getvalue()

        self.sock.sendall(req)

    def read_framed_message(self):
        """Read the response from the wire and return it."""

        message = StringIO()
        buffer_length = ""

        while buffer_length != 0:
            buffer_header = self.sock.recv(4)
            buffer_length = struct.unpack('!I', buffer_header)[0]
            message.write(buffer_header)

            if buffer_length == "":
                raise avro.ipc.ConnectionClosedException("Reader read 0 bytes.")

            message.write(self.sock.recv(buffer_length))

        message.reset()
        reader = avro.ipc.FramedReader(message)
        framed_message = reader.read_framed_message()

        return framed_message

    def close(self):
        """Close this socket for both reading and writing."""

        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
