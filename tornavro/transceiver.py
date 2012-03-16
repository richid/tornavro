"""Simple, blocking, socket-based transceiver."""

import socket

import avro.ipc

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO


class SocketTransceiver(object):
    """Simple, blocking, socket-based transceiver."""

    def __init__(self, host, port, timeout=None):
        """Create and connect to the remote service."""

        self.sock = socket.create_connection((host, port), timeout)

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

        while True:
            buff = StringIO()
            buffer_header = self.sock.recv(avro.ipc.BUFFER_HEADER_LENGTH)
            buffer_length = \
                avro.ipc.BIG_ENDIAN_INT_STRUCT.unpack(buffer_header)[0]

            if buffer_length == "":
                raise avro.ipc.ConnectionClosedException(
                    "Socket read 0 bytes."
                )
            elif buffer_length == 0:
                # A 0-length buffer indicates the end of the message
                return message.getvalue()

            while buff.tell() < buffer_length:
                chunk = self.sock.recv(buffer_length - buff.tell())
                if chunk == "":
                    raise avro.ipc.ConnectionClosedException(
                        "Socket read 0 bytes."
                    )
                buff.write(chunk)

            message.write(buff.getvalue())

    def close(self):
        """Close this socket for both reading and writing."""

        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()
