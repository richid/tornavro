"""Base responder class, subclasses just need to define the endpoints they
expose.
"""

import avro.ipc
import avro.schema


class Responder(avro.ipc.Responder):
    """Base responder class, subclasses just need to define the endpoints they
    expose.

    For example:
        class HelloResponder(tornavro.responder.Responder):
            def hello(self, name):
                return 'Hello, %s' % name
    """

    def __init__(self, local_protocol):
        """Overriden solely to add the initialize() hook for subclasses."""

        super(Responder, self).__init__(local_protocol)

        self.initialize()

    def initialize(self):
        """Hook for subclasses to add any initialization logic."""
        pass

    def invoke(self, message, request):
        """Call the requested method in the subclassed responder."""

        if not hasattr(self, message.name):
            raise avro.schema.AvroException(
                'Method %s not defined in responder' % message.name
            )

        # TODO: It would be awesome to somehow split the RPC params into the
        # method signature.  Could just use **request, but that doesn't work
        # for records
        return getattr(self, message.name)(**request)
