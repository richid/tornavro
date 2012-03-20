"""Base responder class; subclasses need to define the endpoints they expose.
The initialize() method is provided as a way to setup any external resources
the service requires.
"""

import logging

import avro.ipc
import avro.schema


class Responder(avro.ipc.Responder):
    """Base responder class; subclasses need to define the endpoints they
    expose.

    For example:
        class HelloResponder(tornavro.responder.Responder):
            def hello(self, name):
                return "Hello, %s" % name
    """

    def __init__(self, local_protocol):
        """Overriden solely to add the initialize() hook for subclasses."""

        super(Responder, self).__init__(local_protocol)

        self.initialize()

    def initialize(self):
        """Hook for subclasses to add any initialization logic."""
        pass

    def handle_exception(self, endpoint):
        """Handle any uncaught exceptions raised from a Responder sublcass.

        Subclasses are encouraged to override this method for custom error
        handling.
        """

        endpoint = "%s.%s" % (self.__class__.__name__, endpoint)
        logging.error("Exception in %s", endpoint, exc_info=True)

    def respond(self, call_request, callback=None):
        """This method is overriden to add the callback kwarg."""

        response = super(Responder, self).respond(call_request)

        if callback:
            return callback(response)

        return response

    def invoke(self, message, request):
        """Call the requested method in the subclassed responder."""

        # TODO: It would be awesome to somehow split the RPC params into the
        # method signature.  Could just use **request, but that doesn't work
        # for records
        try:
            return getattr(self, message.name)(**request)
        except:
            self.handle_exception(message.name)
            return
