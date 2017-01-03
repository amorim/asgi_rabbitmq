import string
from collections import defaultdict
from functools import partial, wraps
from random import Random

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.spec import BasicProperties

try:
    from threading import Thread, get_ident
except ImportError:
    from threading import Thread, _get_ident as get_ident

try:
    import queue
except ImportError:
    import Queue as queue


class AMQP(object):

    dead_letters = 'dead-letters'

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity,
                 method_calls):

        self.parameters = URLParameters(url)
        self.connection = SelectConnection(
            parameters=self.parameters,
            on_open_callback=self.on_connection_open,
        )
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.capacity = capacity
        self.channel_capacity = channel_capacity
        self.method_calls = method_calls

    def run(self):

        self.connection.ioloop.start()

    def on_connection_open(self, connection):

        connection.channel(self.on_channel_open)

    def on_channel_open(self, amqp_channel):

        amqp_channel.add_on_close_callback(self.on_channel_close)
        self.declare_dead_letters(amqp_channel)
        self.check_method_call(amqp_channel)

    def on_channel_close(self, amqp_channel, code, msg):

        # FIXME: Check if error is recoverable.
        amqp_channel.connection.channel(self.on_channel_open)

    def check_method_call(self, amqp_channel):

        try:
            method = self.method_calls.get_nowait()
            method(amqp_channel=amqp_channel)
        except queue.Empty:
            pass
        amqp_channel.connection.add_timeout(
            0.01,
            partial(self.check_method_call, amqp_channel),
        )

    def retry_if_closed(method):

        @wraps(method)
        def method_wrapper(self, **kwargs):

            amqp_channel = kwargs.pop('amqp_channel')
            if amqp_channel.is_closed:
                self.method_calls.put(partial(method_wrapper, self, **kwargs))
            else:
                method(self, amqp_channel=amqp_channel, **kwargs)

        return method_wrapper

    def propagate_error(method):

        @wraps(method)
        def method_wrapper(self, **kwargs):

            try:
                method(self, **kwargs)
            except Exception as error:
                kwargs['result'].put(partial(throw, error))

        return method_wrapper

    @retry_if_closed
    def send(self, amqp_channel, channel, message, result):

        # FIXME: Avoid constant queue declaration.  Or at least try to
        # minimize its impact to system.

        # Use keyword arguments because of `method_wrapper` signature.
        publish_message = partial(
            self.publish_message,
            amqp_channel=amqp_channel,
            channel=channel,
            message=message,
            result=result,
        )
        self.declare_channel(amqp_channel, channel, publish_message)

    def declare_channel(self, amqp_channel, channel, callback):

        amqp_channel.queue_declare(
            # Wrap in lambda because of `method_wrapper` signature.
            lambda method_frame: callback(method_frame=method_frame),
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    @propagate_error
    def publish_message(self, amqp_channel, channel, message, result,
                        method_frame):

        if method_frame.method.message_count >= self.capacity:
            raise RabbitmqChannelLayer.ChannelFull
        body = self.serialize(message)
        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        amqp_channel.basic_publish(
            exchange='',
            routing_key=channel,
            body=body,
            properties=properties,
        )
        result.put(lambda: None)

    @retry_if_closed
    @propagate_error
    def receive(self, amqp_channel, channels, block, result):

        consumer_tags = {}

        def timeout_callback():
            for tag in consumer_tags:
                amqp_channel.basic_cancel(consumer_tag=tag)
            result.put(lambda: (None, None))

        # FIXME: Set as less as possible, should be configurable.
        # FIXME: Support `block is True` variant.
        timeout_id = amqp_channel.connection.add_timeout(0.1, timeout_callback)

        # FIXME: Don't define function each time.
        def callback(amqp_channel, method_frame, properties, body):
            amqp_channel.connection.remove_timeout(timeout_id)
            amqp_channel.basic_ack(method_frame.delivery_tag)
            for tag in consumer_tags:
                amqp_channel.basic_cancel(consumer_tag=tag)
            channel = consumer_tags[method_frame.consumer_tag]
            message = self.deserialize(body)
            result.put(lambda: (channel, message))

        for channel in channels:
            tag = amqp_channel.basic_consume(callback, queue=channel)
            consumer_tags[tag] = channel

    @retry_if_closed
    @propagate_error
    def channel_exists(self, amqp_channel, channel, result):

        def onerror(channel, code, msg):
            result.put(lambda: False)

        def callback(method_frame):
            amqp_channel.callbacks.remove(amqp_channel.channel_number,
                                          '_on_channel_close', onerror)
            result.put(lambda: True)

        amqp_channel.add_on_close_callback(onerror)
        amqp_channel.queue_declare(callback, queue=channel, passive=True)

    @retry_if_closed
    @propagate_error
    def group_add(self, amqp_channel, group, channel, result):

        # FIXME: Is it possible to do this things in parallel?
        self.expire_group_member(amqp_channel, group, channel)
        declare_group = partial(
            amqp_channel.exchange_declare,
            exchange=group,
            exchange_type='fanout',
        )
        declare_member = partial(
            amqp_channel.exchange_declare,
            exchange=channel,
            exchange_type='fanout',
        )
        declare_channel = partial(
            amqp_channel.queue_declare,
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )
        bind_group = partial(
            amqp_channel.exchange_bind,
            destination=channel,
            source=group,
        )
        bind_channel = partial(
            amqp_channel.queue_bind,
            queue=channel,
            exchange=channel,
        )
        declare_group(
            lambda method_frame: declare_member(
                lambda method_frame: declare_channel(
                    lambda method_frame: bind_group(
                        lambda method_frame: bind_channel(
                            lambda method_frame: result.put(lambda: None),
                        ),
                    ),
                ),
            ),
        )

    @retry_if_closed
    @propagate_error
    def group_discard(self, amqp_channel, group, channel, result):

        unbind_member = partial(
            amqp_channel.exchange_unbind,
            destination=channel,
            source=group,
        )
        unbind_member(lambda method_frame: result.put(lambda: None))

    @retry_if_closed
    def send_group(self, amqp_channel, group, message):

        # FIXME: What about expiration property here?
        body = self.serialize(message)
        amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
        )

    def expire_group_member(self, amqp_channel, group, channel):

        expire_marker = 'expire.bind.%s.%s' % (group, channel)
        ttl = self.group_expiry * 1000

        declare_marker = partial(
            amqp_channel.queue_declare,
            queue=expire_marker,
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-message-ttl': ttl,
                # FIXME: make this delay as little as possible.
                'x-expires': ttl + 500,
                'x-max-length': 1,
            })

        def push_marker(method_frame):
            body = self.serialize({
                'group': group,
                'channel': channel,
            })
            amqp_channel.basic_publish(
                exchange='',
                routing_key=expire_marker,
                body=body,
            )

        declare_marker(push_marker)

    def declare_dead_letters(self, amqp_channel):

        declare_exchange = partial(
            amqp_channel.exchange_declare,
            exchange=self.dead_letters,
            exchange_type='fanout',
        )
        declare_queue = partial(
            amqp_channel.queue_declare,
            queue=self.dead_letters,
        )
        do_bind = partial(
            amqp_channel.queue_bind,
            queue=self.dead_letters,
            exchange=self.dead_letters,
        )
        consume = partial(
            self.consume_from_dead_letters,
            amqp_channel=amqp_channel,
        )
        declare_exchange(
            lambda method_frame: declare_queue(
                lambda method_frame: do_bind(
                    lambda method_frame: consume(),
                ),
            ),
        )

    def consume_from_dead_letters(self, amqp_channel):

        amqp_channel.basic_consume(
            self.on_dead_letter,
            queue=self.dead_letters,
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # FIXME: Ignore max-length dead-letters.
        # FIXME: what the hell zero means here?
        queue = properties.headers['x-death'][0]['queue']
        if self.is_expire_marker(queue):
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            # Use keyword arguments because of `method_wrapper`
            # signature.
            self.group_discard(
                amqp_channel=amqp_channel,
                group=group,
                channel=channel,
                result=skip_result,
            )
        else:
            amqp_channel.exchange_delete(exchange=queue)

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        return value

    def deserialize(self, message):

        return msgpack.unpackb(message, encoding='utf8')

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')

    del retry_if_closed
    del propagate_error


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    def __init__(self, url, expiry, group_expiry, capacity, channel_capacity):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.calls = queue.Queue()
        self.results = defaultdict(partial(queue.Queue, maxsize=1))
        self.amqp = AMQP(url, expiry, group_expiry, capacity, channel_capacity,
                         self.calls)

    def run(self):

        self.amqp.run()


class RabbitmqChannelLayer(BaseChannelLayer):

    extensions = ['groups']

    Random = Random

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        self.thread = ConnectionThread(url, expiry, group_expiry, capacity,
                                       channel_capacity)
        self.thread.start()
        self.random = self.Random()

    # FIXME: Handle queue.Full exception in all method calls blow.

    def send(self, channel, message):

        self.thread.calls.put(
            partial(
                self.thread.amqp.send,
                channel=channel,
                message=message,
                result=self.result_queue,
            ))
        return self.result

    def receive(self, channels, block=False):

        self.thread.calls.put(
            partial(
                self.thread.amqp.receive,
                channels=channels,
                block=block,
                result=self.result_queue,
            ))
        return self.result

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')

        while True:
            chars = (self.random.choice(string.ascii_letters)
                     for _ in range(12))
            random_string = ''.join(chars)
            channel = pattern + random_string
            self.thread.calls.put(
                partial(
                    self.thread.amqp.channel_exists,
                    channel=channel,
                    result=self.result_queue,
                ))
            if not self.result:
                return channel

    def declare_channel(self, channel):

        # Put `result_queue` in the callback closure, since callback
        # will be executed by another thread.  We will resolve another
        # `result_queue` if we access it from `self` in the callback.
        result_queue = self.result_queue
        self.thread.calls.put(
            partial(
                self.thread.amqp.declare_channel,
                channel=channel,
                callback=lambda method_frame: result_queue.put(lambda: None),
            ))
        return self.result

    def group_add(self, group, channel):

        self.thread.calls.put(
            partial(
                self.thread.amqp.group_add,
                group=group,
                channel=channel,
                result=self.result_queue,
            ))
        return self.result

    def group_discard(self, group, channel):

        self.thread.calls.put(
            partial(
                self.thread.amqp.group_discard,
                group=group,
                channel=channel,
                result=self.result_queue,
            ))
        return self.result

    def send_group(self, group, message):

        self.thread.calls.put(
            partial(
                self.thread.amqp.send_group,
                group=group,
                message=message,
            ))

    @property
    def result_queue(self):

        return self.thread.results[get_ident()]

    @property
    def result(self):

        result_getter = self.result_queue.get()
        result = result_getter()
        return result


class NullQueue(object):
    """`queue.Queue` stub."""

    def get(self, block=True, timeout=None):
        pass

    def get_nowait(self):
        pass

    def put(self, item, block=True, timeout=None):
        pass

    def put_nowait(self, item):
        pass


skip_result = NullQueue()


def throw(error):

    raise error


# TODO: is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.
#
# FIXME: `retry_if_closed` and `propagate_error` not works with nested
# callbacks.


def worker_start_hook(sender, **kwargs):
    """Declare necessary queues we gonna listen."""

    layer_wrapper = sender.channel_layer
    layer = layer_wrapper.channel_layer
    if not isinstance(layer, RabbitmqChannelLayer):
        return
    channels = sender.apply_channel_filters(layer_wrapper.router.channels)
    for channel in channels:
        layer.declare_channel(channel)


# FIXME: This must be optional since we don't require channels package
# to be installed.
worker_ready.connect(worker_start_hook)
