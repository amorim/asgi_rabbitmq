import base64
import hashlib
from functools import partial

import msgpack
from asgiref.base_layer import BaseChannelLayer
from channels.signals import worker_ready
from pika import SelectConnection, URLParameters
from pika.channel import Channel
from pika.exceptions import ConnectionClosed
from pika.spec import Basic, BasicProperties

try:
    from concurrent.futures import Future
except ImportError:
    from futures import Future

try:
    from threading import Thread, Lock, Event, get_ident
except ImportError:
    from threading import Thread, Lock, Event, _get_ident as get_ident

SEND = 0
RECEIVE = 1
NEW_CHANNEL = 2
DECLARE_CHANNEL = 3
GROUP_ADD = 4
GROUP_DISCARD = 5
SEND_GROUP = 6
DECLARE_DEAD_LETTERS = 7
EXPIRE_GROUP_MEMBER = 8


class Protocol(object):

    dead_letters = 'dead-letters'

    def __init__(self, expiry, group_expiry, get_capacity, ident, process,
                 crypter):

        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.ident = ident
        self.process = process
        self.crypter = crypter
        self.methods = {
            SEND: self.send,
            RECEIVE: self.receive,
            NEW_CHANNEL: self.new_channel,
            DECLARE_CHANNEL: self.declare_channel,
            GROUP_ADD: self.group_add,
            GROUP_DISCARD: self.group_discard,
            SEND_GROUP: self.send_group,
            DECLARE_DEAD_LETTERS: self.declare_dead_letters,
            EXPIRE_GROUP_MEMBER: self.expire_group_member,
        }

    # Utilities.

    def register_channel(self, method, amqp_channel):

        self.amqp_channel = amqp_channel
        self.apply(*method)

    def apply(self, method_id, args, kwargs):

        self.methods[method_id](*args, **kwargs)

    def protocol_error(self, error):

        self.resolve.set_exception(error)

    def get_queue_name(self, channel):

        if '!' in channel:
            return 'amq.gen-' + channel.rsplit('!', 1)[-1]
        elif '?' in channel:
            return 'amq.gen-' + channel.rsplit('?', 1)[-1]
        else:
            return channel

    # Send.

    def send(self, channel, message):

        # FIXME: Avoid constant queue declaration.  Or at least try to
        # minimize its impact to system.
        queue = self.get_queue_name(channel)
        self.amqp_channel.queue_declare(
            partial(self.publish_message, channel, message),
            queue=queue,
            passive=True if '!' in channel or '?' in channel else False,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    def publish_message(self, channel, message, method_frame):

        if method_frame.method.message_count >= self.get_capacity(channel):
            self.resolve.set_exception(RabbitmqChannelLayer.ChannelFull())
            return
        queue = self.get_queue_name(channel)
        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=queue,
            body=body,
            properties=self.publish_properties,
        )
        self.resolve.set_result(None)

    @property
    def publish_properties(self):

        expiration = str(self.expiry * 1000)
        properties = BasicProperties(expiration=expiration)
        return properties

    # Declare channel.

    def declare_channel(self, channel):

        self.amqp_channel.queue_declare(
            self.channel_declared,
            queue=channel,
            arguments={'x-dead-letter-exchange': self.dead_letters},
        )

    def channel_declared(self, method_frame):

        self.resolve.set_result(None)

    # Receive.

    def receive(self, channels, block):

        if block:
            consumer_tags = {}
            for channel in channels:
                tag = self.amqp_channel.basic_consume(
                    partial(self.consume_message, consumer_tags),
                    queue=self.get_queue_name(channel),
                )
                consumer_tags[tag] = channel
        else:
            if not channels:
                # All channels gave 404 error.
                self.resolve.set_result((None, None))
                return
            channels = list(channels)  # Daphne sometimes pass dict.keys()
            channel = channels[0]
            no_message = partial(self.no_message, channels[1:])
            self.amqp_channel.add_callback(no_message, [Basic.GetEmpty])
            no_queue = partial(self.no_queue, channels[1:])
            self.amqp_channel.add_on_close_callback(no_queue)
            self.resolve.add_done_callback(
                lambda future: self.amqp_channel.callbacks.remove(
                    self.amqp_channel.channel_number,
                    '_on_channel_close',
                    no_queue,
                ),
            )
            self.amqp_channel.basic_get(
                partial(self.get_message, channel, no_message),
                queue=self.get_queue_name(channel),
            )

    # FIXME: If we tries to consume from list of channels.  One of
    # consumer queues doesn't exists.  Channel is closed with 404
    # error. We will never consume from existing queues and will
    # return empty response.
    #
    # FIXME: If we tries to consume from channel which queue doesn't
    # exist at this time.  We consume with blocking == True.  We must
    # start consuming at the moment when queue were declared.

    def consume_message(self, consumer_tags, amqp_channel, method_frame,
                        properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        for tag in consumer_tags:
            amqp_channel.basic_cancel(consumer_tag=tag, nowait=True)
        channel = consumer_tags[method_frame.consumer_tag]
        message = self.deserialize(body)
        self.resolve.set_result((channel, message))

    def get_message(self, channel, no_message, amqp_channel, method_frame,
                    properties, body):

        amqp_channel.callbacks.remove(
            amqp_channel.channel_number,
            Basic.GetEmpty,
            no_message,
        )
        amqp_channel.basic_ack(method_frame.delivery_tag)
        message = self.deserialize(body)
        self.resolve.set_result((channel, message))

    def no_message(self, channels, method_frame):

        if channels:
            self.receive(
                channels=channels,
                block=False,
            )
        else:
            self.resolve.set_result((None, None))

    def no_queue(self, channels, amqp_channel, code, msg):

        self.process(
            self.ident,
            (RECEIVE, (channels, False), {}),
            self.resolve,
        )

    # New channel.

    def new_channel(self):

        self.amqp_channel.queue_declare(
            lambda method_frame: self.resolve.set_result(
                method_frame.method.queue,
            ),
        )

    # Groups.

    def group_add(self, group, channel):

        # FIXME: Is it possible to do this things in parallel?
        self.expire_group_member(group, channel)

        def after_bind(method_frame):

            self.resolve.set_result(None)

        def bind_channel(method_frame):

            self.amqp_channel.queue_bind(
                after_bind,
                queue=self.get_queue_name(channel),
                exchange=channel,
            )

        def bind_group(method_frame):

            self.amqp_channel.exchange_bind(
                bind_channel,
                destination=channel,
                source=group,
            )

        def declare_channel(method_frame):
            if '!' in channel or '?' in channel:
                bind_group(None)
            else:
                self.amqp_channel.queue_declare(
                    bind_group,
                    queue=channel,
                    arguments={'x-dead-letter-exchange': self.dead_letters},
                )

        def declare_member(method_frame):
            self.amqp_channel.exchange_declare(
                declare_channel,
                exchange=channel,
                exchange_type='fanout',
            )

        self.amqp_channel.exchange_declare(
            declare_member,
            exchange=group,
            exchange_type='fanout',
        )

    def group_discard(self, group, channel):

        self.amqp_channel.exchange_unbind(
            lambda method_frame: self.resolve.set_result(None),
            destination=channel,
            source=group,
        )

    def send_group(self, group, message):

        body = self.serialize(message)
        self.amqp_channel.basic_publish(
            exchange=group,
            routing_key='',
            body=body,
            properties=self.publish_properties,
        )
        self.resolve.set_result(None)

    # Dead letters processing.

    def expire_group_member(self, group, channel):

        ttl = self.group_expiry * 1000
        self.amqp_channel.queue_declare(
            partial(self.push_marker, group, channel),
            queue=self.get_expire_marker(group, channel),
            arguments={
                'x-dead-letter-exchange': self.dead_letters,
                'x-message-ttl': ttl,
                # FIXME: make this delay as little as possible.
                'x-expires': ttl + 500,
                'x-max-length': 1,
            },
        )

    def push_marker(self, group, channel, method_frame):

        body = self.serialize({
            'group': group,
            'channel': channel,
        })
        self.amqp_channel.basic_publish(
            exchange='',
            routing_key=self.get_expire_marker(group, channel),
            body=body,
        )

    def get_expire_marker(self, group, channel):

        return 'expire.bind.%s.%s' % (group, self.get_queue_name(channel))

    def declare_dead_letters(self):

        def consume(method_frame):

            self.amqp_channel.basic_consume(
                self.on_dead_letter,
                queue=self.dead_letters,
            )

        def do_bind(method_frame):

            self.amqp_channel.queue_bind(
                consume,
                queue=self.dead_letters,
                exchange=self.dead_letters,
            )

        def declare_queue(method_frame):

            self.amqp_channel.queue_declare(
                do_bind,
                queue=self.dead_letters,
            )

        self.amqp_channel.exchange_declare(
            declare_queue,
            exchange=self.dead_letters,
            exchange_type='fanout',
        )

    def on_dead_letter(self, amqp_channel, method_frame, properties, body):

        amqp_channel.basic_ack(method_frame.delivery_tag)
        # FIXME: what the hell zero means here?
        queue = properties.headers['x-death'][0]['queue']
        reason = properties.headers['x-death'][0]['reason']
        if self.is_expire_marker(queue) and reason == 'expired':
            message = self.deserialize(body)
            group = message['group']
            channel = message['channel']
            self.group_discard(group, channel)
        elif not self.is_expire_marker(queue):
            amqp_channel.exchange_delete(exchange=queue)

    def is_expire_marker(self, queue):

        return queue.startswith('expire.bind.')

    # Serialization.

    def serialize(self, message):

        value = msgpack.packb(message, use_bin_type=True)
        if self.crypter:
            value = self.crypter.encrypt(value)
        return value

    def deserialize(self, message):

        if self.crypter:
            message = self.crypter.decrypt(message, self.expiry + 10)
        return msgpack.unpackb(message, encoding='utf8')


class LayerChannel(Channel):

    def __init__(self, *args, **kwargs):

        self.on_callback_error_callback = None
        super(LayerChannel, self).__init__(*args, **kwargs)

    def _on_getok(self, method_frame, header_frame, body):

        try:
            super(LayerChannel, self)._on_getok(method_frame, header_frame,
                                                body)
        except Exception as error:
            if self.on_callback_error_callback:
                self.on_callback_error_callback(error)


class LayerConnection(SelectConnection):

    Channel = LayerChannel

    def __init__(self, *args, **kwargs):

        self.on_callback_error_callback = kwargs.pop(
            'on_callback_error_callback',
        )
        self.lock = kwargs.pop('lock')
        super(LayerConnection, self).__init__(*args, **kwargs)

    def _process_frame(self, frame_value):

        with self.lock:
            return super(LayerConnection, self)._process_frame(frame_value)

    def _process_callbacks(self, frame_value):

        try:
            return super(LayerConnection, self)._process_callbacks(frame_value)
        except Exception as error:
            self.on_callback_error_callback(error)
            raise

    def _create_channel(self, channel_number, on_open_callback):

        return self.Channel(self, channel_number, on_open_callback)


class RabbitmqConnection(object):

    Parameters = URLParameters
    Connection = LayerConnection
    Protocol = Protocol

    def __init__(self, url, expiry, group_expiry, get_capacity, crypter):

        self.url = url
        self.expiry = expiry
        self.group_expiry = group_expiry
        self.get_capacity = get_capacity
        self.crypter = crypter

        self.protocols = {}
        self.lock = Lock()
        self.is_open = Event()
        self.parameters = self.Parameters(self.url)
        self.connection = self.Connection(
            parameters=self.parameters,
            on_open_callback=self.start_loop,
            on_close_callback=self.notify_futures,
            on_callback_error_callback=self.protocol_error,
            stop_ioloop_on_close=False,
            lock=self.lock,
        )

    def run(self):

        self.connection.ioloop.start()

    def start_loop(self, connection):

        self.is_open.set()
        self.process(None, (DECLARE_DEAD_LETTERS, (), {}), Future())

    def process(self, ident, method, future):

        if (ident in self.protocols and
                self.protocols[ident].amqp_channel.is_open):
            self.protocols[ident].resolve = future
            self.protocols[ident].apply(*method)
            return
        protocol = self.Protocol(self.expiry, self.group_expiry,
                                 self.get_capacity, ident, self.process,
                                 self.crypter)
        protocol.resolve = future
        amqp_channel = self.connection.channel(
            partial(protocol.register_channel, method),
        )
        amqp_channel.on_callback_error_callback = protocol.protocol_error
        self.protocols[ident] = protocol

    def notify_futures(self, connection, code, msg):

        try:
            for protocol in self.protocols.values():
                protocol.resolve.set_exception(ConnectionClosed())
        finally:
            self.connection.ioloop.stop()

    def protocol_error(self, error):

        for protocol in self.protocols.values():
            protocol.resolve.set_exception(error)

    def schedule(self, f, *args, **kwargs):

        if self.connection.is_closing or self.connection.is_closed:
            raise ConnectionClosed
        self.is_open.wait()
        future = Future()
        with self.lock:
            self.process(get_ident(), (f, args, kwargs), future)
        return future


class ConnectionThread(Thread):
    """
    Thread holding connection.

    Separate heartbeat frames processing from actual work.
    """

    Connection = RabbitmqConnection

    def __init__(self, url, expiry, group_expiry, get_capacity, crypter):

        super(ConnectionThread, self).__init__()
        self.daemon = True
        self.connection = self.Connection(url, expiry, group_expiry,
                                          get_capacity, crypter)

    def run(self):

        self.connection.run()

    def schedule(self, f, *args, **kwargs):

        return self.connection.schedule(f, *args, **kwargs)


class RabbitmqChannelLayer(BaseChannelLayer):

    extensions = ['groups']

    Thread = ConnectionThread

    def __init__(self,
                 url,
                 expiry=60,
                 group_expiry=86400,
                 capacity=100,
                 channel_capacity=None,
                 symmetric_encryption_keys=None):

        super(RabbitmqChannelLayer, self).__init__(
            expiry=expiry,
            group_expiry=group_expiry,
            capacity=capacity,
            channel_capacity=channel_capacity,
        )
        if symmetric_encryption_keys:
            try:
                from cryptography.fernet import MultiFernet
            except ImportError:
                raise ValueError("Cannot run without 'cryptography' installed")
            sub_fernets = [
                self.make_fernet(key) for key in symmetric_encryption_keys
            ]
            crypter = MultiFernet(sub_fernets)
        else:
            crypter = None
        self.thread = self.Thread(url, expiry, group_expiry, self.get_capacity,
                                  crypter)
        self.thread.start()

    def make_fernet(self, key):

        from cryptography.fernet import Fernet
        key = key.encode('utf8')
        formatted_key = base64.urlsafe_b64encode(hashlib.sha256(key).digest())
        return Fernet(formatted_key)

    def send(self, channel, message):

        future = self.thread.schedule(SEND, channel, message)
        return future.result()

    def receive(self, channels, block=False):

        future = self.thread.schedule(RECEIVE, channels, block)
        return future.result()

    def new_channel(self, pattern):

        assert pattern.endswith('!') or pattern.endswith('?')
        future = self.thread.schedule(NEW_CHANNEL)
        queue_name = future.result()
        channel = pattern + queue_name[8:]  # Remove 'amq.gen-' prefix.
        return channel

    def declare_channel(self, channel):

        future = self.thread.schedule(DECLARE_CHANNEL, channel)
        return future.result()

    def group_add(self, group, channel):

        future = self.thread.schedule(GROUP_ADD, group, channel)
        return future.result()

    def group_discard(self, group, channel):

        future = self.thread.schedule(GROUP_DISCARD, group, channel)
        return future.result()

    def send_group(self, group, message):

        future = self.thread.schedule(SEND_GROUP, group, message)
        return future.result()


# TODO: is it optimal to read bytes from content frame, call python
# decode method to convert it to string and than parse it with
# msgpack?  We should minimize useless work on message receive.


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
