import grpc
import os
import sys
from .utils import deserializer, serializer
from typing import Optional, Callable, TypeVar


class MessageStoreClient():
    def __init__(self, host):
        self.host = host

    def __enter__(self):
        self._channel = grpc.insecure_channel(self.host)
        self._send_command_req = self._channel.unary_unary(
            '/MessageStore/SendCommand', request_serializer=serializer, response_deserializer=deserializer)
        self._emit_event_req = self._channel.unary_unary(
            '/MessageStore/EmitEvent', request_serializer=serializer, response_deserializer=deserializer)
        self._read_last_message = self._channel.unary_unary(
            '/MessageStore/ReadLastMessage', request_serializer=serializer, response_deserializer=deserializer)
        self._subscribe_req = self._channel.unary_stream(
            '/MessageStore/Subscribe', request_serializer=serializer, response_deserializer=deserializer)
        self._run_projector = self._channel.unary_stream(
            '/MessageStore/RunProjector', request_serializer=serializer, response_deserializer=deserializer)
        return self

    def send_command(self, command: str, category: str, data=None, metadata=None, _id: str = None, expected_version: int = None):
        return self._send_command_req({
            'command': command,
            'category': category,
            'data': data,
            'metadata': metadata,
            'id': _id,
            'expectedVersion': expected_version
        })

    def emit_event(self, event: str, category: str, data=None, metadata=None, _id: str = None, expected_version: int = None):
        return self._emit_event_req({
            'event': event,
            'category': category,
            'data': data,
            'metadata': metadata,
            'id': _id,
            'expectedVersion': expected_version
        })

    def read_last_message(self, stream_name: str):
        return self._read_last_message({
            'streamName': stream_name
        })

    def run_projector(self, stream_name: str, until_position: int = None, reducer, initialValue):
        iterator = self._run_projector({
            'streamName': stream_name,
            'untilPosition': until_position
        })
        state = initialValue
        for item in iterator:
            state = reducer(state, item)
        return state

    def subscribe(self, stream_name: str, handler, options={}):
        iterator = self._subscribe_req({
            'streamName': stream_name,
            'subscribedId': options.get('subscriber_id'),
            'tickDelayMs': options.get('tick_delay_ms'),
            'lastPosition': options.get('last_position'),
            'consumerGroupSize': options.get('consumer_group_size'),
            'consumerGroupMember': options.get('consumer_group_member'),
            'positionUpdateInterval': options.get('position_update_interval'),
            'idleUpdateInterval': options.get('idle_update_interval')
        })
        for item in iterator:
            handler(item)

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._channel.close()
