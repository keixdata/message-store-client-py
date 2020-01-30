import grpc
import os
import sys
from .utils import deserializer, serializer

class MessageStoreClient():
  def __init__(self, host):
    self.host = host
  
  def __enter__(self):
    self._channel = grpc.insecure_channel(self.host)
    self._send_command_req = self._channel.unary_unary('/EventStore/SendCommand', request_serializer=serializer, response_deserializer=deserializer)
    self._emit_event_req = self._channel.unary_unary('/EventStore/EmitEvent', request_serializer=serializer, response_deserializer=deserializer)
    self._subscribe_req = self._channel.unary_stream('/EventStore/Subscribe', request_serializer=serializer, response_deserializer=deserializer)
    return self

  def send_command(self, command, category, data = None, metadata = None, _id = None, expected_version = None):
    return self._send_command_req({
      'command': command,
      'category': category,
      'data': data,
      'metadata': metadata,
      'id': _id,
      'expectedVersion': expected_version
    })

  def emit_event(self, event, category, data = None, metadata = None, _id = None, expected_version = None):
    return self._emit_event_req({
      'event': event,
      'category': category,
      'data': data,
      'metadata': metadata,
      'id': _id,
      'expectedVersion': expected_version
    })
  
  def subscribe(self, stream_name, handler, options = {}):
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
