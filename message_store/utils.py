import msgpack
import datetime

def ext_hook(code, data):
    if code == -1:
        if len(data) == 4:
            secs = int.from_bytes(data, byteorder='big', signed=True)
            nsecs = 0
        elif len(data) == 8:
            data = int.from_bytes(data, byteorder='big', signed=False)
            secs = data & 0x00000003ffffffff
            nsecs = data >> 34
        elif len(data) == 12:
            import struct
            nsecs, secs = struct.unpack('!Iq', data)
        else:
            raise AssertionError("Not reached")
        return datetime.datetime.utcfromtimestamp(secs + nsecs / 1e9)

    else:
        return msgpack.ExtType(code, data)

def serializer(obj):
  return msgpack.packb(obj)

def deserializer(buf):
  return msgpack.unpackb(buf, raw=False, use_list=False, ext_hook=ext_hook)
