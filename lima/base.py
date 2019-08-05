import redis
import struct
import os
import base64
import hashlib
import calendar
import datetime
import numpy as np
from collections import namedtuple
from redis.connection import UnixDomainSocketConnection
from lima.time import *

SERIES_PREFIX = 'l.s'
FRAME_PREFIX = 'l.f'
HASH_PREFIX = 'l.h'

_ENV_VARS = {
    'host': 'LIMA_REDIS_HOST',
    'port': 'LIMA_REDIS_PORT',
    'db': 'LIMA_REDIS_DB',
    'password': 'LIMA_REDIS_PASSWORD',
    'path': 'LIMA_REDIS_SOCKET_PATH'
}

_ARGS = {name:os.environ.get(var, None) for name, var in _ENV_VARS.items() if os.environ.get(var, None)}

#_ARGS['db'] = 1

if 'path' in _ARGS:
    _ARGS.pop('host', None)
    _ARGS.pop('port', None)
    REDIS_POOL = redis.ConnectionPool(connection_class=UnixDomainSocketConnection, **_ARGS)
else:
    REDIS_POOL = redis.ConnectionPool(**_ARGS)

Type = namedtuple('Type', ['code', 'pad_value', 'length'])

TYPES = {
    '<f8': Type('<f8',np.nan,8),
    '|b1': Type('|b1',False,1),
    '<i8': Type('<i8',0,8)
}

METADATA_FORMAT = '<6s6sll'
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)

Metadata = namedtuple('Metadata', ['dtype','periodicity','start','end'])

def get_redis():
    return redis.Redis(connection_pool=REDIS_POOL)

def read_metadata(key):
    data = get_redis().getrange(key,0,METADATA_SIZE-1)
    if len(data) == 0:
        return None
    s = struct.unpack(METADATA_FORMAT, data)
    return Metadata(s[0].decode().strip(), s[1].decode().strip(), s[2], s[3])

def get_metadata_for_series(series):
    if series.index.freq is None:
        raise Exception('Missing index freq.')
    start = get_index(series.index.freq.name, series.index[0])
    return Metadata(series.dtype.str, series.index.freq.name,
                            start, start + len(series.index)-1) 

def update_end(key, end):
    get_redis().setrange(key, METADATA_SIZE - struct.calcsize('<l'), struct.pack('<l',int(end)))

def write(key, metadata, data):
    packed_md = struct.pack(METADATA_FORMAT,
                            '{0: <6}'.format(metadata.dtype).encode(),
                            '{0: <6}'.format(metadata.periodicity).encode(),
                            metadata.start,
                            metadata.end) 
    get_redis().set(key, packed_md + data)
    
def append(key, data):
    get_redis().append(key, data)

def delete(key):
    get_redis().delete(key)

def get_data_range(key, start, end):
    end = -1 if end == -1 else METADATA_SIZE + end - 1
    return get_redis().getrange(key, str(METADATA_SIZE + start), str(end))
    
def set_data_range(key, start, data):
    get_redis().setrange(key, str(METADATA_SIZE + start), data)
    
def hash_set(key, item, value=None):
    if value is None:
        get_redis().hmset(key, item)
    else:
        get_redis().hset(key, item, value)

def hash_get(key, item=None):
    if item is None:
        return get_redis().hgetall(key)
    if isinstance(item, list) or isinstance(item, tuple):
        return get_redis().hmget(key, item)
    return get_redis().hget(key, item)

def list_keys(match='*'):
    return [key for key in get_redis().scan_iter(match=match)]

_LUA_ARCHIVE = """
    local new_key = string.gsub(KEYS[1],'^l.','l.a.') .. '.' .. redis.call('TIME')[1]
    redis.call("RESTORE", new_key, 0, redis.call("DUMP", KEYS[1]))
    return "OK"
""".strip()

_LUA_ARCHIVE_HASH = hashlib.sha1(REDIS_POOL.get_encoder().encode(_LUA_ARCHIVE)).hexdigest()

def archive(key):
    #return redis.Redis(connection_pool=REDIS_POOL).eval(_LUA_ARCHIVE, 1, key)
    return redis.Redis(connection_pool=REDIS_POOL).evalsha(_LUA_ARCHIVE_HASH, 1, key)

