import redis
import struct
import os
import base64
import hashlib
import datetime
import pandas as pd
import numpy as np
from collections import namedtuple
from redis.connection import UnixDomainSocketConnection


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

def _round_w_fri(date):
    date = pd.to_datetime(date).date()
    return date - datetime.timedelta(days=date.weekday()) + datetime.timedelta(days=4)

Periodicity = namedtuple('Periodicity',['pandas_offset','epoque','offset_function'])

PERIODICITIES = {
    'B' : Periodicity(pd.tseries.offsets.BDay(), datetime.date(1970,1,1), lambda epoque, date: np.busday_count(epoque,pd.to_datetime(date).date())),
    'W-FRI' : Periodicity(pd.tseries.offsets.Week(weekday=4), datetime.date(1970,1,2), lambda epoque, date: (_round_w_fri(date) - epoque).days / 7)
}


def get_index(periodicity_code, date):
    p = PERIODICITIES[periodicity_code]
    return p.offset_function(p.epoque, date)

def get_date(periodicity_code, index):
    p = PERIODICITIES[periodicity_code]
    return (p.epoque + p.pandas_offset * index).date()

METADATA_FORMAT = '<6s6sll'
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)

class Metadata:
    __slots__ = ['dtype','periodicity_code','start_index','end_index']

    def __init__(self, dtype, periodicity_code, start_index, end_index):
        self.dtype = dtype
        self.periodicity_code = periodicity_code
        self.start_index = start_index
        self.end_index = end_index

    def start_date(self):
        return get_date(self.periodicity_code, self.start_index)

    def end_date(self):
        return get_date(self.periodicity_code, self.end_index)

def read_metadata(key):
    data = redis.Redis(connection_pool=REDIS_POOL).getrange(key,0,METADATA_SIZE-1)
    if len(data) == 0:
        return None
    s = struct.unpack(METADATA_FORMAT, data)
    return Metadata(np.dtype(s[0].decode().strip()), s[1].decode().strip(), s[2], s[3])

def get_metadata_for_series(series):
    if series.index.freq is None:
        raise Exception('Missing index freq.')
    start_index = get_index(series.index.freq.name, series.index[0])
    return Metadata(series.dtype, series.index.freq.name, start_index, start_index + len(series.index)) 

def pack_metadata(metadata):
    return struct.pack(METADATA_FORMAT, '{0: <6}'.format(metadata.dtype.str).encode(),
                '{0: <6}'.format(metadata.periodicity_code).encode(), metadata.start_index, metadata.end_index)

def metadata_to_date_range(metadata):
    return pd.date_range(get_date(metadata.periodicity_code, metadata.start_index),
                periods=metadata.end_index - metadata.start_index, freq=metadata.periodicity_code)

def update_metadata_end_index(key, end_index):
    redis.Redis(connection_pool=REDIS_POOL).setrange(key, METADATA_SIZE - struct.calcsize('<l'), struct.pack('<l',end_index))

def write(key, metadata, data):
    redis.Redis(connection_pool=REDIS_POOL).set(key, pack_metadata(metadata) + data)
    
def append(key, data):
    redis.Redis(connection_pool=REDIS_POOL).append(key, data)

def delete(key):
    redis.Redis(connection_pool=REDIS_POOL).delete(key)

def getrange(key, start, end):
    return redis.Redis(connection_pool=REDIS_POOL).getrange(key, str(METADATA_SIZE + start), str(end))
    
def hset(key, item, value):
    redis.Redis(connection_pool=REDIS_POOL).hset(key, item, value)

def hmset(key, item_value_dict):
    redis.Redis(connection_pool=REDIS_POOL).hmset(key, item_value_dict)

def hget(key, item):
    return redis.Redis(connection_pool=REDIS_POOL).hget(key, item)

def hmget(key, items):
    return redis.Redis(connection_pool=REDIS_POOL).hmget(key, items)

def hgetall(key):
    return redis.Redis(connection_pool=REDIS_POOL).hgetall(key)

