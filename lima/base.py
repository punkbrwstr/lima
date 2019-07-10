import redis
import struct
import os
import base64
import hashlib
import datetime
import pandas as pd
import numpy as np
from collections import namedtuple
from dateutil.relativedelta import relativedelta
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

def round_bday(date):
    return date - datetime.timedelta(days=max(0, date.weekday() - 4))

def round_w_fri(date):
    date = pd.to_datetime(date).date()
    return date - datetime.timedelta(days=date.weekday()) + datetime.timedelta(days=4)

def round_bq_dec(d):
    remainder = d.month % 3
    plusmonths = 0 if remainder == 0 else 2 // remainder
    d = d +  relativedelta(months=plusmonths)
    return d

Periodicity = namedtuple('Periodicity',['pandas_offset','epoque','round_function','offset_function'])

PERIODICITIES = {
    'B' : Periodicity(pd.tseries.offsets.BDay(),
            datetime.date(1970,1,1),
            lambda d: d,
            lambda epoque, date: np.busday_count(epoque,pd.to_datetime(date).date())),
    'W-FRI' : Periodicity(pd.tseries.offsets.Week(weekday=4),
            datetime.date(1970,1,2),
            round_w_fri,
            lambda epoque, date: (date - epoque).days / 7),
    'BM' : Periodicity(pd.tseries.offsets.BusinessMonthEnd(),
            datetime.date(1970,1,30),
            lambda d: d,
            lambda d2, d1: (d1.year - d2.year) * 12 + d1.month - d2.month),
    'BQ-DEC' : Periodicity(pd.tseries.offsets.BQuarterEnd(startingMonth=3),
            datetime.date(1970,3,31),
            round_bq_dec,
            lambda d2, d1: ((d1.year - d2.year) * 12 + d1.month - d2.month) // 3)

}


def get_index(periodicity_code, date):
    p = PERIODICITIES[periodicity_code]
    return p.offset_function(p.epoque, p.round_function(date))

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
        self.start_index = int(start_index)
        self.end_index = int(end_index)

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
    redis.Redis(connection_pool=REDIS_POOL).setrange(key, METADATA_SIZE - struct.calcsize('<l'), struct.pack('<l',int(end_index)))

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


_LUA_ARCHIVE = """
    local new_key = string.gsub(KEYS[1],'^l.','l.a.') .. '.' .. redis.call('TIME')[1]
    redis.call("RESTORE", new_key, 0, redis.call("DUMP", KEYS[1]))
    return "OK"
""".strip()

_LUA_ARCHIVE_HASH = hashlib.sha1(REDIS_POOL.get_encoder().encode(_LUA_ARCHIVE)).hexdigest()

def archive(key):
    #return redis.Redis(connection_pool=REDIS_POOL).eval(_LUA_ARCHIVE, 1, key)
    return redis.Redis(connection_pool=REDIS_POOL).evalsha(_LUA_ARCHIVE_HASH, 1, key)
