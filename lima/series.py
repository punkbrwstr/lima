from lima.config import *
from lima.time import *
import io
import redis
import json
import struct
import pandas as pd
import numpy as np
import datetime

__all__ = ['read_series','write_series', 'delete_series']

_PAD_VALUE = {'d': np.nan, '?': False}

_METADATA_FORMAT = '<6s6sl'
_METADATA_SIZE = struct.calcsize(_METADATA_FORMAT)

class Metadata:
    __slots__ = 'type','periodicity_code','start_index','end_index'

    @classmethod
    def read(cls, key):
        r = redis.Redis(connection_pool=REDIS_POOL)
        meta_data = r.getrange(key,0,_METADATA_SIZE-1)
        if len(meta_data) == 0:
            return None
        s = struct.unpack(_METADATA_FORMAT, meta_data)
        md = cls()
        md.type = np.dtype(s[0].decode().strip())
        md.periodicity_code = s[1].decode().strip()
        md.start_index = s[2]
        md.end_index = md.start_index + (int(r.strlen(key))-_METADATA_SIZE) // md.type.itemsize
        return md

    @classmethod
    def for_series(cls, series):
        if series.index.freq is None:
            raise Exception('Missing index freq.')
        md = cls()
        md.type = series.dtype
        md.periodicity_code = series.index.freq.name
        md.start_index = get_index(md.periodicity_code, series.index[0])
        md.end_index = get_index(md.periodicity_code, series.index[-1]) + 1
        return md

    def date_range(self):
        return pd.date_range(get_date(self.periodicity_code, self.start_index),
                    get_date(self.periodicity_code, self.end_index-1),freq=self.periodicity_code)

    def dates(self):
        return (get_date(self.periodicity_code, self.start_index), get_date(self.periodicity_code, self.end_index-1))

    def pack(self):
        return struct.pack(_METADATA_FORMAT, '{0: <6}'.format(self.type.str).encode(),
                '{0: <6}'.format(self.periodicity_code).encode(), self.start_index)

    def check_compat(self, other):
        if self.periodicity_code != other.periodicity_code:
            raise Exception('Incompatible periodicity.')
        if self.type.char != other.type.char:
            raise Exception('Incompatible type.')

def read_series(key, date_range=None, nas_if_missing=False):
    r = redis.Redis(connection_pool=REDIS_POOL)
    series_key = f'{SERIES_PREFIX}.{key}'
    saved_md = Metadata.read(series_key)
    if saved_md is None:
        if date_range is None or not nas_if_missing:
            raise Exception(f'No series data for key: "{key}".')
        return pd.Series(np.full(len(date_range),np.nan),index=date_range)
    if date_range is None:
        date_range = saved_md.date_range()
    start_index = get_index(saved_md.periodicity_code, date_range[0])
    end_index = get_index(saved_md.periodicity_code, date_range[-1])
    if not (start_index > saved_md.end_index or end_index < saved_md.start_index):
        selected_start = max(0, start_index - saved_md.start_index)
        selected_end = min(-1, (end_index - saved_md.end_index + 1) * saved_md.type.itemsize - 1)
        buff = r.getrange(series_key, str(selected_start * saved_md.type.itemsize + _METADATA_SIZE), str(selected_end))
        data = np.frombuffer(buff, saved_md.type)
        output_start = max(0, saved_md.start_index - start_index)
    else:
        output_start = None
    if output_start == 0 and len(data) == len(date_range):
        output = data
    else:
        if saved_md.type.char not in _PAD_VALUE:
            raise Exception(f'Unable to pad data for type: "{saved_md.type.char}".')
        output = np.full(len(date_range),_PAD_VALUE[saved_md.type.char])
        if not output_start is None:
            output[output_start:output_start+len(data)] = data
    return pd.Series(output, index=date_range, name=key)

def write_series(key, series, tables={}):
    series_key = f'{SERIES_PREFIX}.{key}'
    metadata = Metadata.for_series(series)
    r = redis.Redis(connection_pool=REDIS_POOL)
    saved_md = Metadata.read(series_key)
    if saved_md is None:
        r.set(series_key, metadata.pack() + series.values.tostring())
    else:
        saved_md.check_compat(metadata)
        if metadata.end_index < saved_md.end_index:
            pass
        elif metadata.start_index < saved_md.end_index:
            r.append(series_key, series.values[saved_md.end_index - metadata.start_index:].tostring())
        else:
            if metadata.start_index > saved_md.end_index:
                if saved_md.type.char not in _PAD_VALUE:
                    raise Exception(f'Unable to pad data for type: "{saved_md.type.char}".')
                r.append(series_key, np.full(metadata.start_index - saved_md.end_index, _PAD_VALUE[saved_md.type.char]).tostring())
            r.append(series_key, series.values.tostring())

def delete_series(key):
    r = redis.Redis(connection_pool=REDIS_POOL)
    r.delete(f'{SERIES_PREFIX}.{key}')
