import redis
import struct
import numpy as np
import pandas as pd
from collections import namedtuple
from redis.connection import UnixDomainSocketConnection
from pynto.time import *
from pynto.main import _Word,Column

SERIES_PREFIX = 'l.s'
FRAME_PREFIX = 'l.f'
HASH_PREFIX = 'l.h'

Type = namedtuple('Type', ['code', 'pad_value', 'length'])

TYPES = {
    '<f8': Type('<f8',np.nan,8),
    '|b1': Type('|b1',False,1),
    '<i8': Type('<i8',0,8)
}

METADATA_FORMAT = '<6s6sll'
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)

Metadata = namedtuple('Metadata', ['dtype','periodicity','start','end'])


class Lima(object):

    def __init__(self, host='127.0.0.1', port=6379, db=0,
                        password=None, socket_path=None):
        if socket_path is None:
            self._pool = redis.ConnectionPool(host=host, port=port, db=db, password=password) 
        else:
            self._pool = redis.ConnectionPool(connection_class=UnixDomainSocketConnection,path=socket_path, db=db, password=password) 
    
    def _get_redis(self):
        return redis.Redis(connection_pool=self._pool)

    def _read_metadata(self, key):
        data = self._get_redis().getrange(key,0,METADATA_SIZE-1)
        if len(data) == 0:
            return None
        s = struct.unpack(METADATA_FORMAT, data)
        return Metadata(s[0].decode().strip(), s[1].decode().strip(), s[2], s[3])

    def _get_metadata_for_series(self, series):
        if series.index.freq is None:
            raise Exception('Missing index freq.')
        start = get_index(series.index.freq.name, series.index[0])
        return Metadata(series.dtype.str, series.index.freq.name,
                                start, start + len(series.index)-1) 

    def _update_end(self, key, end):
        self._get_redis().setrange(key, METADATA_SIZE - struct.calcsize('<l'), struct.pack('<l',int(end)))

    def _write(self, key, metadata, data):
        packed_md = struct.pack(METADATA_FORMAT,
                                '{0: <6}'.format(metadata.dtype).encode(),
                                '{0: <6}'.format(metadata.periodicity).encode(),
                                metadata.start,
                                metadata.end) 
        self._get_redis().set(key, packed_md + data)
        
    def _append(self, key, data):
        self._get_redis().append(key, data)

    def _delete(self, key):
        self._get_redis().delete(key)

    def _get_data_range(self, key, start, end):
        end = -1 if end == -1 else METADATA_SIZE + end - 1
        return self._get_redis().getrange(key, str(METADATA_SIZE + start), str(end))
        
    def _set_data_range(self, key, start, data):
        self._get_redis().setrange(key, str(METADATA_SIZE + start), data)
        
    def _hash_set(self, key, item, value=None):
        if value is None:
            self._get_redis().hmset(key, item)
        else:
            self._get_redis().hset(key, item, value)

    def _hash_get(self, key, item=None):
        if item is None:
            return self._get_redis().hgetall(key)
        if isinstance(item, list) or isinstance(item, tuple):
            return self._get_redis().hmget(key, item)
        return self._get_redis().hget(key, item)

    def _list_keys(self, match='*'):
        return [key for key in self._get_redis().scan_iter(match=match)]

    def read_series(self, key, start=None, end=None, periodicity=None,
                            resample_method='last', as_series=True):
        series_key = f'{SERIES_PREFIX}.{key}'
        md = self._read_metadata(series_key)
        if md is None:
            raise KeyError(f'No series data for key: "{key}".')
        needs_resample = not (periodicity is None or periodicity == md.periodicity)
        if needs_resample:
            if not start is None:
                start_index = get_index(md.periodicity,get_date(periodicity,get_index(periodicity,start)))
            else:
                start_index = md.start
            if not end is None:
                end_index = get_index(md.periodicity,get_date(periodicity,get_index(periodicity,end)))
            else:
                end_index = md.end
        else:
            start_index = md.start if start is None else get_index(md.periodicity, start)
            end_index = md.end if end is None else get_index(md.periodicity, end)
        periodicity = periodicity if periodicity else md.periodicity
        if start_index <= md.end and end_index >= md.start:
            itemsize = np.dtype(md.dtype).itemsize 
            selected_start = max(0, start_index - md.start)
            selected_end = min(end_index, md.end) - md.start + 1
            buff = self._get_data_range(series_key, selected_start * itemsize, selected_end * itemsize)
            data = np.frombuffer(buff, md.dtype)
            if len(data) != end_index - start_index + 1:
                output_start = max(0, md.start - start_index)
                output = np.full(end_index - start_index + 1,TYPES[md.dtype].pad_value)
                output[output_start:output_start+len(data)] = data
            else:
                output = data
        else:
            output = np.full(end_index - start_index + 1,TYPES[md.dtype].pad_value)
        if not (as_series or needs_resample):
            return (start_index, end_index, md.periodicity, output)
        s = pd.Series(output, index=Range(start_index,end_index,md.periodicity).to_pandas(), name=key)
        if needs_resample:
            s = getattr(s.ffill().resample(periodicity),resample_method)().reindex(Range(start,end,periodicity).to_pandas())       
            if not as_series:
                return (get_index(periodicity,s.index[0]), get_index(periodicity,s.index[-1]), periodicity, s.values)
        return s

    def write_series(self, key, series):
        series_key = f'{SERIES_PREFIX}.{key}'
        series_md = self._get_metadata_for_series(series)
        saved_md = self._read_metadata(series_key)
        if saved_md and saved_md.periodicity != series_md.periodicity:
            raise Exception(f'Incompatible periodicity.')   
        data = series.values
        if saved_md is None or series_md.start < saved_md.start:
            self._write(series_key, series_md, data.tostring())
            return
        if series_md.start > saved_md.end:
            pad = np.full(series_md.start - saved_md.end - 1, TYPES[saved_md.dtype].pad_value)
            data = np.hstack([pad, data])
            start = saved_md.end + 1
        else:
            start = series_md.start
        start_offset = (start - saved_md.start) * np.dtype(saved_md.dtype).itemsize
        self._set_data_range(series_key, start_offset, data.tostring())
        if series_md.end > saved_md.end:
            self._update_end(series_key, series_md.end)

    def delete_series(self, key):
        self._delete(f'{SERIES_PREFIX}.{key}')

    def read_series_metadata(self, key, date_range=None):
        series_key = f'{SERIES_PREFIX}.{key}'
        return self._read_metadata(series_key)

    def truncate_series(self, key, end_date):
        series_key = f'{SERIES_PREFIX}.{key}'
        md = self._read_metadata(series_key)
        end_index = get_index(md.periodicity, end_date)
        if end_index < md.end:
            self._update_end(series_key, end_index)

    def list_series(self, match='*'):
        return [key.decode().replace(SERIES_PREFIX + '.','') for key in self._list_keys(f'{SERIES_PREFIX}.{match}')]

    def read_frame_headers(self, key):
        frame_key = f'{FRAME_PREFIX}.{key}'
        return self._get_data_range(frame_key, 0, -1).decode().split('\t')

    def read_frame_series_keys(self, key):
        return [f'{key}.{c}' for c in self.read_frame_headers(key)]

    def read_frame(self, key, start=None, end=None, periodicity=None,
                            resample_method='last', as_frame=True):
        frame_key = f'{FRAME_PREFIX}.{key}'
        md = self._read_metadata(frame_key)
        if md is None:
            raise KeyError(f'No frame data for key: "{key}".')
        start = md.start if start is None else get_index(md.periodicity, start)
        end = md.end if end is None else get_index(md.periodicity, end)
        periodicity = md.periodicity if periodicity is None else periodicity
        columns = self.read_frame_headers(key)
        data = np.column_stack([self.read_series(f'{key}.{c}', start, end, periodicity,
                    resample_method, as_series=False)[3] for c in columns])
        if not as_frame:
            return data
        return pd.DataFrame(data, columns=columns,
                    index=Range(start, end, periodicity).to_pandas())

    def write_frame(self, key, frame):
        frame_key = f'{FRAME_PREFIX}.{key}'
        md = self._read_metadata(frame_key)
        end = get_index(frame.index.freq.name, frame.index[-1].date())
        if md is None:
            start = get_index(frame.index.freq.name, frame.index[0].date())
            md = Metadata('<U', frame.index.freq.name, start, end)
            columns = set()
            first_save = True
        else:
            if md.periodicity != frame.index.freq.name:
                raise Exception('Incompatible periodicity.')
            columns = set(get_data_range(frame_key, 0, -1).decode().split('\t'))
            if end > md.end:
                self._update_end(frame_key, end)
            first_save = False
        new_columns = []
        for column,series in frame.iteritems():
            series_code = f'{key}.{column}'
            if not column in columns:
                columns.add(column)
                new_columns.append(column)
            series = series[series.first_valid_index():series.last_valid_index()]
            self.write_series(series_code, series)
        if first_save:
            self._write(frame_key, md, '\t'.join(new_columns).encode()) 
        elif len(new_columns) > 0:
            self._append(frame_key, ('\t' + '\t'.join(new_columns)).encode()) 

    def delete_frame(self, key):
        for series in self.read_frame_series_keys(key):
            self.delete_series(series)
        self._delete(f'{FRAME_PREFIX}.{key}')

    def read_frame_metadata(self, key):
        frame_key = f'{FRAME_PREFIX}.{key}'
        return self._read_metadata(frame_key)

    def truncate_frame(self, key, end_date):
        frame_key = f'{FRAME_PREFIX}.{key}'
        md = self._read_metadata(frame_key)
        end_index = get_index(md.periodicity, end_date)
        if end_index < md.end:
            self._update_end(frame_key, end_index)

    def list_frames(self, match='*'):
        return [key.decode().replace(FRAME_PREFIX + '.','') for key in self._list_keys(f'{FRAME_PREFIX}.{match}')]

    def read_hash_item(self, key, item):
        values = self._hash_get(f'{HASH_PREFIX}.{key}', item) 
        if not isinstance(item,list):
            values = [values]
        return [v.decode() if not v is None else None for v in values ]

    def read_hash(self, key):
        values = self._hash_get(f'{HASH_PREFIX}.{key}')
        return {k.decode(): v.decode() for k,v in values.items()} if len(values) > 0 else None
        
    def write_hash(self, key, items):
        self._hash_set(f'{HASH_PREFIX}.{key}', items)

    def write_hash_item(self, key, item, value):
        self._hash_set(f'{HASH_PREFIX}.{key}', item, value)

    def delete_hash(self, key):
        self._delete(f'{HASH_PREFIX}.{key}')

    def series_col(self, series_key):
        return _PyntoSeries(self, series_key)

    def frame_cols(self, frame_key):
        return _PyntoFrame(self, frame_key)

class _PyntoSeries(_Word):
    def __init__(self, lima, series_key):
        def lima_series(stack):
            def lima_col(date_range, lima=lima, series_key=series_key):
                return lima.read_series(series_key, date_range.start, date_range.end, date_range.periodicity, as_series=False)[3]
            stack.append(Column(series_key, f'lima series:{series_key}', lima_col))
        super().__init__(lima_series)

class _PyntoFrame(_Word):
    def __init__(self, lima, frame_key):
        def lima_frame(stack):
            for header in lima.read_frame_headers(frame_key):
                def lima_col(date_range, lima=lima, series_key=f'{frame_key}.{header}'):
                    return lima.read_series(series_key, date_range.start, date_range.end, date_range.periodicity, as_series=False)[3]
                stack.append(Column(header, f'{frame_key}:{header}', lima_col))
        super().__init__(lima_frame)
