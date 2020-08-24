import redis
import struct
import numpy as np
import pandas as pd
from collections import namedtuple
from redis.connection import UnixDomainSocketConnection
from pynto.ranges import get_date, get_index, Range
from pynto.main import _Word,Column

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
            self._binary_pool = redis.ConnectionPool(host=host, port=port,
                                            db=db, password=password) 
            self._pool = redis.ConnectionPool(host=host, port=port,
                                    db=db, password=password,
                                    decode_responses=True) 
        else:
            self._binary_pool = redis.ConnectionPool(
                                    connection_class=UnixDomainSocketConnection,
                                    path=socket_path, db=db, password=password) 
            self._pool = redis.ConnectionPool(
                                    connection_class=UnixDomainSocketConnection,
                                    path=socket_path, db=db,
                                    password=password, decode_responses=True) 


    def __deepcopy__(self, memo):
        return self
    
    def get_binary_connection(self):
        return redis.Redis(connection_pool=self._binary_pool)

    def get_connection(self):
        return redis.Redis(connection_pool=self._pool)

    def read_metadata(self, key):
        data = self.get_binary_connection().getrange(key,0,METADATA_SIZE-1)
        if len(data) == 0:
            raise KeyError(f'No metadata for key {key}')
        s = struct.unpack(METADATA_FORMAT, data)
        return Metadata(s[0].decode().strip(), s[1].decode().strip(), s[2], s[3])


    def _update_end(self, key, end):
        self.get_binary_connection().setrange(key, METADATA_SIZE - struct.calcsize('<l'), struct.pack('<l',int(end)))

    def _write(self, key, metadata, data):
        packed_md = struct.pack(METADATA_FORMAT,
                                '{0: <6}'.format(metadata.dtype).encode(),
                                '{0: <6}'.format(metadata.periodicity).encode(),
                                metadata.start,
                                metadata.end) 
        self.get_binary_connection().set(key, packed_md + data)
        
    def _append(self, key, data):
        self.get_binary_connection().append(key, data)

    def _delete(self, key):
        self.get_binary_connection().delete(key)

    def _get_data_range(self, key, start, end):
        end = -1 if end == -1 else METADATA_SIZE + end - 1
        return self.get_binary_connection().getrange(key, str(METADATA_SIZE + start), str(end))
        
    def _set_data_range(self, key, start, data):
        self.get_binary_connection().setrange(key, str(METADATA_SIZE + start), data)
        

    def _list_keys(self, match='*'):
        return [key for key in self.get_connection().scan_iter(match=match)]

    def read_series_data(self, key, start=None, stop=None,
                            periodicity=None, resample_method='last'):
        md = self.read_metadata(key)
        if md is None:
            raise KeyError(f'No series data for key: "{key}".')
        needs_resample = not (periodicity is None or periodicity == md.periodicity)
        if needs_resample:
            if not start is None:
                start_index = get_index(md.periodicity,
                        get_date(periodicity,get_index(periodicity,start)))
            else:
                start_index = md.start
            if not stop is None:
                end_index = get_index(md.periodicity,
                        get_date(periodicity,get_index(periodicity,stop))) 
            else:
                end_index = md.end
        else:
            start_index = md.start if start is None else get_index(md.periodicity, start)
            end_index = md.end if stop is None else get_index(md.periodicity, stop)
        periodicity = periodicity if periodicity else md.periodicity
        if start_index < md.end and end_index >= md.start:
            itemsize = np.dtype(md.dtype).itemsize 
            selected_start = max(0, start_index - md.start)
            selected_end = min(end_index, md.end + 2) - md.start
            buff = self._get_data_range(key, selected_start * itemsize, selected_end * itemsize)
            data = np.frombuffer(buff, md.dtype)
            if len(data) != end_index - start_index:
                output_start = max(0, md.start - start_index)
                output = np.full(end_index - start_index,TYPES[md.dtype].pad_value)
                output[output_start:output_start+len(data)] = data
            else:
                output = data
        else:
            output = np.full(end_index - start_index,TYPES[md.dtype].pad_value)
        if needs_resample:
            s = pd.Series(output, index=Range.from_dates(
                            start_index,end_index,md.periodicity).to_index(), name=key)
            s = getattr(s.resample(periodicity),resample_method)().reindex(Range.from_dates(start, stop, periodicity).to_index())
            return (get_index(periodicity,s.index[0]), get_index(periodicity,s.index[-1]), periodicity, s.values)
        else:
            return (start_index, end_index, md.periodicity, output)

    def read_series(self, key, start=None, stop=None,
                        periodicity=None, resample_method='last'):
        data = self.read_series_data(key, start, stop, periodicity, resample_method)
        return pd.Series(data[3], index=Range.from_dates(*data[:3]).to_index(), name=key)

    def write_series(self, key, series):
        if series.index.freq is None:
            raise Exception('Missing index freq.')
        start = get_index(series.index.freq.name, series.index[0])
        series_md = Metadata(series.dtype.str, series.index.freq.name,
                                start, start + len(series.index)) 
        try:
            saved_md = self.read_metadata(key)
            if saved_md.periodicity != series_md.periodicity:
                raise Exception(f'Incompatible periodicity.')   
        except:
            saved_md = None
        data = series.values
        if saved_md is None or series_md.start < saved_md.start:
            self._write(key, series_md, data.tostring())
            return
        if series_md.start > saved_md.end + 1:
            pad = np.full(series_md.start - saved_md.end - 1, TYPES[saved_md.dtype].pad_value)
            data = np.hstack([pad, data])
            start = saved_md.end + 1
        else:
            start = series_md.start
        start_offset = (start - saved_md.start) * np.dtype(saved_md.dtype).itemsize
        self._set_data_range(key, start_offset, data.tostring())
        if series_md.end > saved_md.end:
            self._update_end(key, series_md.end)

    def delete_series(self, key):
        self._delete(key)

    def truncate_series(self, key, end_date):
        md = self.read_metadata(key)
        end_index = get_index(md.periodicity, end_date)
        if end_index < md.end:
            self._update_end(key, end_index)

    def read_range(self, key):
        md = self.read_metadata(key)
        return Range.from_indicies(md.start, md.end,md.periodicity)
        
    def read_frame_headers(self, key):
        return self._get_data_range(key, 0, -1).decode().split('\t')

    def read_frame_series_keys(self, key):
        return [f'{key}:{c}' for c in self.read_frame_headers(key)]

    def read_frame_data(self, key, start=None, end=None, periodicity=None, resample_method='last'):
        md = self.read_metadata(key)
        start = md.start if start is None else get_index(md.periodicity, start)
        end = md.end if end is None else get_index(md.periodicity, end)
        periodicity = md.periodicity if periodicity is None else periodicity
        columns = self.read_frame_headers(key)
        data = np.column_stack([self.read_series_data(f'{key}:{c}', start, end, periodicity,
                    resample_method)[3] for c in columns])
        return (start, end, periodicity, columns, data)

    def read_frame(self, key, start=None, end=None, periodicity=None, resample_method='last'):
        data = self.read_frame_data(key, start, end, periodicity, resample_method)
        return pd.DataFrame(data[4], columns=data[3],
                    index=Range.from_dates(*data[:3]).to_index())

    def write_frame(self, key, frame):
        end = get_index(frame.index.freq.name, frame.index[-1].date()) + 1
        try:
            md = self.read_metadata(key)
            if md.periodicity != frame.index.freq.name:
                raise Exception('Incompatible periodicity.')
            columns = set(self._get_data_range(key, 0, -1).decode().split('\t'))
            if end > md.end:
                self._update_end(key, end)
            first_save = False
        except KeyError:
            start = get_index(frame.index.freq.name, frame.index[0].date())
            md = Metadata('<U', frame.index.freq.name, start, end)
            columns = set()
            first_save = True
        new_columns = []
        for column,series in frame.iteritems():
            series_code = f'{key}:{column}'
            if not column in columns:
                columns.add(column)
                new_columns.append(column)
            series = series[series.first_valid_index():series.last_valid_index()]
            self.write_series(series_code, series)
        if first_save:
            self._write(key, md, '\t'.join(new_columns).encode()) 
        elif len(new_columns) > 0:
            self._append(key, ('\t' + '\t'.join(new_columns)).encode()) 

    def delete_frame(self, key):
        for series in self.read_frame_series_keys(key):
            self.delete_series(series)
        self._delete(key)

    def truncate_frame(self, key, end_date):
        md = self.read_metadata(key)
        end_index = get_index(md.periodicity, end_date)
        if end_index < md.end:
            self._update_end(key, end_index)

    def series(self, key):
        return _PyntoSeries('series')(self, key)

    def frame(self, key):
        return _PyntoFrame('frame')(self, key)

class _PyntoFrame(_Word):
    def __init__(self, name):
        super().__init__(name)
    def __call__(self, lima, key): return super().__call__(locals())
    def _operation(self, stack, args):
        for header in args['lima'].read_frame_headers(args['key']):
            def lima_col(row_range, key=f'{args["key"]}:{header}'):
                if row_range.range_type == 'datetime':
                    return args['lima'].read_series_data(key, row_range.start,
                                    row_range.stop, row_range.step)[3]
                else:
                    data = args['lima'].read_series_data(key)
                    values = data[3][row_range.start: row_range.stop: row_range.step]
                    row_range.start = data[0]
                    row_range.stop = data[1]
                    row_range.step = data[2]
                    row_range.range_type = 'datetime'
                    return values
            stack.append(Column(header, f'{args["key"]}:{header}', lima_col))

class _PyntoSeries(_Word):
    def __init__(self, name):
        super().__init__(name)
    def __call__(self, lima, key): return super().__call__(locals())

    def _operation(self, stack, args):
        def lima_col(row_range, key=args['key']):
            if row_range.range_type == 'datetime':
                return args['lima'].read_series_data(key, row_range.start,
                                row_range.stop, row_range.step)[3]
            else:
                data = args['lima'].read_series_data(key)
                values = data[3][row_range.start: row_range.stop: row_range.step]
                if row_range.start is None:
                    row_range.start = data[0]
                elif row_range.start < 0:
                    row_range.start = data[1] + row_range.start
                else:
                    row_range.start = data[0] + row_range.start

                if row_range.stop is None:
                    row_range.stop = data[1]
                elif row_range.stop < 0:
                    row_range.stop = data[1] + row_range.stop
                else:
                    row_range.stop = data[0] + row_range.stop
                row_range.step = data[2]
                row_range.range_type = 'datetime'
                return values
        stack.append(Column(args['key'], f'lima series:{args["key"]}', lima_col))

