import redis
import pandas as pd
import numpy as np
from lima.base import *
from lima.series import *

__all__ = ['read_frame','write_frame', 'delete_frame', 'read_frame_metadata',
        'read_frame_headers','read_frame_series_keys']

def read_frame_headers(key):
    frame_key = f'{FRAME_PREFIX}.{key}'
    return get_data_range(frame_key, 0, -1).decode().split('\t')

def read_frame_series_keys(key):
    return [f'{key}.{c}' for c in read_frame_headers(key)]

def read_frame(key, start=None, end=None, periodicity=None,
                        resample_method='last', as_frame=True):
    frame_key = f'{FRAME_PREFIX}.{key}'
    md = read_metadata(frame_key)
    if md is None:
        raise KeyError(f'No frame data for key: "{key}".')
    start = md.start if start is None else get_index(md.periodicity, start)
    end = md.end if end is None else get_index(md.periodicity, end)
    periodicity = md.periodicity if periodicity is None else periodicity
    columns = read_frame_headers(key)
    print(columns)
    data = np.column_stack([read_series(f'{key}.{c}', start, end, periodicity,
                resample_method, as_series=False)[3] for c in columns])
    if not as_frame:
        return data
    return pd.DataFrame(data, columns=columns,
                index=get_date_range(periodicity, start, end))

def write_frame(key, frame):
    frame_key = f'{FRAME_PREFIX}.{key}'
    md = read_metadata(frame_key)
    end = get_index(frame.index.freq.name, frame.index[-1])
    if md is None:
        start = get_index(frame.index.freq.name, frame.index[0])
        md = Metadata('<U', frame.index.freq.name, start, end)
        columns = set()
        first_save = True
    else:
        if md.periodicity != frame.index.freq.name:
            raise Exception('Incompatible periodicity.')
        columns = set(get_data_range(frame_key, 0, -1).decode().split('\t'))
        if end > md.end:
            update_end(frame_key, end)
        first_save = False
    new_columns = []
    for column,series in frame.iteritems():
        series_code = f'{key}.{column}'
        if not column in columns:
            columns.add(column)
            new_columns.append(column)
        series = series[series.first_valid_index():series.last_valid_index()]
        write_series(series_code, series)
    if first_save:
        write(frame_key, md, '\t'.join(new_columns).encode()) 
    elif len(new_columns) > 0:
        append(frame_key, ('\t' + '\t'.join(new_columns)).encode()) 

def delete_frame(key):
    for series in read_frame_series_keys(key):
        delete_series(series)
    delete(f'{FRAME_PREFIX}.{key}')




def read_frame_metadata(key):
    frame_key = f'{FRAME_PREFIX}.{key}'
    return read_metadata(frame_key)
