import redis
import pandas as pd
import numpy as np
from lima.base import *
from lima.series import *

__all__ = ['read_frame','write_frame', 'delete_frame', 'read_frame_metadata',
        'read_frame_headers','read_frame_series_keys']

def read_frame_headers(key):
    frame_key = f'{FRAME_PREFIX}.{key}'
    return getrange(frame_key, 0, -1).decode().split('\t')

def read_frame_series_keys(key):
    return [f'{key}.{c}' for c in read_frame_headers(key)]

def read_frame(key, date_range=None):
    frame_key = f'{FRAME_PREFIX}.{key}'
    md = read_metadata(frame_key)
    if md is None:
        return None
    if date_range is None:
        date_range =  pd.date_range(get_date(md.periodicity_code, md.start_index),
                                        periods=md.end_index - md.start_index + 1, freq=md.periodicity_code)
    else:
        if md.periodicity_code != date_range.freq.name:
            raise Exception('Requested periodicity "{date_range.freq.name}" does not match saved data "{md.periodicity_code}"') 
    columns = read_frame_headers(key)
    return pd.DataFrame({ c: read_series(f'{key}.{c}', date_range) for c in columns})

def write_frame(key, frame):
    frame_key = f'{FRAME_PREFIX}.{key}'
    md = read_metadata(frame_key)
    end = get_index(frame.index.freq.name, frame.index[-1])
    if md is None:
        start = get_index(frame.index.freq.name, frame.index[0])
        md = Metadata(np.dtype('str'), frame.index.freq.name, start, end)
        columns = set()
        first_save = True
    else:
        if md.periodicity_code != frame.index.freq.name:
            raise Exception('Incompatible periodicity.')
        columns = set(getrange(frame_key, 0, -1).decode().split('\t'))
        if end > md.end_index:
            update_metadata_end_index(frame_key, end)
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
    prev_frame = read_frame(key)
    if not prev_frame is None:
        r = redis.Redis(connection_pool=REDIS_POOL)
        r.delete(f'{FRAME_PREFIX}.{key}')
        for column in prev_frame.columns:
            delete_series(f'{key}.{column}')

def read_frame_metadata(key):
    frame_key = f'{FRAME_PREFIX}.{key}'
    return read_metadata(frame_key)
