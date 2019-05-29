import redis
import pandas as pd
from lima.time import *
from lima.config import *
from lima.series import *

__all__ = ['read_frame','write_frame', 'delete_frame']

def read_frame(key, date_range=None):
    frame_key = f'{FRAME_PREFIX}.{key}'
    r = redis.Redis(connection_pool=REDIS_POOL)
    frame = r.hgetall(frame_key)
    if frame is None or len(frame) == 0:
        return None
    per = frame[b'periodicity'].decode()
    if date_range is None:
        date_range =  pd.date_range(get_date(per, int(frame[b'start'])), get_date(per, int(frame[b'end'])), freq=per)
    else:
        if per == date_range.freq.name:
            raise Exception('Requested periodicity "{date_range.freq.name}" does not match saved data "{per}"') 
    columns = frame[b'columns'].decode().split('\t')
    series = frame[b'series_codes'].decode().split('\t')
    return pd.DataFrame({ c: read_series(s, date_range, nas_if_missing=True) for c,s in zip(columns,series)})

def write_frame(key, frame):
    if frame.index.freq is None:
        raise Exception('Missing index freq.')
    per = frame.index.freq.name
    r = redis.Redis(connection_pool=REDIS_DECODED_POOL)
    frame_key = f'{FRAME_PREFIX}.{key}'
    prev_frame = r.hgetall(frame_key)
    end = get_index(per, frame.index[-1])
    if len(prev_frame) > 0:
        if prev_frame['periodicity'] != per:
            raise Exception('Incompatible periodicity.')
        #if end <= int(prev_frame['end']):
        #    return
        start = int(prev_frame['start'])
        columns = prev_frame['columns'].split('\t')
        series_codes = prev_frame['series_codes'].split('\t')
    else:
        start = get_index(per, frame.index[0])
        columns,series_codes = [], []
    frame_dict = {'periodicity': per, 'start': str(start), 'end': str(end) }
    for column,series in frame.iteritems():
    #    if not np.all(np.isnan(series.values)):
        series_code = f'{key}.{column}'
        if not column in columns:
            columns.append(column)
            series_codes.append(series_code)
        series = series[series.first_valid_index():series.last_valid_index()]
        write_series(series_code, series)
    frame_dict['columns'] = '\t'.join(columns)
    frame_dict['series_codes'] = '\t'.join(series_codes)
    r.hmset(frame_key, frame_dict)

def delete_frame(key):
    prev_frame = read_frame(key)
    if not prev_frame is None:
        r = redis.Redis(connection_pool=REDIS_POOL)
        r.delete(f'{FRAME_PREFIX}.{key}')
        for column in prev_frame.columns:
            delete_series(f'{key}.{column}')
