import redis
import pandas as pd
import lima as lm

__all__ = ['read_frame','write_frame', 'delete_frame']

def read_frame(key, date_range=None):
    frame_key = f'{lm.FRAME_PREFIX}.{key}'
    r = redis.Redis(connection_pool=lm.REDIS_POOL)
    frame = r.hgetall(frame_key)
    if frame is None or len(frame) == 0:
        return None
    per = lm.get_periodicity(frame[b'periodicity'].decode())
    if date_range is None:
        date_range =  pd.date_range(per.get_date(int(frame[b'start'])), per.get_date(int(frame[b'end'])), freq=per.to_pandas())
    else:
        if per.get_name() == date_range.freq.name:
            raise Exception('Requested periodicity "{date_range.freq.name}" does not match saved data "{per.get_name()}"') 
    columns = frame[b'columns'].decode().split('\t')
    series = frame[b'series_codes'].decode().split('\t')
    return pd.DataFrame({ c: lm.read_series(s, date_range,nas_if_missing=True) for c,s in zip(columns,series)})

def write_frame(key, frame):
    if frame.index.freq is None:
        raise Exception('Missing index freq.')
    per = lm.get_periodicity(frame.index.freq.name)
    r = redis.Redis(connection_pool=lm.REDIS_DECODED_POOL)
    frame_key = f'{lm.FRAME_PREFIX}.{key}'
    prev_frame = r.hgetall(frame_key)
    end = per.get_index(frame.index[-1])
    if len(prev_frame) > 0:
        if prev_frame['periodicity'] != per.get_name():
            raise Exception('Incompatible periodicity.')
        #if end <= int(prev_frame['end']):
        #    return
        start = int(prev_frame['start'])
        columns = prev_frame['columns'].split('\t')
        series_codes = prev_frame['series_codes'].split('\t')
    else:
        start = per.get_index(frame.index[0])
        columns,series_codes = [], []
    frame_dict = {'periodicity': per.get_name(), 'start': str(start), 'end': str(end) }
    for column,series in frame.iteritems():
    #    if not np.all(np.isnan(series.values)):
        series_code = f'{key}.{column}'
        if not column in columns:
            columns.append(column)
            series_codes.append(series_code)
        series = series[series.first_valid_index():series.last_valid_index()]
        lm.write_series(series_code, series)
    frame_dict['columns'] = '\t'.join(columns)
    frame_dict['series_codes'] = '\t'.join(series_codes)
    r.hmset(frame_key, frame_dict)

def delete_frame(key):
    prev_frame = read_frame(key)
    if not prev_frame is None:
        r = redis.Redis(connection_pool=lm.REDIS_POOL)
        r.delete(f'{lm.FRAME_PREFIX}.{key}')
        for column in prev_frame.columns:
            delete_series(f'{key}.{column}')
