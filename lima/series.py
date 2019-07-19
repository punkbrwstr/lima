from lima.base import *
from lima.time import *
import pandas as pd
import numpy as np

__all__ = ['read_series','write_series', 'delete_series', 'read_series_metadata']


def read_series(key, start=None, end=None, periodicity=None,
                        resample_method='last', as_series=True):
    series_key = f'{SERIES_PREFIX}.{key}'
    md = read_metadata(series_key)
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
        buff = get_data_range(series_key, selected_start * itemsize, selected_end * itemsize)
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
    s = pd.Series(output, index=get_date_range(md.periodicity,start_index,end_index), name=key)
    if needs_resample:
        s = getattr(s.ffill().resample(periodicity),resample_method)().reindex(pd.date_range(start,end, freq=periodicity))       
        if not as_series:
            return (get_index(periodicity,s.index[0]), get_index(periodicity,s.index[-1]), periodicity, s.values)
    return s

def write_series(key, series):
    series_key = f'{SERIES_PREFIX}.{key}'
    series_md = get_metadata_for_series(series)
    saved_md = read_metadata(series_key)
    if saved_md and saved_md.periodicity != series_md.periodicity:
        raise Exception(f'Incompatible periodicity.')   
    data = series.values
    if saved_md is None or series_md.start < saved_md.start:
        write(series_key, series_md, data.tostring())
        return
    if series_md.start > saved_md.end:
        pad = np.full(series_md.start - saved_md.end, TYPES[saved_md.dtype].pad_value)
        data = np.hstack([pad, np.full])
        start = saved_md.end + 1
    else:
        start = series_md.start
    start_offset = (start - saved_md.start) * np.dtype(saved_md.dtype).itemsize
    set_data_range(series_key, start_offset, data.tostring())
    if series_md.end > saved_md.end:
        update_end(series_key, series_md.end)

def delete_series(key):
    delete(f'{SERIES_PREFIX}.{key}')

def read_series_metadata(key, date_range=None):
    series_key = f'{SERIES_PREFIX}.{key}'
    return read_metadata(series_key)
