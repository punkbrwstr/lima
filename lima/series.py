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
    start = md.start if start is None else get_index(md.periodicity, start)
    end = md.end if end is None else get_index(md.periodicity, end)
    needs_resample = not (periodicity is None or periodicity == md.periodicity)
    if start <= md.end and end >= md.start:
        itemsize = np.dtype(md.dtype).itemsize 
        selected_start = max(0, start - md.start)
        selected_end = min(-1, (end - md.end + 1) * itemsize - 1)
        buff = get_data_range(series_key, selected_start * itemsize, selected_end)
        data = np.frombuffer(buff, md.dtype)
        if len(data) != end - start + 1:
            output_start = max(0, md.start - start)
            output = np.full(end - start + 1,TYPES[md.dtype].pad_value)
            output[output_start:output_start+len(data)] = data
        else:
            output = data
    else:
        output = np.full(end - start + 1,TYPES[md.dtype].pad_value)
    if not (as_series or needs_resample):
        return (start, end, md.periodicity, output)
    s = pd.Series(output, index=get_date_range(md.periodicity,start,end), name=key)
    if needs_resample:
        s = getattr(s.ffill().resample(periodicity),resample_method)()       
        #if len(s) != len(date_range):
            #s = s.reindex(date_range)
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
        pad = np.full(metadata.start - saved_md.end, TYPES[saved_md.dtype].pad_value)
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
