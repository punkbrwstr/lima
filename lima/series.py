from lima.base import *
import pandas as pd
import numpy as np

__all__ = ['read_series','write_series', 'delete_series', 'read_series_metadata']

_PAD_VALUE = {'d': np.nan, '?': False}

def read_series(key, date_range=None, resample_method='last'):
    series_key = f'{SERIES_PREFIX}.{key}'
    saved_md = read_metadata(series_key)
    if saved_md is None:
        raise Exception(f'No series data for key: "{key}".')
    needs_resample = False
    if date_range is None:
        saved_date_range = metadata_to_date_range(saved_md)
        start_index = saved_md.start_index
        end_index = saved_md.end_index
    else:
        if saved_md.periodicity_code == date_range.freq.name:
            saved_date_range = date_range
        else:
            needs_resample = True
            saved_date_range = pd.date_range(date_range[0], date_range[-1], freq=saved_md.periodicity_code)
        start_index = get_index(saved_md.periodicity_code, saved_date_range[0])
        end_index = get_index(saved_md.periodicity_code, saved_date_range[-1])
    if not (start_index > saved_md.end_index or end_index < saved_md.start_index):
        selected_start = max(0, start_index - saved_md.start_index)
        selected_end = min(-1, (end_index - saved_md.end_index + 1) * saved_md.dtype.itemsize - 1)
        buff = getrange(series_key, selected_start * saved_md.dtype.itemsize, selected_end)
        data = np.frombuffer(buff, saved_md.dtype)
        output_start = max(0, saved_md.start_index - start_index)
    else:
        data = None
    if len(data) == len(saved_date_range):
        output = data
    else:
        output = np.full(len(saved_date_range),_PAD_VALUE[saved_md.dtype.char])
        if not data is None:
            output[output_start:output_start+len(data)] = data
    s = pd.Series(output, index=saved_date_range, name=key)
    if needs_resample:
        return getattr(s.ffill().resample(date_range.freq.name),resample_method)()       
    return s

def write_series(key, series):
    series_key = f'{SERIES_PREFIX}.{key}'
    metadata = get_metadata_for_series(series)
    saved_md = read_metadata(series_key)
    if saved_md is None:
        write(series_key, metadata, series.values.tostring())
    else:
        if saved_md.periodicity_code != metadata.periodicity_code:
            raise Exception(f'Incompatible periodicity.')   
        if metadata.end_index < saved_md.end_index:
            return
        elif metadata.start_index < saved_md.end_index:
            append(series_key, series.values[saved_md.end_index - metadata.start_index:].tostring())
        else:
            if metadata.start_index > saved_md.end_index:
                append(series_key, np.full(metadata.start_index - saved_md.end_index, _PAD_VALUE[saved_md.dtype.char]).tostring())
            append(series_key, series.values.tostring())
        if metadata.end_index > saved_md.end_index:
            update_metadata_end_index(series_key, metadata.end_index)

def delete_series(key):
    delete(f'{SERIES_PREFIX}.{key}')

def read_series_metadata(key, date_range=None):
    series_key = f'{SERIES_PREFIX}.{key}'
    return read_metadata(series_key)
