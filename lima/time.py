import pandas as pd
import numpy as np
from datetime import datetime, timedelta

class Periodicity:
    def __init__(self, pandas_offset, bloomberg_code, epoque, offset_function):
        self.pandas_offset = pandas_offset
        self.bloomberg_code = bloomberg_code
        self.epoque = epoque
        self.offset_function = offset_function

    def get_name(self):
        return self.pandas_offset.name

    def to_pandas(self):
        return self.pandas_offset

    def get_index(self, date):
        return self.offset_function(self.epoque, date)

    def get_date(self, index):
        return (self.epoque + self.pandas_offset * index).date()

    def get_bloomberg_code(self):
        return self.bloomberg_code

def _round_w_fri(date):
    date = pd.to_datetime(date).date()
    return date - timedelta(days=date.weekday()) + timedelta(days=4)

_PERIODICITIES = {
    'B': Periodicity(pandas_offset=pd.tseries.offsets.BDay(),
            bloomberg_code='DAILY',
            epoque=pd.to_datetime('1970-01-01').date(),
            offset_function= lambda epoque, date: np.busday_count(epoque,pd.to_datetime(date).date())),
    'W-FRI': Periodicity(pandas_offset=pd.tseries.offsets.BDay(),
            bloomberg_code='WEEKLY',
            epoque=pd.to_datetime('1970-01-02').date(),
            offset_function= lambda epoque, date: (_round_w_fri(date) - epoque).days / 7)
}

def get_periodicity(code):
    return _PERIODICITIES[code]
