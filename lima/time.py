import datetime
import pandas as pd
import numpy as np
from collections import namedtuple

def _round_w_fri(date):
    date = pd.to_datetime(date).date()
    return date - datetime.timedelta(days=date.weekday()) + datetime.timedelta(days=4)

Periodicity = namedtuple('Periodicity',['pandas_offset','epoque','offset_function'])

PERIODICITIES = {
    'B' : Periodicity(pd.tseries.offsets.BDay(), datetime.date(1970,1,1), lambda epoque, date: np.busday_count(epoque,pd.to_datetime(date).date())),
    'W-FRI' : Periodicity(pd.tseries.offsets.Week(weekday=4), datetime.date(1970,1,2), lambda epoque, date: (_round_w_fri(date) - epoque).days / 7)
}

def get_index(periodicity_code, date):
    p = PERIODICITIES[periodicity_code]
    return p.offset_function(p.epoque, date)

def get_date(periodicity_code, index):
    p = PERIODICITIES[periodicity_code]
    return (p.epoque + p.pandas_offset * index).date()
