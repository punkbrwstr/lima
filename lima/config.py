import redis
import os
import base64
import hashlib
from redis.connection import UnixDomainSocketConnection


_ENV_VARS = {
    'host': 'LIMA_REDIS_HOST',
    'port': 'LIMA_REDIS_PORT',
    'db': 'LIMA_REDIS_DB',
    'password': 'LIMA_REDIS_PASSWORD',
    'path': 'LIMA_REDIS_SOCKET_PATH'
}

_ARGS = {name:os.environ.get(var, None) for name, var in _ENV_VARS.items() if os.environ.get(var, None)}

print(_ARGS)

if 'path' in _ARGS:
    _ARGS.pop('host', None)
    _ARGS.pop('port', None)
    REDIS_POOL = redis.ConnectionPool(connection_class=UnixDomainSocketConnection, **_ARGS)
    REDIS_DECODED_POOL = redis.ConnectionPool(connection_class=UnixDomainSocketConnection, decode_responses=True, **_ARGS)
else:
    REDIS_POOL = redis.ConnectionPool(**_ARGS)
    REDIS_DECODED_POOL = redis.ConnectionPool(decode_responses=True, **_ARGS)

SERIES_PREFIX = 'lm.s'
FRAME_PREFIX = 'lm.f'
TABLE_PREFIX = 'lm.t'

