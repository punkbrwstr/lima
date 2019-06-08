from lima.base import *

__all__ = ['read_hash','read_hash_item','read_hash_items','write_hash','write_hash_item','delete_hash']


def read_hash_item(key, item):
    value = hget(f'{HASH_PREFIX}.{key}', item) 
    return value.decode() if not value is None else None

def read_hash_items(key, items):
    values = hmget(f'{HASH_PREFIX}.{key}', items) 
    return [v.decode() if not v is None else None for v in values ]

def read_hash(key):
    values = hgetall(f'{HASH_PREFIX}.{key}')
    return {k.decode(): v.decode() for k,v in values.items()} if len(values) > 0 else None
    
def write_hash(key, items):
    hmset(f'{HASH_PREFIX}.{key}', items)

def write_hash_item(key, item, value):
    hset(f'{HASH_PREFIX}.{key}', item, value)

def delete_hash(key):
    delete(f'{HASH_PREFIX}.{key}')
