from functools import wraps
from time import perf_counter

def time_excecution(func: 'function'):
    @wraps(func)
    async def wrap(*args, **kwargs):
        start = perf_counter()
        result = await func(*args, **kwargs)
        end = perf_counter()
        return result, end - start
    return wrap