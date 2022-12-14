import logging
import logging.config
import inspect


logging.config.fileConfig('logger.conf', disable_existing_loggers=False)

def logger(func):
    def wrapper(*args, **kwargs):
        _predebug(func, args, kwargs)
        try:
            value = func(*args, **kwargs)
        except Exception as e:
            _error_handler(e)
        else:
            _postdebug(func, value)
            return value
    async def awrapper(*args, **kwargs):
        _predebug(func, args, kwargs)
        try:
            value = await func(*args, **kwargs)
        except Exception as e:
            _error_handler(e)
        else:
            _postdebug(func, value)
            return value
    return awrapper if inspect.iscoroutinefunction(func) else wrapper

debug_logger = logging.getLogger('debugLogger')
def _predebug(func, args, kwargs):
    params = []
    params = ', '.join(
            [_str(arg) for arg in args]+\
            [f'{k}={_str(v)}' for k, v in kwargs.items()]
    ) if args or kwargs else ''
    debug_logger.debug(f'Enter to {func.__name__}({params})')

def _str(arg) -> str:
    try: return str(arg)
    except: return arg.__class__.__name__

def _postdebug(func, value):
    debug = f'Exit from {func.__name__}'
    debug += f' with {value}' if value else ''
    debug_logger.debug(debug)

def _error_handler(error: Exception):
    error_logger = logging.getLogger('errorLogger')
    error_logger.error('Exception', exc_info=error)


import meta
from meta import MetaTable
from table import Table
def _log_package(package):
    for name, func in inspect.getmembers(package, inspect.ismethod):
        setattr(package, name, logger(func))
[_log_package(package) for package in (meta, MetaTable, Table)]
