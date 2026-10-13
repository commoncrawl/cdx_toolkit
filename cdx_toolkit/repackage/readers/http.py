'''
Range reads over HTTP(S).
'''
import asyncio
import re
import threading
from typing import Optional


class RangeError(OSError):
    """A range request returned other bytes than requested."""


def check_range_response(status: int, content_range: Optional[str], body_length: int, offset: int, length: int,
                         url: str) -> None:
    """Raise RangeError unless an HTTP response holds exactly the bytes offset .. offset+length-1.

    A 206 response must name the requested range in Content-Range. A 200 response (the server ignored the
    range) is only accepted for a range from offset 0 that is the whole file. Redirects are not followed:
    a URL that redirects must be given as the redirect target."""
    end = offset + length - 1
    if status == 206:
        match = re.fullmatch(r'bytes (\d+)-(\d+)/(\d+|\*)', (content_range or '').strip())
        if not match or (int(match.group(1)), int(match.group(2))) != (offset, end):
            raise RangeError(f'{url}: requested bytes {offset}-{end}, got Content-Range {content_range!r}')
    elif status == 200:
        if offset != 0:
            raise RangeError(f'{url}: the server ignored the range request for bytes {offset}-{end}')
    else:
        raise RangeError(f'{url}: unexpected status {status} for the range request for bytes {offset}-{end}')
    if body_length != length:
        raise RangeError(f'{url}: requested {length} bytes from offset {offset}, got {body_length}')


async def run_in_daemon_thread(func, *args, **kwargs):
    """Run a blocking function in a daemon thread of its own (like asyncio.to_thread).

    Unlike asyncio.to_thread, nothing waits for a cancelled call: not the cancelled task, not asyncio.run() (which
    waits for the threads of the default executor) and not the interpreter at exit."""
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def set_result(result, exception):
        if not future.cancelled():
            if exception is not None:
                future.set_exception(exception)
            else:
                future.set_result(result)

    def run():
        try:
            result, exception = func(*args, **kwargs), None
        except BaseException as e:
            result, exception = None, e
        try:
            loop.call_soon_threadsafe(set_result, result, exception)
        except RuntimeError:
            pass  # the event loop is closed: nobody waits for the result anymore

    threading.Thread(target=run, daemon=True).start()
    return await future
