import asyncio
import logging
from typing import Tuple
from os import urandom

try:
    from botocore.exceptions import ClientError, EndpointConnectionError
except ImportError:  # pragma: no cover - S3 support is optional (cdx_toolkit[s3])
    class ClientError(Exception):
        pass

    class EndpointConnectionError(Exception):
        pass


logger = logging.getLogger(__name__)


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse a S3 URI and return bucket and prefix."""
    if not uri.startswith('s3://'):
        raise ValueError(f'Not an S3 URI: {uri}')
    rest = uri[5:]
    i = rest.find('/')
    if i <= 0 or i == len(rest) - 1:
        raise ValueError(f'Malformed S3 URI: {uri}')
    return rest[:i], rest[i+1:]


def is_s3_url(url: str) -> bool:
    return url.startswith('s3:/')


async def with_retries(coro_factory, *, op_name: str, max_attempts: int, base_backoff_seconds: float):
    """Perform operation with retries and backoff."""
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await coro_factory()
        except (TimeoutError, ClientError, EndpointConnectionError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                logger.error('%s failed after %d attempts: %r', op_name, attempt, exc)
                break
            sleep_s = _backoff(attempt, base_backoff_seconds)
            logger.warning(
                '%s failed (attempt %d/%d) - retrying in %.2fs',
                op_name,
                attempt,
                max_attempts,
                sleep_s,
            )
            await asyncio.sleep(sleep_s)
    raise last_exc


def _backoff(attempt: int, base_backoff_seconds: float) -> float:
    """Time to sleep based on number of attempts"""
    base = base_backoff_seconds * (2 ** (attempt - 1))

    # Add random jitter between 80-120% of base delay
    return max(0.05, base * (0.8 + 0.4 * urandom(1)[0] / 255))
