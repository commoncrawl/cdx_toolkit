'''
Defaults for cdxt's requests to S3, as recommended by Common Crawl for downloading from s3://commoncrawl:

    region = us-east-1
    retry_mode = adaptive
    max_attempts = 100000
    s3 =
      addressing_style = virtual
      payload_signing_enabled = false
      max_concurrent_requests = 2
      (multipart_threshold = multipart_chunksize = 10GB: no multipart transfers)

The user stays in control: a default is only used for a setting that is not configured in the
environment (AWS_REGION / AWS_DEFAULT_REGION, AWS_RETRY_MODE, AWS_MAX_ATTEMPTS), in the AWS config
file of the active profile (~/.aws/config, AWS_PROFILE) or explicitly (e.g. the retries in the fsspec
configuration). CDXT_AWS_DEFAULTS=0 disables all defaults. The region is, in this order: an explicit
region, AWS_REGION (which botocore itself ignores), AWS_DEFAULT_REGION, the profile, the default.

The multipart settings are for whole-file transfers (`aws s3 cp`): cdxt reads WARC records with one
range request each and reads index files sequentially, so it makes no multipart downloads anyway.

The same settings are used for the Athena client and, as far as it has equivalents, for DuckDB.
'''
import functools
import logging
import os
import re
from typing import List, Optional


LOGGER = logging.getLogger(__name__)

DEFAULT_REGION = 'us-east-1'
DEFAULT_RETRY_MODE = 'adaptive'
DEFAULT_MAX_ATTEMPTS = 100000  # total attempts, as `max_attempts` in the AWS config file
DEFAULT_S3 = {'addressing_style': 'virtual', 'payload_signing_enabled': False}
DEFAULT_MAX_CONCURRENT_REQUESTS = 2

# environment variables that configure a setting (botocore reads them itself, except AWS_REGION)
SETTING_ENV_VARS = {
    'region': ('AWS_REGION', 'AWS_DEFAULT_REGION'),
    'retry_mode': ('AWS_RETRY_MODE',),
    'max_attempts': ('AWS_MAX_ATTEMPTS',),
}


def defaults_enabled() -> bool:
    return os.environ.get('CDXT_AWS_DEFAULTS', '1').strip().lower() not in ('0', 'false', 'no', 'off')


@functools.lru_cache(maxsize=None)
def user_aws_config() -> dict:
    """The AWS config file settings of the active profile ({} without botocore or a config file)."""
    try:
        import botocore.session

        return dict(botocore.session.Session().get_scoped_config())
    except Exception as e:  # botocore missing, unknown profile, unreadable config file
        LOGGER.debug('no AWS config file settings: %r', e)
        return {}


def user_configured(setting: str) -> bool:
    if any(os.environ.get(name) for name in SETTING_ENV_VARS.get(setting, ())):
        return True
    return setting in user_aws_config()


def user_s3_setting(name: str):
    s3 = user_aws_config().get('s3')
    return s3.get(name) if isinstance(s3, dict) else None


# the retry settings that bound the number of attempts (botocore prefers total_max_attempts)
RETRY_BOUNDS = ('max_attempts', 'total_max_attempts')


def botocore_config_kwargs() -> dict:
    """kwargs for botocore.config.Config with the defaults for the settings the user did not configure.

    botocore merges the `retries` and `s3` settings key by key with the environment and the AWS
    config file, so the settings the user configured keep their values."""
    kwargs = {}
    if os.environ.get('AWS_REGION'):
        kwargs['region_name'] = os.environ['AWS_REGION']  # botocore only reads AWS_DEFAULT_REGION
    if not defaults_enabled():
        return kwargs
    if not user_configured('region'):
        kwargs['region_name'] = DEFAULT_REGION
    retries = {}
    if not user_configured('retry_mode'):
        retries['mode'] = DEFAULT_RETRY_MODE
    if not user_configured('max_attempts'):
        retries['total_max_attempts'] = DEFAULT_MAX_ATTEMPTS
    if retries:
        kwargs['retries'] = retries
    s3 = {name: value for name, value in DEFAULT_S3.items() if user_s3_setting(name) is None}
    if s3:
        kwargs['s3'] = s3
    return kwargs


def merge_config_kwargs(defaults: dict, explicit: dict) -> dict:
    """Explicit botocore config kwargs win over the defaults; the `retries` and `s3` dicts are merged key by key,
    and an explicit bound of the attempts (max_attempts or total_max_attempts) replaces the default one."""
    merged = dict(defaults)
    for key, value in explicit.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            default = dict(merged[key])
            if key == 'retries' and any(bound in value for bound in RETRY_BOUNDS):
                for bound in RETRY_BOUNDS:
                    default.pop(bound, None)
            merged[key] = {**default, **value}
        else:
            merged[key] = value
    return merged


def botocore_config(region_name: Optional[str] = None, **kwargs):
    """A botocore Config with the defaults, plus the given options, e.g. timeouts (they win over the defaults).

    An explicit `region_name` wins over the default and the user's configuration."""
    from botocore.config import Config

    config_kwargs = merge_config_kwargs(botocore_config_kwargs(), kwargs)
    if region_name:
        config_kwargs['region_name'] = region_name
    return Config(**config_kwargs)


def max_concurrent_requests() -> int:
    configured = user_s3_setting('max_concurrent_requests')
    if configured is not None:
        return int(configured)
    return DEFAULT_MAX_CONCURRENT_REQUESTS


def configure_fsspec_s3() -> None:
    """Use the defaults for s3:// paths opened with fsspec (s3fs), without changing options that are already
    set in the fsspec configuration (~/.config/fsspec/, FSSPEC_S3_* environment variables)."""
    if not defaults_enabled():
        return
    import fsspec.config

    conf = fsspec.config.conf.setdefault('s3', {})
    config_kwargs = merge_config_kwargs(botocore_config_kwargs(), dict(conf.get('config_kwargs') or {}))
    if config_kwargs:
        conf['config_kwargs'] = config_kwargs
    conf.setdefault('max_concurrency', max_concurrent_requests())


def process_pool(max_workers: int, mp_context=None, initializer=None, initargs=()):
    """A ProcessPoolExecutor whose processes use the S3 defaults of configure_fsspec_s3() like this one: a process
    started with spawn (macOS, Windows) does not inherit the changes of fsspec.config.conf. Settings from
    files and environment variables apply in every process; other in-memory changes of the configuration
    are not passed on."""
    from concurrent.futures import ProcessPoolExecutor

    return ProcessPoolExecutor(
        max_workers=max_workers, mp_context=mp_context, initializer=_init_process, initargs=(initializer, initargs)
    )


def _init_process(initializer, initargs) -> None:
    configure_fsspec_s3()
    if initializer is not None:
        initializer(*initargs)


def _user_value(setting: str) -> Optional[str]:
    for name in SETTING_ENV_VARS.get(setting, ()):
        if os.environ.get(name):
            return os.environ[name]
    value = user_aws_config().get(setting)
    return str(value) if value is not None else None


def duckdb_settings(region_name: Optional[str] = None) -> List[str]:
    """SET statements with the defaults for DuckDB's S3 access (httpfs), as far as DuckDB has equivalents.

    Settings the user configured (environment, AWS config file) are used instead of the defaults:
    - region -> s3_region (an explicit `region_name` wins)
    - max_attempts -> http_retries (attempts - 1), with a constant wait of 1s between retries, because
      DuckDB's exponential backoff has no maximum wait
    - s3 addressing_style -> s3_url_style (virtual: vhost, path: path)
    There is no equivalent of retry_mode (DuckDB has no adaptive mode) and max_concurrent_requests
    (only `threads`, which limits all of DuckDB's parallelism)."""
    if not defaults_enabled():
        region = region_name or _user_value('region')
        return [f"SET s3_region='{region}';"] if region and re.fullmatch(r'[a-z0-9-]+', region) else []
    region = region_name or _user_value('region') or DEFAULT_REGION
    if not re.fullmatch(r'[a-z0-9-]+', region):
        raise ValueError(f'invalid AWS region {region!r}')
    attempts = _user_value('max_attempts')
    attempts = int(attempts) if attempts and attempts.isdigit() else DEFAULT_MAX_ATTEMPTS
    url_style = 'path' if (user_s3_setting('addressing_style') or DEFAULT_S3['addressing_style']) == 'path' else 'vhost'
    return [
        f"SET s3_region='{region}';",
        f"SET s3_url_style='{url_style}';",
        f'SET http_retries={max(attempts - 1, 0)};',
        'SET http_retry_wait_ms=1000;',
        'SET http_retry_backoff=1;',
    ]


def aws_region(region_name: Optional[str] = None) -> str:
    """The region for AWS clients that need a concrete value (explicit, the user's, or the default)."""
    region = region_name or _user_value('region') or DEFAULT_REGION
    if not re.fullmatch(r'[a-z0-9-]+', region):
        raise ValueError(f'invalid AWS region {region!r}')
    return region
