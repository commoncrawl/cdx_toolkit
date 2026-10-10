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
environment (AWS_REGION / AWS_DEFAULT_REGION, AWS_RETRY_MODE, AWS_MAX_ATTEMPTS) or in the AWS config
file of the active profile (~/.aws/config, AWS_PROFILE). CDXT_AWS_DEFAULTS=0 disables all defaults.

The multipart settings are for whole-file transfers (`aws s3 cp`): cdxt reads WARC records with one
range request each and reads index files sequentially, so it makes no multipart downloads anyway.
'''
import functools
import logging
import os
from typing import Optional


LOGGER = logging.getLogger(__name__)

DEFAULT_REGION = 'us-east-1'
DEFAULT_RETRY_MODE = 'adaptive'
DEFAULT_MAX_ATTEMPTS = 100000  # total attempts, as `max_attempts` in the AWS config file
DEFAULT_S3 = {'addressing_style': 'virtual', 'payload_signing_enabled': False}
DEFAULT_MAX_CONCURRENT_REQUESTS = 2

# environment variables that configure a setting (botocore reads them itself)
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


def botocore_config_kwargs() -> dict:
    """kwargs for botocore.config.Config with the defaults for the settings the user did not configure.

    botocore merges the `retries` and `s3` settings key by key with the environment and the AWS
    config file, so the settings the user configured keep their values."""
    if not defaults_enabled():
        return {}
    kwargs = {}
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


def botocore_config(region_name: Optional[str] = None, **kwargs):
    """A botocore Config with the defaults, plus the given (non-default) options, e.g. timeouts.

    An explicit `region_name` wins over the default and the user's configuration."""
    from botocore.config import Config

    config_kwargs = botocore_config_kwargs()
    if region_name:
        config_kwargs['region_name'] = region_name
    return Config(**config_kwargs, **kwargs)


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
    config_kwargs = dict(conf.get('config_kwargs') or {})
    for key, value in botocore_config_kwargs().items():
        if isinstance(value, dict) and isinstance(config_kwargs.get(key), dict):
            config_kwargs[key] = {**value, **config_kwargs[key]}
        else:
            config_kwargs.setdefault(key, value)
    if config_kwargs:
        conf['config_kwargs'] = config_kwargs
    conf.setdefault('max_concurrency', max_concurrent_requests())
