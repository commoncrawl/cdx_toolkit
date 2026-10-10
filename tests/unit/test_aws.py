"""Common Crawl's S3 defaults (cdx_toolkit.aws): used for the settings the user did not configure."""
import asyncio
from unittest.mock import patch

import pytest

from cdx_toolkit import aws

FULL_DEFAULTS = {
    'region_name': 'us-east-1',
    'retries': {'mode': 'adaptive', 'total_max_attempts': 100000},
    's3': {'addressing_style': 'virtual', 'payload_signing_enabled': False},
}


@pytest.fixture
def no_user_config(monkeypatch, tmp_path):
    for name in ('AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_RETRY_MODE', 'AWS_MAX_ATTEMPTS', 'CDXT_AWS_DEFAULTS',
                 'AWS_PROFILE'):
        monkeypatch.delenv(name, raising=False)
    # botocore reads the AWS config file itself: use an empty one, not the one of the machine
    monkeypatch.setenv('AWS_CONFIG_FILE', str(tmp_path / 'aws-config'))
    with patch.object(aws, 'user_aws_config', return_value={}):
        yield


def test_reads_the_aws_config_file(monkeypatch, tmp_path):
    pytest.importorskip('botocore')
    config_file = tmp_path / 'aws-config'
    config_file.write_text(
        '[default]\nmax_attempts = 5\ns3 =\n  addressing_style = path\n  max_concurrent_requests = 4\n'
    )
    for name in ('AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_RETRY_MODE', 'AWS_MAX_ATTEMPTS', 'AWS_PROFILE'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('AWS_CONFIG_FILE', str(config_file))
    aws.user_aws_config.cache_clear()
    try:
        assert aws.botocore_config_kwargs() == {
            'region_name': 'us-east-1',
            'retries': {'mode': 'adaptive'},
            's3': {'payload_signing_enabled': False},
        }
        assert aws.max_concurrent_requests() == 4
    finally:
        aws.user_aws_config.cache_clear()


def test_defaults(no_user_config):
    assert aws.botocore_config_kwargs() == FULL_DEFAULTS
    assert aws.max_concurrent_requests() == 2


def test_environment_variables_win(no_user_config, monkeypatch):
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'eu-west-1')
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '3')
    assert aws.botocore_config_kwargs() == {
        'retries': {'mode': 'adaptive'},
        's3': {'addressing_style': 'virtual', 'payload_signing_enabled': False},
    }


def test_aws_config_file_wins(no_user_config):
    user_config = {
        'retry_mode': 'standard', 'max_attempts': '10',
        's3': {'addressing_style': 'path', 'max_concurrent_requests': '8'},
    }
    with patch.object(aws, 'user_aws_config', return_value=user_config):
        assert aws.botocore_config_kwargs() == {'region_name': 'us-east-1', 's3': {'payload_signing_enabled': False}}
        assert aws.max_concurrent_requests() == 8


def test_defaults_can_be_disabled(no_user_config, monkeypatch):
    monkeypatch.setenv('CDXT_AWS_DEFAULTS', '0')
    assert aws.botocore_config_kwargs() == {}


def test_botocore_resolves_the_defaults_and_the_user_settings(no_user_config, monkeypatch):
    boto3 = pytest.importorskip('boto3')

    def client(**config):
        # a new session reads the environment and the (empty) AWS config file
        session = boto3.session.Session(aws_access_key_id='x', aws_secret_access_key='x')
        return session.client('s3', config=aws.botocore_config(**config))

    s3 = client()
    assert s3.meta.region_name == 'us-east-1'
    assert s3.meta.config.retries == {'mode': 'adaptive', 'total_max_attempts': 100000}
    assert s3.meta.config.s3 == {'addressing_style': 'virtual', 'payload_signing_enabled': False}

    # a value from the environment is not overridden: botocore reads it itself
    monkeypatch.setenv('AWS_MAX_ATTEMPTS', '3')
    assert client().meta.config.retries == {'mode': 'adaptive', 'total_max_attempts': 3}

    # an explicit region wins
    assert client(region_name='eu-central-1').meta.region_name == 'eu-central-1'


def test_configure_fsspec_s3_keeps_fsspec_settings(no_user_config, monkeypatch):
    import fsspec.config

    user_conf = {'s3': {'config_kwargs': {'retries': {'mode': 'standard'}}, 'anon': False}}
    monkeypatch.setattr(fsspec.config, 'conf', user_conf)
    aws.configure_fsspec_s3()
    assert fsspec.config.conf['s3'] == {
        'anon': False,
        'max_concurrency': 2,
        'config_kwargs': {
            'region_name': 'us-east-1',
            'retries': {'mode': 'standard', 'total_max_attempts': 100000},
            's3': {'addressing_style': 'virtual', 'payload_signing_enabled': False},
        },
    }

    monkeypatch.setattr(fsspec.config, 'conf', {})
    monkeypatch.setenv('CDXT_AWS_DEFAULTS', 'off')
    aws.configure_fsspec_s3()
    assert fsspec.config.conf == {}


def test_s3fs_uses_the_defaults(no_user_config, monkeypatch):
    s3fs = pytest.importorskip('s3fs')
    import fsspec.config

    monkeypatch.setattr(fsspec.config, 'conf', {})
    aws.configure_fsspec_s3()
    fs = s3fs.S3FileSystem(skip_instance_cache=True, key='x', secret='x')
    assert fs.config_kwargs['retries'] == {'mode': 'adaptive', 'total_max_attempts': 100000}
    assert fs.max_concurrency == 2


def test_warc_fetcher_s3_clients(no_user_config):
    pytest.importorskip('aioboto3')
    from cdx_toolkit.repackage.sources.records import RecordsSource
    from cdx_toolkit.repackage.warc_filter import WARCFilter

    wf = WARCFilter(source=RecordsSource([], None), prefix_path='out', writer_info={},
                    warc_download_prefix='s3://commoncrawl', n_parallel_readers=4)
    with patch('aioboto3.Session') as session:
        asyncio.run(wf.get_aws_clients())
    read_config, write_config = (c.kwargs['config'] for c in session.return_value.client.call_args_list)
    for config in (read_config, write_config):
        assert config.region_name == 'us-east-1'
        assert config.retries == {'mode': 'adaptive', 'total_max_attempts': 100000}
        assert config.s3 == {'addressing_style': 'virtual', 'payload_signing_enabled': False}
    assert read_config.max_pool_connections == 12 and read_config.read_timeout == 300


def test_cli_configures_fsspec(no_user_config, monkeypatch, tmp_path):
    import fsspec.config
    from cdx_toolkit.cli import main

    monkeypatch.setattr(fsspec.config, 'conf', {})
    captures = tmp_path / 'captures.jsonl'
    captures.write_text('')
    main(['repackage', str(captures), '--prefix', str(tmp_path / 'out')])
    assert fsspec.config.conf['s3']['config_kwargs']['retries']['total_max_attempts'] == 100000
