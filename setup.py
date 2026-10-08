#!/usr/bin/env python

from os import path

from setuptools import setup, find_packages


packages = find_packages(include=['cdx_toolkit*'])

# remember: keep requires synchronized with requirements.txt
requires = ['requests', 'warcio', 'fsspec', 'aiofiles', 'url-is-in>=0.1.1']

test_requirements = ['pytest', 'pytest-cov', 'flake8', 'responses']
# S3 reads/writes (index files, WARCs)
optional_s3_requirements = ['fsspec[s3]', 'botocore', 'aioboto3']
# `cdxt index athena://...`
optional_athena_requirements = ['boto3']
# `cdxt index cc-url-index-s3` and other parquet indexes
optional_duckdb_requirements = ['duckdb', 'pytz']  # pytz: DuckDB returns timestamps with time zone
# Hugging Face Storage Buckets (hf://): `cdxt index cc-url-index-hf`, `cdxt repackage` reads and writes
optional_hf_requirements = ['huggingface_hub>=0.30', 'aiohttp']
# Parquet output of `cdxt index` (and Parquet input of `cdxt repackage`)
optional_parquet_requirements = ['pyarrow']

package_requirements = ['twine', 'setuptools', 'setuptools-scm']

dev_requirements = ['pre-commit']

extras_require = {
    's3': optional_s3_requirements,
    'athena': optional_athena_requirements,
    'duckdb': optional_duckdb_requirements,
    'hf': optional_hf_requirements,
    'parquet': optional_parquet_requirements,
    'test': test_requirements,  # setup no longer tests, so make them an extra
    'package': package_requirements,
    'dev': package_requirements,
    'all': (
        test_requirements + package_requirements + dev_requirements
        + optional_s3_requirements + optional_athena_requirements
        + optional_duckdb_requirements + optional_hf_requirements + optional_parquet_requirements
    ),
}

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    description = f.read()

setup(
    name='cdx_toolkit',
    use_scm_version=True,
    description='A toolkit for working with CDX indices',
    long_description=description,
    long_description_content_type='text/markdown',
    author='Greg Lindahl and others',
    author_email='lindahl@pbm.com',
    url='https://github.com/cocrawler/cdx_toolkit',
    packages=packages,
    python_requires=">=3.9",
    extras_require=extras_require,
    setup_requires=['setuptools-scm'],
    install_requires=requires,
    entry_points='''
        [console_scripts]
        cdxt = cdx_toolkit.cli:main
    ''',
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Operating System :: POSIX :: Linux',
        'Environment :: MacOS X',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        # 'Programming Language :: Python :: 3.5',  # setuptools-scm problem
        # 'Programming Language :: Python :: 3.6',  # not offered in github actions
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Programming Language :: Python :: 3.14',
        'Programming Language :: Python :: 3 :: Only',
    ],
)
