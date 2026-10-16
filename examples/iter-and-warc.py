#!/usr/bin/env python
'''
Select captures from Common Crawl's CDX index and write them into a WARC file,
like `cdxt index cc --url 'commoncrawl.org/blog/*' --filter =status:200 --limit 10 | cdxt repackage ...`
'''
from cdx_toolkit.indexes import IndexQuery, open_index
from cdx_toolkit.repackage import repackage

index = open_index('cc')
query = IndexQuery(urls='commoncrawl.org/blog/*', filters='=status:200', limit=10)

captures = []
for capture in index.records(query):
    print('extracting url', capture['url'], 'timestamp', capture['timestamp'])
    captures.append(capture)

result = repackage(
    captures,
    prefix='EXAMPLE',
    subprefix='COMMONCRAWL',
    is_part_of='EXAMPLE-COMMONCRAWL',
    description='warc extraction',
)
print('wrote', result.written, 'captures to EXAMPLE-COMMONCRAWL-001.warc.gz, ...;', result.failed, 'failed')
