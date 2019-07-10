#!/usr/bin/env python3

import sys
import re
import zstandard as zstd
import time
from datetime import datetime
from pathlib import Path

if len(sys.argv) < 3:
    print('Usage: {} dump_directory delimeter1 [delimeter2 ...]'.format(sys.argv[0]))
    exit(1)

chunkSize = 131072

totalProcessed = totalDuration = totalBytes = 0
delims = sys.argv[2:]
for i in range(len(delims)):
    delims[i] = delims[i].encode()

def splitter(cred, count=0):
    try:
        username, password = cred.split(delims[count], 1)
        return True, username, password
    except ValueError:
        return splitter(cred, count + 1)
    except IndexError:
        return False, None, None

commonDelims = [b':', b';', b'|']
regexpReplacements = re.compile(b'(' + b'|'.join(map(re.escape, commonDelims)) + b')')
def detector(cred):
    try:
        return regexpReplacements.sub(lambda match: b'\033[1m\033[31m' + match.group(1) + b'\033[0m', cred).decode()
    except:
        return cred

archive = Path(sys.argv[1])

meta = {
    'count': {
        'total': 0,
        'success': 0,
        'errors': {
            'empty_lines': 0,
            'excessively_long': 0,
            'parse_errors': 0,
            'empty_passwords': 0,
        },
        'bytes_est': 0,
    },
    'size': archive.stat().st_size,
    'added_on': datetime.now().timestamp(), #.strftime('%Y-%m-%d %H:%M:%S'),
    'file': str(archive),
    'tag': 'Collection #1',
    'completed': False,
    'chunk_size_used': chunkSize,
    'duration': 0,
}

startTime = time.time()
saved = None

print(f'[{archive}] [{archive.stat().st_size / 2 ** 20:.2f} MB] Processing ... ')
sys.stdout.flush()

try:
    with open(archive, 'rb') as f:

        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(f)

        while True:

            # Chunk Processing
            chunk = reader.read(chunkSize)
            if not chunk: break
            if saved:
                chunk = saved + chunk
                saved = None
            splitted = chunk.splitlines()
            trail = splitted.pop()
            if trail:
                saved = trail

            # Refine Each Credential in Chunk
            for cred in splitted:

                meta['count']['total'] += 1

                # Count Specific Erorrs
                if not cred:
                    meta['count']['errors']['empty_lines'] += 1
                    continue
                if len(cred) > 1024:
                    meta['count']['errors']['excessively_long'] += 1
                    continue
                success, username, password = splitter(cred)
                if not success:
                    meta['count']['errors']['parse_errors'] += 1
                    cred = detector(cred)
                    print(cred)
                    continue
                if not password:
                    meta['count']['errors']['empty_passwords'] += 1
                    continue

                # Write
                # TODO
                meta['count']['success'] += 1
                meta['count']['bytes_est'] += len(username) + len(password) + 1

except KeyboardInterrupt:
    pass

else:
    if meta['count']['total'] > 0:
        meta['completed'] = True

finally:
    totalProcessed += meta['count']['total']
    totalBytes += meta['count']['bytes_est']
    meta['duration'] = time.time() - startTime
    totalDuration += meta['duration']

    print(f'{"SUCCESS" if meta["completed"] else "FAILED"}. Took {meta["duration"]:.2f}s, {meta["count"]["total"] / meta["duration"] if meta["duration"] > 0 else 0:.0f} creds/s, {meta["count"]["total"]} creds processed, {meta["count"]["bytes_est"] / 1048576:.0f} MB est bytes.')
    print(meta)
    print()
    print(f'Parse Errors : \033[1m\033[92m{meta["count"]["errors"]["parse_errors"]}\033[0m')
    print(f'Delimeters   : {delims}')

