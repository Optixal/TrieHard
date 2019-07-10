#!/usr/bin/env python3
# 2019 | Optixal

import sys
import zstandard as zstd
# import hasher
import replacer
import struct
import os
import time
from datetime import datetime
from pathlib import Path

delims = [b':', b';', b'|']
def splitter(cred, count=0):
    try:
        username, password = cred.split(delims[count], 1)
        return True, username, password
    except ValueError:
        return splitter(cred, count + 1)
    except IndexError:
        return False, None, None

def generateOutputs(key, value, depth=3):

    content = b''

    # Generate full path of leaf
    leafPathList = []
    for i in range(depth):
        leafPathList.append(47) # b'/'
        try:
            c = key[i]
        except IndexError:
            leafPathList.append(36) # b'$', '/c/a/t/$' for depth=4
            break
        if (c >= 97 and c <= 122) or (c >= 48 and c <= 57):
            leafPathList.append(c) # lowercase-alphanumeric
        else:
            leafPathList.append(37) # b'%'
            content += struct.pack('B', c) # save special chars into content; using struct.pack as this condition occurs less often, making it faster than .append & bytes

    leafPath = bytes(leafPathList)
    content += key[depth:] + b'\x00' + value # final content: saved special chars + remaining key + b'\x00' + value

    return leafPath, content


def refineArchive(archiveName, outputDir, chunkSize=1024 * 128):
    archive = Path(archiveName)

    # Check whether this dump has been added before
    # hash = hasher.xxhashsum(archive).encode()
    # if:
        # continue

    meta = {
        'count': {
            'total': 0,
            'success': 0,
            'errors': {
                'empty_lines': 0,
                'excessively_long': 0,
                'parse_errors': 0,
                'empty_fields': 0,
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

    print(f'[{archive}] [{archive.stat().st_size / 2 ** 20:.2f} MB] Processing ... ', end='')
    sys.stdout.flush()

    try:
        with open(archive, 'rb') as f:

            dctx = zstd.ZstdDecompressor()
            reader = dctx.stream_reader(f)
            memory = {}
            batchCounter = 0

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
                        continue
                    if not username or not password:
                        meta['count']['errors']['empty_fields'] += 1
                        continue

                    # Compression
                    username = username.lower() # assumes all usernames/emails/keys are case insensitive
                    username = replacer.compress(username)

                    # Write
                    leafPath, content = generateOutputs(username, password)

                    if leafPath in memory:
                        memory[leafPath].append(content)
                    else:
                        memory[leafPath] = [content]

                    meta['count']['success'] += 1
                    batchCounter += 1

                    if batchCounter % 100000 == 0:
                        print(f'Speed: {meta["count"]["total"] / (time.time() - startTime):.2f} creds/s')
                        print(f'Batch Count: {batchCounter}');
                        memoryLen = len(memory.keys())
                        print(f'Length of memory: {memoryLen}')
                        ratio = memoryLen / batchCounter
                        print(f'Ratio: {ratio:.2f}');
                        # print(f'Memory used: {sys.getsizeof(memory) / 1048576:.2f} MB')
                        if ratio < 0.2:
                            print('Ratio reached! Writing...')
                            writeStartTime = time.time()
                            for leafPath, contents in memory.items():
                                dirPath = leafPath[:-2] # b'/a/b/c' -> b'/a/b'
                                os.makedirs(outputDir + dirPath, exist_ok=True)
                                with open(outputDir + leafPath, 'ab') as outputFile:
                                    for content in contents:
                                        outputFile.write(content + b'\n')
                            memory = {}
                            batchCounter = 0
                            print(f'WRITTEN! Took {time.time() - writeStartTime:.2f}s')
                        print()
                    # DEBUG
                    # meta['count']['bytes_est'] += len(username) + 1 + len(password)

    except KeyboardInterrupt:
        raise KeyboardInterrupt
    except Exception as e:
        print(f'[-] Error detected: {e}')
    else:
        if meta['count']['total'] > 0:
            meta['completed'] = True
    finally:
        meta['duration'] = time.time() - startTime
        print(f'{"SUCCESS" if meta["completed"] else "FAILED"}. Took {meta["duration"]:.2f}s, {meta["count"]["total"] / meta["duration"] if meta["duration"] > 0 else 0:.0f} creds/s, {meta["count"]["total"]} creds processed, {meta["count"]["bytes_est"] / 1048576:.0f} MB est bytes.')
        return meta


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print('Usage: {} [dump_directory]'.format(sys.argv[0]))
        exit(1)

    outputDir = b'/home/optixal/Downloads/outputsilo/'
    totalProcessed = totalDuration = totalBytes = 0
    # for archiveName in Path(sys.argv[1]).rglob('*.zst'):
    for archiveName in sys.argv[1:]:

        try:
            metaResult = refineArchive(archiveName, outputDir)
        except KeyboardInterrupt:
            break

        totalProcessed += metaResult['count']['total']
        totalBytes += metaResult['count']['bytes_est']
        totalDuration += metaResult['duration']

    print(f'[+] Done! Total duration {totalDuration:.2f}s, \033[1m\033[92m{totalProcessed / totalDuration if totalDuration > 0 else 0:.0f}\033[0m avg creds/s, {totalProcessed} total creds processed, \033[1m\033[95m{totalBytes / 1048576:.0f} MB\033[0m total est bytes.')

