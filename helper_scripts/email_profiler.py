#!/usr/bin/env python3

import sys
import zstandard as zstd
import struct

domains = {}
blockSize = 4096
saved = None
domainID = 1

for archive in sys.argv[1:]:
    with open(archive, 'rb') as f:

        decompressor = zstd.ZstdDecompressor()
        for chunk in decompressor.read_to_iter(f, read_size=blockSize):
            splitted = chunk.split(b'\r\n')
            if saved:
                splitted[0] = saved + splitted[0]
                saved = None
            trail = splitted.pop()
            if trail:
                saved = trail

            for cred in splitted:
                try:
                    username, password = cred.split(b':', 1)
                    user, domain = username.split(b'@', 1)
                except ValueError:
                    continue
                domain = domain.lower()
                if domain in domains:
                    domains[domain] += 1
                else:
                    domains[domain] = 1

sdomains = sorted(domains.items(), key=lambda x: x[1], reverse=True)
print('replacements = {')
for domain, count in sdomains[:255]:
    print("    {:26} : {:8}, # {:3} : {}".format(str(domain), str(struct.pack('B', domainID)), str(domainID), count))
    domainID += 1
print('}')
print('Total: {}'.format(len(domains)))
print('live.com.sg in domains? {}. Count: {}'.format(b'live.com.sg' in domains, domains.get(b'live.com.sg', 0)))
def getIndex(email):
    print('{} index number? {}'.format(email, [k for k, v in sdomains].index(email) if email in sdomains else -1))
getIndex(b'outlook.com')
getIndex(b'hotmail.co.uk')
getIndex(b'hotmail.fr')
getIndex(b'wanadoo.fr')
getIndex(b'free.fr')
getIndex(b'rediffmail.com')
