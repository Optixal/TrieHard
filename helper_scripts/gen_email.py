#!/usr/bin/env python3

import re

formatCheck = re.compile(r'{(.*?)}')

domains = [
    'gmail.com',
    'hotmail.{}',
    'yahoo.{}',
    'live.{}',
    'mail.{}',
    'msf.com',
    'web.{}',
    'facebook.com',
    'aol.com',
    'googlemail.com',
]

cctld = [
    'com',
    'net',
    'co',
    'fr',
    'it',
    'de',
    'co.uk',
    'nl',
    'com.sg',
    'sg',
]


def generateEmails(username):
    for domain in domains:
        if formatCheck.findall(domain):
            for tld in cctld:
                yield f'{username}@{domain.format(tld)}'
        else:
            yield f'{username}@{domain}'


if __name__ == '__main__':
    for email in generateEmails('testing'):
        print(email)

