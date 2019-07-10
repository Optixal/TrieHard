#!/usr/bin/env python3
# 2019 | Optixal

# For max_folder_depth of 3
# dir  - dir  - dir  - leaf
# 37   + 38^2 + 38^3 + 38^4
# = 2,141,489 files/folders
#
#   2,141,489 * 1024 (1KB) / 1,073,741,824 (1GB)
# = 2.04 GB

import os
import sys
import string
from itertools import product

if len(sys.argv) < 2:
    print(f'Usage: {sys.argv[0]} max_folder_depth')
    exit(1)

outputDir = '/home/optixal/Downloads/outputsilo/'
charset = string.ascii_lowercase + string.digits + '%'
maxDirDepth = int(sys.argv[1])
for i in product(charset, repeat=maxDirDepth):
    path = outputDir + '/'.join(i)
    # print(path)
    os.makedirs(path)
