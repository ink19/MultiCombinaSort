#!/usr/bin/env python3

import os
import sys
import random


out_file_n = int(sys.argv[1])


for out_file_index in range(out_file_n):
  with open("data/%d.data"%out_file_index, "w+") as fp:
    for i in range(100000):
      fp.write("%d\n"%random.randint(0, (2 ** 31) - 1))
