#!/usr/bin/env python

import sys
import time
import random

sleep = 0
start_time = time.time()
max_records = 1000 * 1000
repeat_every = None

if len(sys.argv) > 1:
	max_records = int(sys.argv[1])
if len(sys.argv) > 2:
	sleep = int(sys.argv[2])
if len(sys.argv) > 3:
	repeat_every = int(sys.argv[3])

i = 1
while i <= max_records:
        if repeat_every:
		record_key = i % repeat_every
	else:
		record_key = i

	print "Iteration %d (key=%d) at %s, %d seconds elapsed" % (i, record_key, time.strftime("%H:%M:%S"), int(time.time() - start_time))
	sys.stdout.flush()
	if sleep > 0:
		time.sleep(sleep)
	i = i + 1
