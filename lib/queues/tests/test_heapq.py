import os
import heapq
from time import time
from random import random

test_heapq = "test_heapq"
if not os.path.exists(test_heapq):
    os.makedirs(test_heapq)

num_files = 1000
num_lines = 1000

files = []
for i in range(num_files):
    data = ["%.10f\n" % random() for j in range(num_lines)]
    data.sort()
    path = os.path.join(test_heapq, str(i))
    f = open(path, "w")
    f.writelines(data)
    f.close()
    f = open(path, "r")
    files.append(f)

print "created %d files" % len(files)

output = open(os.path.join(test_heapq, "output"), "w")
start = time()
for line in heapq.merge(*files):
    output.write(line)
output.close()
end = time()
for f in files:
    f.close()
elapsed = end - start
num_total = num_lines * num_files
rate = num_total / elapsed
print "merged %d lines in %.3f seconds, %.3f lines/second" % (num_total, elapsed, rate)



