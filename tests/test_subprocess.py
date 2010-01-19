import os
import traceback
import subprocess
from random import random
from PyCrawler.PersistentQueue import PersistentQueue, LineFiles

path = "test"
pq = PersistentQueue(path, marshal=LineFiles())

for i in range(1000):
    pq.put(str(random()))

pq.close()

# get list of all individual cache files:
files = []
for file_path in os.listdir(os.path.join(path, "data")):
    files.append(os.path.join(path, "data", file_path))


sort = subprocess.Popen(
    args=["-u", "-"],
    #args=files,
    executable="sort",
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    #stderr=subprocess.PIPE,
    )

if sort.poll() is not None:
    print "sort process says: " + str(sort.poll())
    print "files: " + str(files)

for file_path in files:
    chunk = open(file_path)
    while True:
        line = chunk.readline()
        if not line: break
        sort.stdin.write(line)

sort.stdin.close()
    
while True:
    line = sort.stdout.readline()
    if not line: break
    print line.strip()

print sort.poll()
