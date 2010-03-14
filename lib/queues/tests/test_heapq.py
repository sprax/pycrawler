import os
import sys
import heapq
import tempfile
from time import time
from random import random

def log(msg):
    sys.stderr.write(msg + "\n")
    sys.stdout.flush()

if __name__ == '__main__':
    test_heapq = "test_heapq"
    if not os.path.exists(test_heapq):
        os.makedirs(test_heapq)

    num_files = 10000
    num_lines = 10

    file_names = []
    for i in range(num_files):
        data = ["%.10f\n" % random() for j in range(num_lines)]
        data.sort()
        path = os.path.join(test_heapq, str(i))
        f = open(path, "w")
        f.writelines(data)
        f.close()
        file_names.append(path)
    log("created %d files" % len(file_names))

    max_open = 512
    loops = []
    start = time()
    to_delete = []
    while len(file_names) > 0:
        t0 = time()
        # open up to max_open files to merge
        to_merge = []
        for x in range(max_open):
            try:
                to_merge.append(open(file_names.pop(), "r"))
            except IndexError:
                break
        t1 = time()
        if len(file_names) == 0:
            write_to = sys.stdout # almost done!
        else:
            write_to = tempfile.NamedTemporaryFile(delete=False)
            # schedule it for deletion after all done
            to_delete.append(write_to.name)
            # put its name at end of file_names to merge
            file_names.append(write_to.name)
        t2 = time()
        write_to.writelines(heapq.merge(*to_merge))
        t3 = time()
        for fh in to_merge:
            fh.close()
        t4 = time()
        loops.append((t0, t1, t2, t3, t4))
    start_remove = time()
    for fn in to_delete:
        os.remove(fn)
    end = time()
    log("took %.6f seconds to remove %d tempfiles" % (end - start_remove, len(to_delete)))
    elapsed = end - start
    num_total = num_lines * num_files
    rate = num_total / elapsed
    log("merged %d lines in %.3f seconds, %.3f lines/second" % (num_total, elapsed, rate))
    log("%d loops:" % len(loops))
    deltas = []
    while True:
        try:
            loop = loops.pop()
        except IndexError:
            break
        d = []
        for i in range(1, 5):
            d.append(loop[i] - loop[i - 1])
        deltas.append(d)
        prev = next

    def avg(vals):
        if not vals: return 0.0
        return sum(vals) / float(len(vals))

    names = {0: "build merge_to",
             1: "make tempfile",
             2: "merge files",
             3: "close files"}

    for i in range(4):
        samples = [d[i] for d in deltas]
        log("%s:\t%.20f" % (names[i], avg(samples)))
