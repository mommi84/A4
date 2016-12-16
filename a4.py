#!/usr/bin/env python
import sys
from normal_read import MyReader
import multiprocessing as mp
from joblib import Parallel, delayed

reload(sys)
sys.setdefaultencoding("utf-8")

def read(f):
    print "Started reading {}...".format(f)
    d = MyReader().start(f)
    print "Done."
    for k in d:
        print k.decode("utf-8")
        for v in d[k]:
            print "\t" + v.decode("utf-8")

# index strings in datasets
datasets = sys.argv[1:3]
Parallel(n_jobs = 2)(delayed
    (read)(f) for f in datasets
)
