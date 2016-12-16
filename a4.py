#!/usr/bin/env python
import sys
from normal_read import MyReader
import multiprocessing as mp
from joblib import Parallel, delayed
import ppjoin

reload(sys)
sys.setdefaultencoding("utf-8")

def read(f):
    print "Started reading {}...".format(f)
    d = MyReader().start(f)
    print "Done."
    # for k in d:
    #     print k.decode("utf-8")
    #     for v in d[k]:
    #         print "\t" + v.decode("utf-8")
    return d

# index strings in datasets
datasets = sys.argv[1:3]
labels = Parallel(n_jobs = 2)(delayed
    (read)(f) for f in datasets
)

# similarity join
print "|S|={}, |T|={}".format(len(labels[0]), len(labels[1]))
for k in labels[0]:
    if k in labels[1]:
        print "Match in S and T: {}".format(k)
        print "\t{}, {}".format(labels[0][k], labels[1][k])

# l = list(labels[0].keys()).extend(list(labels[1].keys()))
# print l
# _, x, _ = ppjoin.prepare_strings(l)
# print ppjoin.candidate_pairs(x)
#
# print x[1], x[2]

