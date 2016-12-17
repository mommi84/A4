#!/usr/bin/env python
import sys
from normal_read import MyReader
import multiprocessing as mp
from joblib import Parallel, delayed
# import ppjoin

reload(sys)
sys.setdefaultencoding("utf-8")

N_EXAMPLES = 10

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
print "Indexing strings in datasets..."
datasets = sys.argv[1:3]
labels = Parallel(n_jobs = 2)(delayed
    (read)(f) for f in datasets
)

# (exact) similarity join
print "|S|={}, |T|={}".format(len(labels[0]), len(labels[1]))
print "Launching similarity join..."
indices = dict()
for k in labels[0]:
    if k in labels[1]:
        # print "Match in S and T: {}".format(k)
        # print "\t{}, {}".format(labels[0][k], labels[1][k])
        # as examples, get the ones in the smallest sets (labels[0][k] and labels[1][k])
        n = len(labels[0][k]) + len(labels[1][k])
        if len(indices) < N_EXAMPLES:
            indices[k] = n
        else:
            if n < max(indices.values()):
                indices[k] = n
                temp = dict()
                i = 0
                for key, value in sorted(indices.iteritems(), key=lambda (k,v): (v,k)):
                    temp[key] = value
                    i += 1
                    if i == N_EXAMPLES:
                        break
                indices = temp
        # print " === {}".format(indices)
# collect examples
print "Collecting examples..."         
examples = list()
for k in indices:
    setS = labels[0][k]
    setT = labels[1][k]
    for s in setS:
        for t in setT:
            examples.append([s, t])
print "examples: {}".format(examples)

# TODO (similar) similarity join
# l = list(labels[0].keys()).extend(list(labels[1].keys()))
# print l
# _, x, _ = ppjoin.prepare_strings(l)
# print ppjoin.candidate_pairs(x)
# print x[1], x[2]

# TODO (A) label examples

# TODO classify

# TODO parallelized evaluation (on sub-sample?)

# TODO if not termination criteria, choose next examples and goto (A)

