#!/usr/bin/env python
import sys
from normal_read import MyReader
import multiprocessing as mp
from joblib import Parallel, delayed
from cbd_getter import CBDGetter
from rdflib import Literal, XSD
import similarities as sim
import numpy as np
# import ppjoin

reload(sys)
sys.setdefaultencoding("utf-8")

datasets = sys.argv[1:3]
g_truth = sys.argv[3]
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
            examples.append((s, t))
print "examples: {}".format(examples)

# TODO (similar) similarity join
# l = list(labels[0].keys()).extend(list(labels[1].keys()))
# print l
# _, x, _ = ppjoin.prepare_strings(l)
# print ppjoin.candidate_pairs(x)
# print x[1], x[2]

def label_examples(examples):
    pos_ex = dict()
    OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"
    with open(g_truth) as f:
        for line in f:
            for ex in examples:
                line2 = "<{}> <{}> <{}> .\n".format(ex[0], OWL_SAMEAS, ex[1])
                if line == line2:
                   pos_ex[ex] = 1
    return pos_ex

# (A) label examples until n/2 < p < 2n
pos_ex = label_examples(examples)
n_pos = len(pos_ex)
n_neg = len(examples) - n_pos
print "{} positive, {} negative examples found.".format(n_pos, n_neg)
for i in range(len(examples)-1):
    # generate negatives
    gen = list()
    gen.append((examples[i][0], examples[i+1][1]))
    new_pos = label_examples(gen)
    n_pos += len(new_pos)
    n_neg += len(gen) - len(new_pos)
    print "{} positive, {} negative examples found.".format(n_pos, n_neg)
    # update lists
    examples += gen
    pos_ex.update(new_pos)
    if 2 * n_neg > n_pos and 2 * n_pos > n_neg:
        break

# TODO create features
# get CBDs
g_src, g_tgt = CBDGetter(datasets[0]), CBDGetter(datasets[1])
cbd_src = g_src.get(examples, 0)
cbd_tgt = g_tgt.get(examples, 1)
print "CBD(src) = {}".format(cbd_src)
print "CBD(tgt) = {}".format(cbd_tgt)
feat = dict() # dict of feature vectors
indices = dict() # indices for the respective pair (p1,p2)
# calculate number of properties
prop1 = set()
for cbd in cbd_src:
    for s, p, o in cbd_src[cbd]:
        prop1.add(unicode(p))
prop2 = set()
for cbd in cbd_tgt:
    for s, p, o in cbd_tgt[cbd]:
        prop2.add(unicode(p))
n_p1, n_p2 = len(prop1), len(prop2)
del prop1, prop2
# create features
for ex in examples:
    v = np.zeros(n_p1 * n_p2)
    src, tgt = cbd_src[ex[0]], cbd_tgt[ex[1]]
    # select source...
    for s1, p1, o1 in src:
        if type(o1) is Literal:
            if o1.datatype == None or o1.datatype == XSD.string:
                x = unicode(o1)
            else:
                continue
        else:
            continue
        # select target...
        for s2, p2, o2 in tgt:
            if type(o2) is Literal:
                if o2.datatype == None or o2.datatype == XSD.string:
                    y = unicode(o2)
                    sim_xy = sim.compute(x,y)
                    print p1, p2, x, y, sim_xy
                    if (p1, p2) not in indices:
                        indices[(p1, p2)] = len(indices)
                    ind = indices[(p1, p2)]
                    v[ind] = float(sum(sim_xy)) / max(len(sim_xy), 1) # mean
                    # print " --- added: {}".format(v[ind])
    feat[ex] = v
    if ex in pos_ex:
        pass # positive example
    
for ex in feat:
    print ex, feat[ex]
    
# TODO classify

# TODO parallelized evaluation (on sub-sample?)

# TODO if not termination criteria, choose next examples and goto (A)

