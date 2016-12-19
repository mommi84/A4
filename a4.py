#!/usr/bin/env python
import sys
from normal_read import MyReader
import multiprocessing as mp
from joblib import Parallel, delayed
import feature_build as fb
import learning
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

# feature creation
feats, classes = fb.build(examples, pos_ex, datasets)

# classify
svm = learning.learn(feats, classes)
print "Training accuracy: {}".format(learning.accuracy(svm, feats, classes)) 

# evaluation
all_examples = set()
# TODO parallelize?
for obj_src in labels[0]:
    for sbj_src in labels[0][obj_src]:
        for obj_tgt in labels[0]:
            for sbj_tgt in labels[0][obj_tgt]:
                # get all subjects (which are in labels[0] and labels[1])
                all_examples.add((sbj_src, sbj_tgt))
print len(all_examples)
# TODO filter out non-estimated (p1,p2) properties
# all_feats, all_classes = fb.build(all_examples, pos_ex, datasets)
# print "Test accuracy: {}".format(learning.accuracy(svm, all_feats, all_classes))


# TODO if not termination criteria, choose next examples and goto (A)

