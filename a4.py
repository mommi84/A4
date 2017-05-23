#!/usr/bin/env python
"""

TODO semantic filtering (keep super-properties of known property matches)
TODO add language-aware similarity measures (word2vec, wordnet)
TODO add compatibility with any classifier

"""
import sys
from normal_read import MyReader
import multiprocessing as mp
from joblib import Parallel, delayed
import feature_build as fb
import learning
import time
# import ppjoin
import emailer

reload(sys)
sys.setdefaultencoding("utf-8")

OWL_SAMEAS = "http://www.w3.org/2002/07/owl#sameAs"

datasets = sys.argv[1:3]
g_truth = sys.argv[3]
BATCH_MSIZE = int(sys.argv[4])
N_EXAMPLES = int(sys.argv[5])
N_CORES = mp.cpu_count()

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

# import pdb; pdb.set_trace()

# TODO (similar) similarity join
# l = list(labels[0].keys()).extend(list(labels[1].keys()))
# print l
# _, x, _ = ppjoin.prepare_strings(l)
# print ppjoin.candidate_pairs(x)
# print x[1], x[2]

def label_examples(examples):
    pos_ex = dict()
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
feats, classes, pr_indices = fb.build(examples, pos_ex, datasets)
print "pr_indices ({}) = {}".format(len(pr_indices), pr_indices)

# classify
svm = learning.learn(pr_indices, feats, classes)
print "Training F1-score: {}".format(learning.f_score(pr_indices, svm, feats, classes)) 

# batch msize     rate   memory
#         100     ~800      490
#         500    ~1300      450
#        1000    ~1300      610
#        2000    ~1100      460
#        5000    ~1000      350

# on-the-fly evaluation
srcuris = set()
for v in labels[0].values()[:100]:
    srcuris = srcuris.union(v)
tgturis = set()
for v in labels[1].values()[:100]:
    tgturis = tgturis.union(v)
print "SRC URIs={}, TGT URIs={}".format(len(srcuris), len(tgturis))

def pair_gen(labels):
    # get all subjects (which are in labels[0] and labels[1])
    pairs = list()
    for sbj_src in srcuris: 
        for sbj_tgt in tgturis: 
            pair = (sbj_src, sbj_tgt)
            pairs.append(pair)
            if len(pairs) == BATCH_MSIZE:
                yield pairs
                # empty list
                del pairs[:]
    # yield remainder
    if len(pairs) > 0:
        yield pairs

space_size = 0
for pairs in pair_gen(labels):
    space_size += len(pairs)
print "Expecting {} pairs.".format(space_size)

def evaluation(onepair):
    # print "len(pairs)=",len(onepair)
    # onepair = list()
    # onepair.append(pair)
    # print "Testing pair: {}".format(pair)
    # print "Building features..."
    pair_feats, pair_classes, pair_indices = fb.build_test(pr_indices, onepair, pos_ex, datasets)

    # print "Evaluating pair..."
    tp, fp, tn, fn, fp_list, fn_list = learning.f_score(pr_indices, svm, pair_feats, pair_classes)
    print "Test F1-score: {}".format((tp, fp, tn, fn))
    for x in fp_list:
        print "FP: {}".format(onepair[x])
    for x in fn_list:
        print "FN: {}".format(onepair[x])
    return (tp, fp, tn, fn)

start = time.time()
# evaluation
result = Parallel(n_jobs = N_CORES)(delayed
    (evaluation)(pair) for pair in pair_gen(labels)
)
end = time.time()
tot = 0
for r in result:
    tot += sum(r)
print "Tot (dedupl.): {}".format(tot)
rate = tot / (end-start)
print "Start: {} - End: {}".format(start, end)
print "Rate: {} ex/s".format(rate)
print "result (tp, fp, tn, fn) = ", result

tp = 0
fp = 0
tn = 0
fn = 0
for r in result:
    tp += r[0]
    fp += r[1]
    tn += r[2]
    fn += r[3]
print "tp={}, fp={}, tn={}, fn={}".format(tp, fp, tn, fn)
pre = tp / float(tp + fp) if tp + fp > 0 else 0.0
rec = tp / float(tp + fn) if tp + fn > 0 else 0.0
f1 = 2 * pre * rec / (pre + rec) if pre + rec > 0 else 0.0
out = "f1={}, pre={}, rec={}".format(f1, pre, rec)
print out
# emailer.send("{}-{}: {}".format(datasets[0], datasets[1], out))

# TODO if not termination criteria, choose next examples and goto (A)
