#!/usr/bin/env python
from cbd_getter import CBDGetter
import similarities as sim
from rdflib import Literal, XSD
import numpy as np

def build(examples, pos_ex, datasets):
    g_src, g_tgt = CBDGetter(datasets[0]), CBDGetter(datasets[1])
    cbd_src = g_src.get(examples, 0)
    cbd_tgt = g_tgt.get(examples, 1)
    print "CBD(src) = {}".format(cbd_src)
    print "CBD(tgt) = {}".format(cbd_tgt)
    feat = dict() # dict of feature vectors
    clas = dict() # dict of classes
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
                        v[ind] = sim_xy
                        # print " --- added: {}".format(v[ind])
        feat[ex] = v
        if ex in pos_ex:
            clas[ex] = 1 # positive example
        else:
            clas[ex] = 0 # negative example

    for ex in feat:
        print ex, feat[ex], clas[ex]
    print feat.values()
    print clas.values()
    return feat.values(), clas.values()
    
