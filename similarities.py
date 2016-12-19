#!/usr/bin/env python
import distance

def compute(x, y):
    v = list()
    v.append(1-distance.jaccard(x, y))
    v.append(1-distance.nlevenshtein(x, y))
    return v

if __name__ == '__main__':
    print compute("ciao", "hi")
