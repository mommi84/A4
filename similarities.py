#!/usr/bin/env python
import distance

def compute(x, y):
    if x == "" and y == "":
        return 1.0
    v = list()
    # print "{} ### {}".format(x,y)
    v.append(1-distance.jaccard(x, y))
    v.append(1-distance.nlevenshtein(x, y))
    return float(sum(v)) / max(len(v), 1) # mean

if __name__ == '__main__':
    print compute("ciao", "hi")
