#!/usr/bin/env python
import sys
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
from rdflib.term import URIRef

class CBDGetter():
    
    def __init__(self, filename):
        self.filename = filename
    
    class StreamSink(Sink):
    
        def __init__(self, filename, uris, cbd):
            self.filename = filename
            self.uris = uris
            self.cbd = cbd
        
        def triple(self, s, p, o):
            if unicode(s) in self.uris:
                self.cbd[unicode(s)].append((s,p,o))
                return
            if unicode(o) in self.uris:
                self.cbd[unicode(o)].append((s,p,o))

    def get(self, arr, index):
        uris = list()
        self.cbd = dict()
        for pair in arr:
            uris.append(pair[index])
            self.cbd[pair[index]] = list()
        src = self.StreamSink(self.filename, uris, self.cbd)
        src_n = NTriplesParser(src)
        with open(self.filename, "r") as anons:
            src_n.parse(anons)
        return self.cbd

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    
    g = CBDGetter(sys.argv[1])
    arr = list()
    arr.append((sys.argv[2], "http://example.com/"))
    cbd = g.get(arr, 0)
    print cbd
    