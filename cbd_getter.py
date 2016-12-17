#!/usr/bin/env python
import sys
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
from rdflib.term import URIRef

reload(sys)
sys.setdefaultencoding("utf-8")

class CBDGetter():
    
    def __init__(self, filename):
        self.filename = filename
    
    class StreamSink(Sink):
    
        def __init__(self, filename, uri, cbd):
            self.filename = filename
            self.uri = uri
            self.cbd = cbd
        
        def triple(self, s, p, o):
            if unicode(s) == self.uri or unicode(o) == self.uri:
                self.cbd.append((s,p,o))

    def get(self, uri):
        self.cbd = list()
        src = self.StreamSink(self.filename, uri, self.cbd)
        src_n = NTriplesParser(src)
        with open(self.filename, "r") as anons:
            src_n.parse(anons)
        return self.cbd

if __name__ == '__main__':
    g = CBDGetter(sys.argv[1])
    cbd = g.get(sys.argv[2])
    print cbd
    