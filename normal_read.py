#!/usr/bin/env python
import sys
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
from rdflib.term import URIRef
from rdflib import Literal, XSD
import time

reload(sys)
sys.setdefaultencoding("utf-8")

class MyReader():
    
    def __init__(self):
        self.d = dict()
    
    class StreamSink(Sink):
        
        def __init__(self, d):
            self.d = d
        
        def insert(self, o, s):
            if o not in self.d:
                self.d[o] = list()
            self.d[o].append(s)
        
        def triple(self, s, p, o):
            if type(o) is Literal:
                if o.datatype == None:
                    self.insert(unicode(o), unicode(s))
                elif o.datatype == XSD.string:
                    self.insert(unicode(o), unicode(s))
    
    def start(self, filename):
        
        src = self.StreamSink(self.d)
        src_n = NTriplesParser(src)
        start = time.time()
        with open(filename, "r") as anons:
            src_n.parse(anons)
        end = time.time()

        print "{} strings indexed.".format(len(self.d))
        print "Time: {} s.".format((end-start))
    
        return self.d
    

if __name__ == '__main__':
    r = MyReader()
    d = r.start(sys.argv[1])
    print d
