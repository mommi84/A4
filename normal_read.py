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
        
        def triple(self, s, p, o):
            # global d
            if type(o) is Literal:
                if o.datatype == None:
                    self.d[unicode(o)] = unicode(s)
                elif o.datatype == XSD.string:
                    self.d[unicode(o)] = unicode(s)
    
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
