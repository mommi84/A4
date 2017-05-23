import urllib, urllib2
import sys

# split to prevent spam
URL_A = "http://mommi84.alter"
URL_B = "vista.org/noti"
URL_C = "fier/?"

def send(body):
    b = urllib.urlencode({"sysout": body})
    url = URL_A + URL_B + URL_C + b
    print "Opening", url
    urllib2.urlopen(url).read()
    print "Email sent."
    
if __name__ == '__main__':
    send(sys.argv[1])
    