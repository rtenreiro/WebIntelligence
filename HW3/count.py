#!/usr/bin/env python
import re
from collections import Counter
import glob
import mincemeat

text_files = glob.glob('hw3data/*')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source = dict((file_name,file_contents(file_name))
                  for file_name in text_files)

def mapfn(key, value):
    for line in value.splitlines():
        wordsinsentence = line.split(":::")
        authors = wordsinsentence[1].split("::")
        words = wordsinsentence[2].split(" ")
        #words = re.sub(r'([^\s\w-])+', '', words)
        for author in authors:
            for word in words:
                #word = word.replace("-"," ")
                word = word.lower()
                yield  author, word

def reducefn(key, value):
    return Counter(value)


s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")

print (results)

i = open('outF','w')
i.write(str(results))
i.close()

