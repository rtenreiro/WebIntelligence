#!/usr/bin/env python

import glob
import mincemeat

text_files=glob.glob('hw3data/*')

def file_contents(file_name):
    f=open(file_name)
    try:
        return f.read()
    finally:
        f.close()

source=dict((file_name,file_contents(file_name))
	for file_name in text_files)

# setup map and reduce functions

def mapfn(key,value):
      for line in value.splitlines():
          for word in line.split():
               yield word.lower(),1
                   
             

def reducefn(key,value):
       return key,len(value)
    
# start the server

s =	mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")
print (results)
i = open('outfile','w')
i.write(str(results))
i.close()


"""

import re
import glob
import mincemeat
from collections import Counter

text_files=glob.glob('./c0001')

def file_contents(file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

datasource = dict((file_name, file_contents(file_name)) for file_name in text_files)

def mapfn(key, value):
    for line in value.splitlines():
        wordsinsentence = line.split(":::")
        authors = wordsinsentence[1].split("::")
        # print authors
        words = str(wordsinsentence[2])
        words = re.sub(r'([^\s\w-])+', '', words)
        # re.sub(r'[^a-zA-Z0-9: ]', '', words)
        words = words.split(" ")
        for author in authors:
            for word in words:
                word = word.replace("-"," ")
                word = word.lower()
                yield author, word

def reducefn(key, value):
    return Counter(value)

s = mincemeat.Server()
s.datasource = datasource
s.mapfn = mapfn
s.reducefn = reducefn
results = s.run_server(password="changeme")
# print results
print (results)

i = open('outfile','w')
i.write(str(results))
i.close()"""

