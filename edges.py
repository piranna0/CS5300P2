#! /Users/ay226/anaconda/python.app/Contents/MacOS/python

from __future__ import division
import numpy as np
import sys

if __name__ == '__main__':
    x = np.loadtxt(sys.argv[1],dtype='string',usecols=(0,1))
    y = open(sys.argv[2],'w')
    #f = lambda i: (x[:,1][x[:,0]==i]).astype(np.int32)
    #minnode = x[0,0]
    maxnode = x[-1,0]
    height = np.shape(x)[0]
    last = 0
    for i in range(1,height):
        i1 = x[i-1,0]
        i2 = x[i,0]
        if i%1000000==0:
            print 'we are at ' + str(i)
        if i1<>i2:
            nodes = x[last:i,1]
            s = '_'.join(nodes)
            s2 = ''
            s2 = i1 + ';' + str(1/int(maxnode)) + '_' + s
            y.write(s2)
            y.write('\n')
            last = i
            for j in range(int(i1)+1 , int(i2)):
                s2 = str(j) + ';' + str(1/int(maxnode)) + '_-1'
                y.write(s2)
                y.write('\n')
    y.close()
