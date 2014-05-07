#! /Users/ay226/anaconda/python.app/Contents/MacOS/python

from __future__ import division
import numpy as np
import sys

#arg1: edges
#arg2: output file
#arg2: nodes.txt
if __name__ == '__main__':
    x = np.loadtxt(sys.argv[1],dtype='string',usecols=(0,1))
    y = open(sys.argv[2],'w')
    z = np.loadtxt(sys.argv[3],dtype='string',usecols=(1,))
    maxnode = x[-1,0]
    height = np.shape(x)[0]
    f = lambda i: z[int(i)] + '~' + i # i is a string
    last = 0
    for i in range(1,height):
        i1 = x[i-1,0]
        i2 = x[i,0]
        if i%1000000==0:
            print 'we are at ' + str(i)
        if i1<>i2:
            nodes = x[last:i,1]
            nodes2 = map(f,nodes)
            s = '_'.join(nodes2)
            s2 = z[int(i1)] + ';' + i1 + '_-1_' + str(1/int(maxnode)) + '_' + s
            y.write(s2)
            y.write('\n')
            last = i
            for j in range(int(i1)+1 , int(i2)):
                s2 = z[j] + ';' + str(j) + '_-1_' + str(1/int(maxnode)) + '_-1'
                y.write(s2)
                y.write('\n')
    y.close()
