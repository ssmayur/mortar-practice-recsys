#!/usr/bin/env python

"""
 Copyright 2014 Mortar Data Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "as is" Basis,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
"""

"""
This script helps tune the LOGISTIC_PARAM parameter.
It requires that you have numpy, scipy, and matplotlib installed
If you do not, the easiest way to install is using the free tool Anaconda:
https://store.continuum.io/cshop/anaconda/

In the mortar recommender system, the scores of duplicate user-item records are first summed up,
and then a logistic scale (diminishing returns) is applied.
Given a value for the logistic parameter, this script will produce a graph
showing raw score values on the x-axis and scaled values on the y-axis.

To run the script use:

python logistic_scale_vis.py LOGISTIC_PARAM

where LOGISTIC_PARAM is a floating point number
"""

import numpy as np
import matplotlib.pyplot as plt
import sys

from math import ceil, exp, log
from scipy.optimize import fmin

if len(sys.argv) != 2:
    print "Usage: %s logistic-param" % sys.argv[0]

logistic_param = float(sys.argv[1])

def logistic_scale(x):
    return -1.0 + 2.0 / (1.0 + exp(-logistic_param * x))

def logistic_scale_inv(y):
    return -log(2.0 / (y + 1.0) - 1) / logistic_param

def frange(start, end = None, inc = 1.):
    if end is None:
        end, start = start, 0.
    else:
        start = float(start)
        end = float(end)

    count = int(ceil((end - start) / inc)) + 1
    return np.asarray([start + n*inc for n in xrange(count)])

fig = plt.figure()
ax = fig.add_subplot(111)

start = 0.0
end = logistic_scale_inv(0.99)
inc = (end - start) / 100.0

plt.xlim([start, end])
plt.ylim([0.0, 1.0])

X = frange(start, end, inc)
Y = np.vectorize(logistic_scale)(X)
ax.plot(X, Y)

Y = np.asarray([0.25, 0.33, 0.50, 0.66, 0.75, 0.90, 0.95])
X = np.vectorize(logistic_scale_inv)(Y)
labels = ["(%.2f, %.2f)" % (x, y) for x, y in zip(X, Y)]
ax.scatter(X, Y)
for label, x, y in zip(labels, X, Y):
    plt.annotate(
        label,
        xy = (x, y), xytext = (15, -30),
        textcoords = 'offset points', ha = 'left', va = 'bottom',
        arrowprops = dict(arrowstyle = '->', connectionstyle = 'arc3,rad=0')
    )

plt.show()
