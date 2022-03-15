import numpy as np
import pandas as pd
from binge import B


################################################################################
# how is the number of iterations, n, inferred?

# out of all input parameters, the one with the largest size
# will define the value of n if it is not provided
# all parameters that do not have a siwe of n will be treated as
# size=1, hence copied as-is to each thread

def dum(a, b): return sum(a)*b

# this call will infer an n=4 based on the size of the second parameter
res = B(dum)([1,2], [0,1,2,3])
# and will return [0,3,6,9]

# this call has a n=2 value forced. The first parameter will be passed
# as [0,1,2,3] to each of the 2 threads used
res = B(dum, n=2)([0,1,2,3], [1,2])
# and will return [6,12]


################################################################################
# calculate something out of a large np.array

# by default, B will not split the np.array to the several threads
# and will treat them as elements of size 1
# giving typin='nda' as input allows the splitting on the first
# dimension of the np.array
# by default, B returns a list. giving typout='nda' makes sure
# that the result is a np.array

data = np.random.uniform(low=0, high=100, size=(10, 5000, 5000))

# calculating
r = np.sum(data, axis=tuple(range(1, data.ndim)))

# is equivalent to
r2 = B(np.sum, typin='nda', typout='nda')(data)

r == r2

# You will find that the binged version of the calculation is much
# slower because it needs to copy the data over to process and this
# cannot obviously compete with the pure-C implementation of np.sum
# This approach will become interesting when the processing of the
# data is not a simple operation such as sum


################################################################################
# working with generators

# generators cannot be pickled to several threads
# by default B will not spit generators and will treat them a
# elements of size 1
# giving typin='gen' will force B to transform generators into
# list and split them accross processes

l = [1,2,3,4]
r = B(print, typin='gen')(x for x in l)
# will print 4 lines: 1, then 2, 3, and finally 4

# while
r = B(print)(x for x in l)
# will print 1 line: (1, 2, 3, 4)


################################################################################
# working with strings

# same as generators and nup.array, the strings will not be split accros
# threads unless told so.
# giving typin='str' will force string to be split
# 

def dum(ch): return ch

res = B(dum, typin='str')('hello')
# will return ['h', 'e', 'l', 'l', 'o']


################################################################################
# working with pandas DataFrame

def dum(x):
    return pd.DataFrame.from_dict({'col': x})

data = np.random.uniform(size=(4,10))

res = B(dum, typin='nda', typout='df', threads=4)(data)
