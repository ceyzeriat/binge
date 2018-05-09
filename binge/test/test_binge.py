#!/usr/bin/env python
# -*- coding: utf-8 -*-

###############################################################################
#  
#  BINGE - Lazy multiprocess your callables in three extra characters
#  Copyright (C) 2018  Guillaume Schworer
#  
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#  
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#  
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#  
#  For any information, bug report, idea, donation, hug, beer, please contact
#    guillaume.schworer@gmail.com
#
###############################################################################


import numpy as np
import time
import pandas as pd
from nose.tools import raises


from ..binge import B
from ..binge import _wrap_fct


def test_wrap_fct():
    a = 1
    b = 1
    res = _wrap_fct([_dummy_wrap, 1, True, 2] + [a] + [("b",b)])
    assert res == a+b


def _dummy(a):
    return a


def _dummy2(a, b=1):
    return a + b


def _dummy_wrap(a, b, _pinfo):
    return a + b


def _dummy_wait(a):
    time.sleep(1)
    return a


def _dummy_pd(x, col):
    return pd.DataFrame([{col: x}])


def test_dum_create():
    l = [1,2,3]
    res = B(_dummy)(l)
    assert l == res


def test_dum_create2():
    l = [1,2,3]
    res = B(_dummy)(a=l)
    assert l == res


def test_dum_create3():
    l = [1,2,3]
    res = B(_dummy2)(l)
    assert [i+1 for i in l] == res


def test_dum_create4():
    l = [1,2,3]
    b = 2
    res = B(_dummy2)(l, b=b)
    assert [i+b for i in l] == res


def test_dum_create5():
    l = [1,2,3]
    b = 2
    res = B(_dummy2)(a=l, b=b)
    assert [i+b for i in l] == res


def test_cores():
    l = [1,2,3,4]
    res = B(_dummy_wait, cores=1)(l)
    assert l == res


def test_timing_core():
    l = [1,2,3,4]
    t = time.time()
    res = B(_dummy_wait, cores=1)(l)
    timeonecore = time.time() - t
    t = time.time()
    res = B(_dummy_wait, cores=4)(l)
    timefourcore = time.time() - t
    assert abs(timeonecore/timefourcore - 4) / 4 < 0.1


def test_n():
    l = [1,2,3,4]
    res = B(_dummy, n=len(l))(l)
    assert l == res


def test_range():
    l = range(4)
    res = B(_dummy)(l)
    assert list(l) == res


def test_input_generator_nope():
    l = [1,2,3,4]
    res = B(_dummy)(x for x in l)
    assert l == list(res[0])


def test_input_generator():
    l = [1,2,3,4]
    res = B(_dummy, typin='gen')(x for x in l)
    assert l == res


def test_input_str_nope():
    l = 'hello'
    res = B(_dummy)(l)
    assert l == res[0]


def test_input_str():
    l = 'hello'
    res = B(_dummy, typin='str')(l)
    assert list(l) == res


def test_input_nda_nope():
    l = np.array([[1,2],[3,4]])
    res = B(_dummy)(l)
    assert np.alltrue(l == res[0])


def test_input_nda():
    l = np.array([[1,2],[3,4]])
    res = B(_dummy, typin='nda')(l)
    assert np.alltrue(l == np.vstack(res))


def test_input_str_gen():
    l = 'hello'
    res = B(_dummy, typin=['str','gen'])(l[i:] for i in range(2))
    assert [l, l[1:]] == res


def test_output_nd1():
    l = np.array([[1,2],[3,4]])
    res = B(_dummy, typin='nda', typout='nd1')(l)
    assert np.alltrue(l.ravel() == res)


def test_output_nda():
    l = np.array([[1,2],[3,4]])
    res = B(_dummy, typin='nda', typout='nda')(l)
    assert np.alltrue(l == res)


def test_output_pd():
    l = [0,1,2]
    col = 'col'
    res = B(_dummy_pd, typout='df')(l, col=col)
    assert l == res[col].values.tolist()


@raises(Exception)
def test_not_callable():
    B(23)(range(2))


@raises(ValueError)
def test_bad_typin():
    B(_dummy, typin=12)(range(2))


@raises(ValueError)
def test_bad_typin():
    B(_dummy, typin='blah')(range(2))


def test_bad_typout():
    l = [1,2]
    res = B(_dummy, typout='blah')(l)
    assert l == res


def test_error_typout_processing():
    l = [1,2]
    res = B(_dummy, typout='df')(l)
    assert l == res


def test_repr_str():
    Bf = B(_dummy)
    assert str(Bf) == repr(Bf)


def test_verbose():
    Bf = B(_dummy, verbose=True)(range(2))


def test_pinfo():
    l = [1,2,3]
    res = B(_dummy_wrap, fwp_pinfo)(l)
    assert l == res


def _dum_wrap(_pinfo):
    time.sleep(0.5)
    return _pinfo


def test_pinfo():
    res = B(_dum_wrap, fwd_pinfo=True, n=4, cores=4)()
    assert sorted([i[0] for i in res]) == [0,1,2,3]
    assert sorted([i[1] for i in res]) == [1,1,1,1]


def _dum_n(a, b):
    return sum(a)*b


def test_nulti_n():
    a = [1,2]
    b = [0,1,2,3]
    res = B(_dum_n)(a, b)
    assert res == [sum(a)*i for i in b]


def test_nulti_n():
    a = [0,1,2,3]
    b = [1,2]
    res = B(_dum_n, n=2)(a, b)
    assert res == [sum(a)*i for i in b]

