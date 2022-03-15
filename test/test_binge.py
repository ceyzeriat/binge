from unittest import TestCase
import numpy as np
import time
import pandas as pd

from binge import B
from binge.binge import _wrap_fct


def dummy_wrap(a, b, _pinfo):
    return a + b


def dum_wrap(_pinfo):
    time.sleep(0.5)
    return _pinfo


def dum_n(a, b):
    return sum(a)*b


def dummy(a):
    return a


def dummy2(a, b=1):
    return a + b


def dummy_wait(a):
    time.sleep(1)
    return a


def dummy_pd(x, col):
    return pd.DataFrame([{col: x}])


class TestPlay(TestCase):
    
    def test_wrap_fct(self):
        a = 1
        b = 1
        res = _wrap_fct([dummy_wrap, 1, True, 2] + [a] + [("b", b)])
        self.assertEqual(res, a + b)
    
    def test_dum_create(self):
        li = [1, 2, 3]
        res = B(dummy)(li)
        self.assertEqual(li, res)
    
    def test_dum_create2(self):
        li = [1, 2, 3]
        res = B(dummy)(a=li)
        self.assertEqual(li, res)
    
    def test_dum_create3(self):
        li = [1, 2, 3]
        res = B(dummy2)(li)
        self.assertEqual([i+1 for i in li], res)
    
    def test_dum_create4(self):
        li = [1, 2, 3]
        b = 2
        res = B(dummy2)(li, b=b)
        self.assertEqual([i+b for i in li], res)
    
    def test_dum_create5(self):
        li = [1, 2, 3]
        b = 2
        res = B(dummy2)(a=li, b=b)
        self.assertEqual([i+b for i in li], res)
    
    def test_threads(self):
        li = [1, 2, 3, 4]
        res = B(dummy_wait, threads=1)(li)
        self.assertEqual(li, res)
    
    def test_timing_thread(self):
        li = [1, 2, 3, 4]
        t = time.time()
        B(dummy_wait, threads=1)(li)
        time_one_thread = time.time() - t
        t = time.time()
        B(dummy_wait, threads=4)(li)
        time_four_threads = time.time() - t
        self.assertTrue(abs(time_one_thread/time_four_threads - 4) / 4 < 0.5)
    
    def test_n(self):
        li = [1,2,3,4]
        res = B(dummy, n=len(li))(li)
        self.assertEqual(li, res)
    
    def test_range(self):
        li = range(4)
        res = B(dummy)(li)
        self.assertEqual(list(li), res)
    
    def test_input_generator_nope(self):
        li = [1, 2, 3, 4]
        res = B(dummy)(x for x in li)
        self.assertEqual(li, list(res[0]))
    
    def test_input_generator(self):
        li = [1, 2, 3, 4]
        res = B(dummy, type_in='gen')(x for x in li)
        self.assertEqual(li, res)
    
    def test_input_str_nope(self):
        li = 'hello'
        res = B(dummy)(li)
        self.assertEqual(li, res[0])
    
    def test_input_str(self):
        li = 'hello'
        res = B(dummy, type_in='str')(li)
        self.assertEqual(list(li), res)
    
    def test_input_nda_nope(self):
        li = np.array([[1, 2], [3, 4]])
        res = B(dummy)(li)
        self.assertTrue(np.alltrue(li == res[0]))
    
    def test_input_nda(self):
        li = np.array([[1, 2], [3, 4]])
        res = B(dummy, type_in='nda')(li)
        self.assertTrue(np.alltrue(li == np.vstack(res)))
    
    def test_input_str_gen(self):
        li = 'hello'
        res = B(dummy, type_in=['str', 'gen'])(li[i:] for i in range(2))
        self.assertEqual([li, li[1:]], res)
    
    def test_output_nd1(self):
        li = np.array([[1, 2], [3, 4]])
        res = B(dummy, type_in='nda', type_out='nd1')(li)
        self.assertTrue(np.alltrue(li.ravel() == res))
    
    def test_output_nda(self):
        li = np.array([[1, 2], [3, 4]])
        res = B(dummy, type_in='nda', type_out='nda')(li)
        self.assertTrue(np.alltrue(li == res))
    
    def test_output_pd(self):
        li = [0, 1, 2]
        col = 'col'
        res = B(dummy_pd, type_out='df')(li, col=col)
        self.assertEqual(li, res[col].values.tolist())
    
    def test_not_callable(self):
        with self.assertRaises(Exception):
            B(23)(range(2))
    
    def test_bad_type_in(self):
        with self.assertRaises(ValueError):
            B(dummy, type_in=12)(range(2))
    
    def test_bad_type_in2(self):
        with self.assertRaises(ValueError):
            B(dummy, type_in='blah')(range(2))
    
    def test_bad_type_out(self):
        li = [1, 2]
        res = B(dummy, type_out='blah')(li)
        self.assertEqual(li, res)
    
    def test_error_type_out_processing(self):
        li = [1, 2]
        res = B(dummy, type_out='df')(li)
        self.assertEqual(li, res)
    
    def test_repr_str(self):
        bf = B(dummy)
        self.assertEqual(str(bf), repr(bf))
    
    def test_verbose(self):
        B(dummy, verbose=True)(range(2))
    
    def test_pinfo(self):
        li = [1, 2, 3]
        res = B(dummy_wrap, fwd_pinfo=True)(li, b=0)
        self.assertEqual(li, res)
    
    def test_pinfo_2(self):
        res = B(dum_wrap, fwd_pinfo=True, n=4, threads=4)()
        assert sorted([i[0] for i in res]) == [0, 1, 2, 3]
        assert sorted([i[1] for i in res]) == [1, 1, 1, 1]
    
    def test_nulti_n(self):
        a = [1, 2]
        b = [0, 1, 2, 3]
        res = B(dum_n)(a, b)
        self.assertEqual(res, [sum(a)*i for i in b])
    
    def test_nulti_n2(self):
        a = [0, 1, 2, 3]
        b = [1, 2]
        res = B(dum_n, n=2)(a, b)
        self.assertEqual(res, [sum(a)*i for i in b])
