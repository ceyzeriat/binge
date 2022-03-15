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

from __future__ import annotations
import typing

if typing.TYPE_CHECKING:
    from typing import Callable, Set

import multiprocessing
import traceback
import types
from typing import Iterable


def _wrap_fct(params):
    n_threads = params.pop(3)
    pinfo = params.pop(2)
    largs = params.pop(1)
    fct = params.pop(0)
    if pinfo:
        d = multiprocessing.Process()._identity + (None,)
        params += [('_pinfo', [d[0] % n_threads, d[1]])]
    return fct(*params[:largs], **dict(params[largs:]))


_ALLOWED_TYPE_IN: Set = {'nda', 'str', 'gen'}
_ALLOWED_TYPE_OUT: Set = {'df', 'nd1', 'nda'}


class B(object):
    _font_blue: str = '\033[34m'
    _font_normal: str = '\033[39m\033[21m\033[22m'

    def __init__(self, fct: Callable,
                 threads: int or None = None,
                 n: int or None = None,
                 type_out: str or None = None,
                 type_in: str or None = None,
                 fwd_pinfo: bool = False,
                 verbose: bool = False
                 ):
        """
        Call M(f)(arg1, arg2) instead of f(arg1, arg2) to benefit
        from multi-processing

        Args:
          * fct (callable): the function to parallelize
          * threads (int or None): the number of threads on which to
            parallelize. None means use all CPUs
          * n (int or None): the size of the input on which to
            parallelize. None means infer from inputs
          * type_out (str or None): when not None, extra post-
            transformations will be carried on the input/output.
            See below
          * type_in (str or None or list of str): when not None, instructs
            how to automatically infer the size of the dimension to
            distribute on: n. Ignored is n is given.
            See below
          * fwd_pinfo (bool): if True, passes the process information
            (process_index, process_iteration_index) to the worker
            fct under parameter name '_pinfo'

        type_out:
          * df: the output will be concatenated into a single pandas df
          * nd1: the output will be a stack of the first dimension of
            the threads outputs.
            e.g. thread1: shape=(2,3), thread2: shape=(7,3)
                output will be of shape (9,3)
          * nda: the output will be a stack in a new first dimension of
            the threads outputs, which need to have all the same shape.
            e.g. thread1: shape=(7,3), thread2: shape=(7,3)
                output will be of shape=(2,7,3)

        type_in:
          * nda: any input of type numpy array will be distributed along
            its first dimension
          * str: any input of str- or bytes-like types will be
            distributed as if a list of single characters
          * gen: any input of generators types will be distributed

        Example:
          > def f(x, y=1., p=2.): return (x*y)**p
          Calling
          > M(f)([1,2], p=3)
          will call simultaneously on 2 different threads:
          > f(1, p=3)
          > f(2, p=3)
          and return:
          > [1.0, 8.0]
        """
        if not callable(fct):
            raise Exception("fct must be callable")
        self._fct: Callable = fct
        self.threads: int = multiprocessing.cpu_count() if threads is None else int(threads)
        # initial instruction
        self._n: int or None = int(n) if n is not None else None
        # real one
        self.n: int = self._n if self._n is not None else 1
        self.verbose: bool = bool(verbose)

        if type_in is None:
            self.type_in = None
        elif isinstance(type_in, str):
            self.type_in = {type_in.lower()}
        elif isinstance(type_in, (tuple, list)):
            self.type_in = {str(typ).lower() for typ in type_in}
        else:
            raise ValueError(f"type_in '{type_in}' not understood")

        if self.type_in is not None:
            for typ in self.type_in:
                if typ not in _ALLOWED_TYPE_IN:
                    raise ValueError(f"type_in '{typ}' not understood")
        self._split_str: bool = False if self.type_in is None else ('str' in self.type_in)
        self._split_ndarray: bool = False if self.type_in is None else ('nda' in self.type_in)
        self._split_gen: bool = False if self.type_in is None else ('gen' in self.type_in)

        ndarray = None
        if self._split_ndarray:
            # requested numpy input to be split, so allow fail if cannot import
            from numpy import ndarray
            self.ndarray = ndarray
        else:
            # did NOT requested numpy input to be split, so protect fail if cannot import
            try:
                from numpy import ndarray
            except ImportError:
                pass
        self.ndarray = ndarray
        self.type_out: str or None = str(type_out) if type_out is not None else None
        self._fwd_pinfo: bool = bool(fwd_pinfo)

    def _info(self) -> str:
        f_name = getattr(self._fct, 'func_name', self._fct.__name__)
        return f"Multi-processing wrapper for {self._font_blue}{f_name}{self._font_normal} over {self.threads} processes"

    def __repr__(self):
        return self._info()

    def __str__(self):
        return self._info()

    def _inspect_it(self, item) -> bool:
        if self.ndarray is None:
            # numpy could not be loaded, so at this point there should not be any numpy array as input, so we
            # inspect everything
            pass
        # not a numpy array input
        elif not isinstance(item, self.ndarray):
            # numpy was loaded but we don't have a ndarray, so we inspect this one
            pass
        else:
            # we have a numpy array so just do as per instruction
            return self._split_ndarray

        if isinstance(item, str):
            # and we have a string input so we go as per instruction
            return self._split_str
        elif isinstance(item, types.GeneratorType):
            # here is a generator.. to split or not to split? follow instruction
            return self._split_gen
        return isinstance(item, Iterable)

    def __call__(self, *args, **kwargs):
        # init number of multi-iteration
        params = [list(args), kwargs]
        # initial instruction was unknown, need to infer the n
        if self._n is None:
            self.n = 1
            # check all input arguments
            for p, param in [(0, enumerate(params[0])), (1, params[1].items())]:
                for idx, item in param:
                    # this input is inspect-compatible
                    if self._inspect_it(item):
                        if isinstance(item, types.GeneratorType):
                            # can't take len of generator, gotta force it into a tuple
                            item = tuple(item)
                            params[p][idx] = item
                        # keep the longest dimension to infer n
                        self.n = max(self.n, len(item))
                        if self.verbose:
                            print(f"Input index '{idx}' iterable with size {len(item)}, iterations is {self.n}")
                    elif self.verbose:
                        print(f"Input index '{idx}' skipped")
        if self.verbose:
            print(f"Will do {self.n:d} iterations\nWill use {self.threads:d} threads")

        # go through args and kwargs input arguments and make lists of it if need be
        for p, param in [(0, enumerate(params[0])), (1, params[1].items())]:
            for idx, item in param:
                if self._inspect_it(item):
                    # this input is inspect-compatible
                    if len(item) != self.n:
                        # but its length is not the one we want to iterate on wrap it into a fake 1-item list
                        params[p][idx] = [item]
                else:
                    # item not inspect-compatible
                    # wrap it into a fake 1-item list
                    if isinstance(item, types.GeneratorType):
                        # can't pickle generators, so gotta force it into tuple
                        item = tuple(item)
                    params[p][idx] = [item]

        # make the single parameter list for the pool
        all_params = []
        for j in range(self.n):
            temp = [self._fct, len(params[0]), self._fwd_pinfo, self.threads]
            for item in params[0]:
                temp.append(item[min(j, len(item) - 1)])
            for key, item in params[1].items():
                temp.append((key, item[min(j, len(item) - 1)]))
            all_params.append(temp)

        del params, temp

        with multiprocessing.Pool(processes=self.threads) as pool:
            mapped_pool = pool.map(_wrap_fct, all_params)
            # back to initial instruction
            self.n = self._n if self._n is not None else 1
            if self.type_out is None:
                return mapped_pool
            try:
                if self.type_out == 'df':
                    from pandas import DataFrame
                    from numpy import vstack
                    return DataFrame(vstack(mapped_pool), columns=mapped_pool[0].columns)
                elif self.type_out == 'nd1':
                    from numpy import concatenate
                    return concatenate(mapped_pool, axis=0)
                elif self.type_out == 'nda':
                    from numpy import concatenate
                    return concatenate([[item] for item in mapped_pool], axis=0)
                else:
                    raise Exception("Unkonwn typout '{}'".format(self.type_out))
            except:
                issue = traceback.format_exc()
                print("Some error happened trying to post-process the output " +
                      f"according to typout '{self.type_out}':\n{issue}\nReturned the raw output instead")
            return mapped_pool

def dum(a):
    return a+1

if __name__ == '__main__':
    print(B(dum)([1, 2]))
