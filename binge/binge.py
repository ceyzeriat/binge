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


import multiprocessing
from collections import Iterable
import traceback
import types


# cross-compatible python2-3 string checking
try:
    # PY2
    _STRINGLIKE = (basestring,unicode,str,bytes)
except:
    # PY3
    _STRINGLIKE = (str,bytes)


__all__ = ['B']


def _wrap_fct(params):
    ncores = params.pop(3)
    pinfo = params.pop(2)
    largs = params.pop(1)
    fct = params.pop(0)
    if pinfo:
        d = multiprocessing.Process()._identity + (None,)
        params += [('_pinfo', [d[0]%ncores, d[1]])]
    return fct(*params[:largs], **dict(params[largs:]))


_TYPIN = set(['nda', 'str', 'gen'])
_TYPOUT = set(['df', 'nd1', 'nda'])


class B(object):
    _fontblue = '\033[34m'
    _fontnormal = '\033[39m\033[21m\033[22m'

    def __init__(self, fct, cores=None, n=None, typout=None,
                 typin=None, fwd_pinfo=False, verbose=False):
        """
        Call M(f)(arg1, arg2) instead of f(arg1, arg2) to benefit
        from multi-processing

        Args:
          * fct (callable): the function to parallelize
          * cores (int or None): the number of cores on which to
            parallelize. None means use all CPUs
          * n (int or None): the size of the input on which to
            parallelize. None means infer from inputs
          * typout (str or None): when not None, extra post-
            transformations will be carried on the input/output.
            See below
          * typin (str or None or list of str): when not None, instructs
            how to automatically infer the size of the dimension to
            distribute on: n. Ignored is n is given.
            See below
          * fwd_pinfo (bool): if True, passes the process information
            (process_index, process_iteration_index) to the worker
            fct under parameter name '_pinfo'

        Typout:
          * df: the output will be contatenated into a single pandas df
          * nd1: the output will be a stack of the first dimension of
            the threads outputs.
            e.g. thread1: shape=(2,3), thread2: shape=(7,3)
                output will be of shape (9,3)
          * nda: the output will be a stack in a new first dimension of
            the threads outputs, which need to have all the same shape.
            e.g. thread1: shape=(7,3), thread2: shape=(7,3)
                output will be of shape=(2,7,3)
          
        Typin:
          * nda: any input of type numpy array will be distributed along
            its first dimension
          * str: any input of str- or bytes-like types will be
            distributed as if a list of single characters
          * gen: any input of genertors types will be distributed

        Example:
          > def f(x, y=1., p=2.): return (x*y)**p
          Calling
          > M(f)([1,2], p=3)
          will call simultaneously on 2 different cores:
          > f(1, p=3)
          > f(2, p=3)
          and return:
          > [1.0, 8.0]
        """
        if not callable(fct):
            raise Exception("fct must be callable")
        self._fct = fct
        self.cores = multiprocessing.cpu_count() if cores is None\
            else int(cores)
        # initial instruction
        self._n = int(n) if n is not None else None
        # real one
        self.n = self._n if self._n is not None else 1
        self.verbose = bool(verbose)
        ndarray = None
        if typin is None:
            self.typin = None
        elif isinstance(typin, str):
            self.typin = set([typin.lower()])
        elif isinstance(typin, (tuple, list)):
            self.typin = set([str(typ).lower() for typ in typin])
        else:
            raise ValueError("Typin '{}' not understood".format(typin))
        if self.typin is not None:
            for typ in self.typin:
                if typ not in _TYPIN:
                    raise ValueError("Typin '{}' not understood".format(typ))
        self._split_str = False if self.typin is None else\
            ('str' in self.typin)
        self._split_ndarray = False if self.typin is None else\
            ('nda' in self.typin)
        self._split_gen = False if self.typin is None else\
            ('gen' in self.typin)
        if self._split_ndarray:
            # requested numpy input to be split, so allow fail
            # if cant import
            from numpy import ndarray
        else:
            # did NOT requested numpy input to be split, so protect fail
            # if cant import
            try:
                from numpy import ndarray
            except ImportError:
                pass
        self.ndarray = ndarray
        self.typout = str(typout) if typout is not None else None
        self._fwd_pinfo = bool(fwd_pinfo)

    def _info(self):
        return "Multi-processing wrapper for {}{}{} over {} processes".\
            format(self._fontblue,
                   getattr(self._fct, 'func_name', self._fct.__name__),
                   self._fontnormal,
                   self.cores)

    def __repr__(self):
        return self._info()

    def __str__(self):
        return self._info()

    def _inspect_it(self, item):
        if self.ndarray is None:
            # numpy could not be loaded, so at this point there
            # should not be any numpy array as input, so we
            # inspect everything
            pass
            # not a numpy array input
        elif not isinstance(item, self.ndarray):
            # numpy was loaded but we don't have a ndarray, so we
            # inspect this one
            pass
        else:
            # we have a numpy array so just do as per instruction
            return self._split_ndarray
        if isinstance(item, _STRINGLIKE):
            # and we have a string input so we go as per
            # instruction
            return self._split_str
        elif isinstance(item, types.GeneratorType):
            # here is a generator.. to split or not to split?
            # follow instruction
            return self._split_gen
        else:
            return isinstance(item, Iterable)

    def __call__(self, *args, **kwargs):
        # init number of multi-iteration
        # initial instruction was unknown
        params = [list(args), kwargs]
        if self._n is None:
            self.n = 1
            # check all input arguments
            for p, param in [(0, enumerate(params[0])),
                             (1, params[1].items())]:
                for idx, item in param:
                    if self._inspect_it(item):
                        if isinstance(item, types.GeneratorType):
                            # can't take len of generator, gotta force it
                            # into a tuple
                            item = tuple(item)
                            params[p][idx] = item
                        # keep the longest dimension to infer n 
                        self.n = max(self.n, len(item))
                        if self.verbose:
                            print("Input index '{}' iterable with size"\
                                  " {}, iterations is {}"\
                                    .format(idx, len(item), self.n))
                    elif self.verbose:
                        print("Input index '{}' skipped".format(idx))
        if self.verbose:
            print("Will do {:d} iterations\nWill use {:d} cores"\
                .format(self.n, self.cores))
        # go through args and kwargs input arguments and make lists of it if
        # need be
        for p, param in [(0, enumerate(params[0])),
                         (1, params[1].items())]:
            for idx, item in param:
                if self._inspect_it(item):
                    # this input is inspect-compatible
                    if len(item) != self.n:
                        # but its length is not the one we want to iterate on
                        # wrap it into a fake 1-item list
                        params[p][idx] = [item]
                else:
                    # item not inspect-compatible
                    # wrap it into a fake 1-item list
                    if isinstance(item, types.GeneratorType):
                        # can't pickle generators, so gotta force it into
                        # tuple
                        item = tuple(item)
                    params[p][idx] = [item]
        # make the single parameter list for the pool
        allparams = []
        for j in range(self.n):
            temp = [self._fct, len(params[0]), self._fwd_pinfo, self.cores]
            for item in params[0]:
                temp.append(item[min(j, len(item)-1)])
            for key, item in params[1].items():
                temp.append((key, item[min(j, len(item)-1)]))
            allparams.append(temp)
        del params, temp
        #allparams =\
        #    ([self._fct, len(params[0]), self._fwd_pinfo, self.cores]
        #        + [item[min(j, len(item)-1)] for item in params[0]]
        #        + [(key, item[min(j, len(item)-1)])
        #            for key, item in params[1].items()]
        #    for j in range(self.n))
        pool = multiprocessing.Pool(processes=self.cores)
        res = pool.map(_wrap_fct, allparams)
        # back to initial instruction
        self.n = self._n if self._n is not None else 1
        if self.typout is None:
            return res
        try:
            if self.typout == 'df':
                from pandas import DataFrame
                from numpy import vstack
                return DataFrame(vstack(res), columns=res[0].columns)
            elif self.typout == 'nd1':
                from numpy import concatenate
                return concatenate(res, axis=0)
            elif self.typout == 'nda':
                from numpy import concatenate
                return concatenate([[item] for item in res], axis=0)
            else:
                raise Exception("Unkonwn typout '{}'".format(self.typout))
        except:
            bug = traceback.format_exc()
            print("Some error happened trying to post-process the output "\
                  "according to typout '{}':\n{}\n"\
                  "Returned the raw output instead".format(
                    self.typout, bug))
        return res
