binge
=====

Lazy multi-process your callables in three extra characters

Built by `Guillaume Schworer <https://github.com/ceyzeriat>`_. Licensed under
the GNU General Public License v3 or later (GPLv3+) license (see ``LICENSE``).


Installation
------------

Just run

::

    pip install binge

to get the most recent stable version.


Usage
-----

The only entry point is the ``binge.B`` classe. You'll just use it like this:

::

    import time
    from binge import B

    def f(x, a=2):
        time.sleep(1)
        return x*a

    # normal call
    [f(x) for x in range(4)]
    > [0, 2, 4, 6]  # takes 4 seconds to run
    
    # binged call
    B(f)([x for x in range(4)])
    > [0, 2, 4, 6]  # takes 1 second to run on 4 CPUs
    
    # and if you're very lazy, you could even do
    B(f)(range(4))
    
    # and what about the ``a`` parameter? too easy
    B(f)(range(4), 3)
    > [0, 3, 6, 9]
    
    # and guess what also works?
    B(f)(range(4), a=[1,2,3,6])
    > [0, 2, 6, 18]


More usage details, see `example.py
<https://github.com/ceyzeriat/binge/blob/master/example.py>`_)


Documentation
-------------

All the options are documented in the docstrings for the ``B`` classes.
These can be viewed in a Python shell using:

::

    from binge import B
    print(B.__doc__)

or, in IPython using:

::

    from binge import B
    B?



License
-------

Copyright 2018 Guillaume Schworer

patiencebar is free software made available under the GNU General
Public License v3 or later (GPLv3+) license (see ``LICENSE``).
