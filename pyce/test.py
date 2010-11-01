'''
PyCE: Computational experiment management framework.

Some manual testing
Copyright 2010, Konstantin Tretyakov.
Licensed under the BSD (3-clause) license.
'''

import os, sys
from computation import *
from runner import *
from util import *

def testfunc(*args, **kw):
    print 'Testfunc: ' + str(args) + ':' + str(kw)

def file(name,**kw):
    print 'ah, %s' % name

CACHEDIR = 'testdata'
if not os.path.exists(CACHEDIR):
    os.mkdir(CACHEDIR)

@pycex_experiment(cache_dir = CACHEDIR, version="1.1", runner=PYTHON_RUNNER)
def do_experiment(scheme):
    saved, run = scheme.data_object, scheme.computation
    saved._result[-1] = run.pycex.test.file('name')
    saved._result[0] = run.pycex.test.testfunc(saved._result[-1], 'Something')
    for i in range(1,10):
        saved._result[i] = run.compute_something(saved._result[i-1], i)
    saved._upload_result = run.upload_result(saved._result[9])
    scheme.set_main_target(saved._upload_result)

if __name__ == "__main__":
    do_experiment()