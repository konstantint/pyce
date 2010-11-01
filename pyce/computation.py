'''
PyCE: Computational experiment management framework.

Computation definition framework
Copyright 2010, Konstantin Tretyakov.
Licensed under the BSD (3-clause) license.
'''

'''
Example:
    scheme = ComputationScheme(cache_dir = './datafiles')
    saved, run = scheme.data_object, scheme.computation

    ...
    for i in range(10):
        saved.data[i] = run.some_function(i,2,3)
        saved.data2[i] = run.some_other_function(saved.data[i])
    saved.result = run.third_function(saved.data2[10])

    # Now the object "scheme" knows about the required computations as well as all
    # dependencies among the "saved.*" objects
'''

from urllib import quote
import os, sys

class ComputationScheme:
    def __init__(self, cache_dir = '.'):
        '''
        cache_dir - directory for storing computed files
        '''
        self.all_invocations = []
        self.invocation_idx = dict()
        self.dependency_graph = dict()
        self.cache_dir = cache_dir
        self.data_object = DataObjectDescriptor(scheme=self)
        self.computation = ComputationDescriptor()
        self.main_target = None

    def target_exists(self, target_str):
        return target_str in self.invocation_idx

    def add_invocation(self, data_object, computation):
        target = str(data_object)
        if self.dependency_graph.get(target, None) is not None:
            raise Exception("Target %s has multiple specifications" % target)

        if not isinstance(computation, ComputationDescriptor):
            computation = ComputationDescriptor(name='copy', args=[computation])

        self.all_invocations.append( (data_object, computation) )
        deps = map(str, computation.dependencies())

        self.dependency_graph[target] = deps
        self.invocation_idx[target] = len(self.all_invocations) - 1

    def set_main_target(self, data_object):
        self.main_target = data_object

    def find_invocation_for_target(self, target_str):
        i = self.invocation_idx.get(target_str, None)
        return None if i is None else self.all_invocations[i]

    #TODO: This stuff is prone to race conditions
    def find_next_step_to(self, target_str):
        # Is it locked?
        if self.is_locked(target_str):
            return None

        # Is there a dependency record (if there were now target=... line, there would not be one)
        deps = self.dependency_graph.get(target_str, None)
        if deps is None:
            return None

        # Are there undone and unlocked dependencies?
        all_done = True
        for k in deps:
            if self.is_locked(k):
                all_done = False
            elif not self.is_done(k):
                nxt = self.find_next_step_to(k)
                if nxt is not None:
                    return nxt
                else:
                    all_done = False

        if all_done:
            return target_str
        else:
            return None

    def save_makefile(self, compute_command, ostream=sys.stdout):
        '''
        compute_command - command that takes '[targetname]' as an argument and invokes the computation. Typically
        this would be something like "python this_script.py compute"
        '''
        if self.main_target is not None:
            print >>ostream, "%s:" % self.target_filename(self.main_target)
        for (lhs, rhs) in self.all_invocations:
            print "%s: %s" % (self.target_filename(lhs), " ".join(map(self.target_filename, rhs.dependencies())))
            print "\t%s \"%s\"" % (compute_command, str(lhs).replace('"', '\\"').replace('$', '\\$'))

    def target_filename(self, obj):
        return os.path.join(self.cache_dir, quote(str(obj)))

    def is_locked(self, target_str):
        return os.path.exists(self.target_filename(target_str) + ".locked")

    def is_done(self, target_str):
        return not self.is_locked(target_str) and os.path.exists(self.target_filename(target_str))

    def lock_target(self, target_str):
        if self.is_locked(target_str):
            return False
        else:
            f = open(self.target_filename(target_str) + ".locked", "w")
            f.write(str(os.getpid()))
            f.close()
            return True

    def remove_target(self, target_str):
        if os.path.exists(self.target_filename(target_str)):
            os.unlink(self.target_filename(target_str))

    def unlock_target(self, target_str):
        if os.path.exists(self.target_filename(target_str) + ".locked"):
            os.unlink(self.target_filename(target_str) + ".locked")


class ComputationDescriptor:
    def __init__(self, name=None, args=None, kwargs=None):
        self.name = name
        self.args = args
        self.kwargs = kwargs
    def __getattr__(self, name):
        newname = name if self.name is None else self.name + '.' + name
        return ComputationDescriptor(newname)
    def __call__(self, *args, **kw):
        return ComputationDescriptor(self.name, args, kw)
    def __repr__(self):
        return self.__str__()
    def __str__(self):
        if self.name is None:
            return "--"
        s = self.name + "("
        if self.args is not None:
            s = s + ",".join(map(str, self.args))
            if self.kwargs is not None and len(self.kwargs) > 0 and len(self.args) > 0:
                s = s + ","
        if self.kwargs is not None:
            s = s + ",".join(map(lambda (x,y): "%s=%s" % (str(x),str(y)), self.kwargs.iteritems()))
        s = s + ")"
        return s
    def dependencies(self):
        """Returns a list of parameters which are of type DataObjectDescritor"""
        result = ComputationDescriptor.extract_data_objects(self.args)
        if self.kwargs is not None:
            result = result + ComputationDescriptor.extract_data_objects(self.kwargs.values())
        return result

    @staticmethod
    def extract_data_objects(lst):
        """Given a list of elements extracts those of type DataObjectDescriptor. Descends recursively into sublists. When given None, returns []."""
        res = []
        if lst is None:
            return res
        for el in lst:
            if isinstance(el, DataObjectDescriptor):
                res.append(el)
            elif isinstance(el, list) or isinstance(el, tuple):
                res = res + ComputationDescriptor.extract_data_objects(el)
        return res


class DataObjectDescriptor:
    def __init__(self, name=None, idx=None, scheme=None):
        self.__dict__['_name'] = name  # Avoid self._name as it will invoke setattr here
        self.__dict__['_idx'] = idx
        self.__dict__['_scheme'] = scheme
    def __getattr__(self, name):
        return DataObjectDescriptor(name, scheme = self._scheme)
    def __setattr__(self, name, value):
        newobject = DataObjectDescriptor(name)
        self._scheme.add_invocation(newobject, value)
    def __getitem__(self, idx):
        return DataObjectDescriptor(self._name, idx, self._scheme)
    def __setitem__(self, idx, value):
        newobject = DataObjectDescriptor(self._name, idx)
        self._scheme.add_invocation(newobject, value)
    def __repr__(self):
        return self.__str__()
    def __str__(self):
        if self._name is None:
            return "--"
        s = self._name
        if self._idx is not None:
            s = s + "[%s]" % str(self._idx)
        return s
