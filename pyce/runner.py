'''
PyCE: Computational experiment management framework.

Command invocation module
Copyright 2010, Konstantin Tretyakov.
Licensed under the BSD (3-clause) license.
'''

from computation import DataObjectDescriptor

class ComputationRunnerInterface:
    '''
    Dysfunctional class which is here only to document the interface expected from a
    ComputationRunner instance.
    You can inherit from it, if you wish, but it's not necessary as long as you have the
    appropriate functions defined.
    '''
    def compute_target(self, scheme, target_data_object, computation):
        '''
        Given a ComputationScheme, a DataObjectDescriptor and a ComputationDescriptor,
        perform the prescribed computation required to build target_data_object.
        Return True if computation was successful and False otherwise.
        If compute_target returns False or throws an exception, the file
        scheme.target_filename(target_data_object) should be removed.
        '''
        print "Building %s as %s" % (target_data_object, computation)
        return True

    def describe_compute_target(self, scheme, target_data_object, computation):
        '''
        Return a string, describing the computation to be done.
        '''
        return "Build %s as %s" % (target_data_object, computation)

class ComputationRunner(ComputationRunnerInterface):
    '''
    Yet another dysfynctional class. Basically a renaming of the ComputationRunnerInteface.
    '''
    def compute_target(self, scheme, target_data_object, computation):
        ComputationRunnerInterface.compute_target(self, scheme, target_data_object, computation)

class PythonFunctionRunner(ComputationRunnerInterface):
    '''
    For a given computation of the form
        dataobject[etc].etc = do.something.here(param1,param2,...)
    does:
        import do.something
        do.something.here(param1,param2,..., _output = dataobject[etc].etc),
    where all dataobject identifiers are transformed into strings representing the corresponding filenames.
    The '_depend' keyword argument, if present, is removed from the argument list.
    Unless the name of the dataobject starts with _, the output filename is passed to the function
    via the _output parameter. Otherwise, the result of the function call is converted to
    string and written to the file.
    '''
    def compute_target(self, scheme, target_data_object, computation):
        print "Building %s as %s" % (target_data_object, computation)
        [target_function, args, kwargs, save_result_to_file, output_warning] = self.resolve_compute_target(scheme, target_data_object, computation)
        if (target_function is None):
            raise Exception("Could not import requested function/package")
        result = target_function(*args, **kwargs)
        if save_result_to_file is not None:
            with open(save_result_to_file, 'w') as outfile:
                outfile.write(str(result))
        return True

    def describe_compute_target(self, scheme, target_data_object, computation):
        [target_function, args, kwargs, save_result_to_file, output_warning] = self.resolve_compute_target(scheme, target_data_object, computation)
        args = map(repr, args)
        kwargs = map(lambda (x,y): '%s=%s' % (str(x), repr(y)), kwargs.iteritems())
        result = '%s(%s)' % (computation.name, ', '.join(args + kwargs))
        if save_result_to_file is not None:
            result = result +'\nOUTPUT SAVED TO: %s' % save_result_to_file
        if target_function is None:
            result = result + '\nERROR: Function %s could not be resolved!' % computation.name
        if output_warning:
            result = result +'\nWARNING: Parameter _output is replaced from its original value!'
        return result

    @staticmethod
    def resolve_compute_target(scheme, target_data_object, computation):
        '''
        Given a target data object and a computation to be performed, finds a function, and a set of parameters
        required to compute it. Used both by compute_target and describe_compute_target.
        Returns a tuple:
        [target_function, args, kwargs, save_result_to_file, output_warning]
        * If target_function can't be resolved, it is returned as None and an exception is *printed* onto stdout.
        * save_result_to_file is None if nothing has to be saved, and a name of the file to save function output to otherwise.
        * output_warning is True if the invocation had originally specified an _output parameter which will be
        replaced on actual invocation.
        '''
        output_warning = False
        try:
            target_function = PythonFunctionRunner.import_function(computation.name)
        except:
            import traceback
            traceback.print_exc()
            target_function = None
        kwargs = computation.kwargs if computation.kwargs is not None else {}
        args   = computation.args   if computation.args   is not None else []

        # if target_data_object's name starts with _, we do not pass the
        # _output parameter to the function. Instead, we manually create the corresponding file.
        have_output_param = not str(target_data_object).startswith('_')

        if '_depend' in kwargs:
            del kwargs['_depend']
        if ('_output' in kwargs) and have_output_param:
            output_warning = True
        if have_output_param:
            kwargs['_output'] = scheme.target_filename(target_data_object)
            save_result_to_file = None
        else:
            save_result_to_file = scheme.target_filename(target_data_object)

        kwargs = PythonFunctionRunner.replace_targets_with_filenames(scheme, kwargs)
        args   = PythonFunctionRunner.replace_targets_with_filenames(scheme, args)
        return [target_function, args, kwargs, save_result_to_file, output_warning]

    # TODO: May get stuck in an infinite loop when given bad input with self-recursion.
    @staticmethod
    def replace_targets_with_filenames(scheme, obj):
        '''
        If the given object is a DataObjectDescriptor, replaces it with its filename.
        If the given object is a list or a dict, descends recursively.
        Otherwise returns the object without changes.
        '''
        if obj is None:
            return None
        elif isinstance(obj, DataObjectDescriptor):
            return scheme.target_filename(obj)
        elif isinstance(obj, list) or isinstance(obj, tuple):
            return map(lambda x: PythonFunctionRunner.replace_targets_with_filenames(scheme, x), obj)
        elif isinstance(obj, dict):
            for k in obj:
                obj[k] = PythonFunctionRunner.replace_targets_with_filenames(scheme, obj[k])
            return obj
        else:
            return obj

    @staticmethod
    def import_function(name):
        '''
        Given an absolute name of a function (e.g. 'os.path.abspath') imports the function from the package
        and returns it. Throws an exception on failure.
        '''
        path = name.split('.')
        if len(path) == 1:
            f = globals()[path[0]]
        else:
            mod = '.'.join(path[0:-1])
            m = __import__(mod, fromlist=mod)
            f = getattr(m, path[-1])
        return f

SYSTEM_RUNNER = ComputationRunner()
PYTHON_RUNNER = PythonFunctionRunner()
