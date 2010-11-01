'''
PyCE: Computational experiment management framework.

Utility functions
Copyright 2010, Konstantin Tretyakov.
Licensed under the BSD (3-clause) license.
'''

import sys, optparse, traceback
from runner import *
from computation import *

# -------------- Utility functions -----------------
def print_dependency_list(scheme, target_str):
    cur = [(target_str,0)]
    while len(cur) > 0:
        (c, l) = cur.pop()
        print "%s%s" % (" "*l, c)
        deps = scheme.dependency_graph.get(c, None)
        if deps is None:
            print "%s%s" % (" "*(l+1), "??")
        else:
            for k in scheme.dependency_graph[c]:
                cur.append( (k, l+1) )

def print_dependencytodo_list(scheme, target_str):
    targets_mentioned = set()
    cur = [(target_str,0)]
    while len(cur) > 0:
        (c, l) = cur.pop()
        if c in targets_mentioned:
            continue
        targets_mentioned.add(c)

        if scheme.is_locked(c):
            stat = "LOCKED"
        elif scheme.is_done(c):
            stat = "DONE  "
        else:
            stat = "      "
        print "%s\t%s%s" % (stat," "*l, c)
        deps = scheme.dependency_graph.get(c, None)
        if deps is None:
            print "No deps for %s" % c
            print "%s%s" % (" "*(l+1), "??")
        else:
            for k in scheme.dependency_graph[c]:
                cur.append( (k, l+1) )

def print_files_list(scheme):
    for (a, b) in scheme.all_invocations:
        print scheme.target_filename(a)

def print_target_list(scheme):
    for (a, b) in scheme.all_invocations:
        print str(a)

def print_target_list_with_stats(scheme):
    total = locked = done = not_done = 0
    for (a, b) in scheme.all_invocations:
        stat = "      "
        target_str = str(a)
        total = total + 1
        if scheme.is_locked(target_str):
            stat = "LOCKED"
            locked = locked + 1
        elif scheme.is_done(target_str):
            stat = "DONE  "
            done = done + 1
        else:
            not_done = not_done + 1
        print "%s\t%s" % (stat, str(a)) #, scheme.target_filename(str(a)))
    print "----------------"
    print "Not done: %d" % not_done
    print "Locked:    %d" % locked
    print "Done:     %d" % done
    print "Total targets: %d" % total

def print_target_filename(scheme, target_name):
    if not scheme.target_exists(target_name):
        print "ERROR: Target not found"
    else:
        print scheme.target_filename(target_name)

# -------------- Function for invoking the computation of a given target -----------------
# Possible return values for the next two functions
STEP_RUN_OK = 0
STEP_RUN_FAILED = 1
STEP_RUN_FAILED_WITH_EXCEPTION = 2
TARGET_NOT_FOUND = 3
TARGET_LOCKED = 4
TARGET_READY = 5
NO_STEPS_AVAILABLE = 6
COMPUTE_TARGET_RESULT_MSG = ["Step successful", "Step run failed", \
                       "Step run failed with exception", "Target not found",\
                       "Target locked", "Target ready, nothing to be done",\
                       "No steps available"]
def compute_target(scheme, computation_runner, target_str):
    '''
    Invokes the computation registered for creating a given target.
    The computation is invoked even if the target file already exists.
    The computation is NOT invoked, if the target is locked, though.
    The target is locked for the duration of the computation.
    '''
    if not scheme.target_exists(target_str):
        return TARGET_NOT_FOUND
    if scheme.is_locked(target_str):
        return TARGET_LOCKED

    scheme.lock_target(target_str)
    try:
        print "Computing target: %s..." % target_str
        sys.stdout.flush()
        (obj, comp) = scheme.find_invocation_for_target(target_str)
        if computation_runner.compute_target(scheme, obj, comp):
            result = STEP_RUN_OK
            print "Target successful: %s" % target_str
        else:
            result = STEP_RUN_FAILED
            print "Target failed: %s" % target_str
            scheme.remove_target(target_str)
        sys.stdout.flush()
    except:
        traceback.print_exc()
        result = STEP_RUN_FAILED_WITH_EXCEPTION
        print "Target failed with exception: %s" % target_str
        sys.stdout.flush()
        scheme.remove_target(target_str)
    finally:
        scheme.unlock_target(target_str)
    return result

def view_compute_target(scheme, computation_runner, target_str):
    # Is there such a target at all?
    if not scheme.target_exists(target_str):
        print "Target %s does not exist" % target_str
        return TARGET_NOT_FOUND
    (obj, comp) = scheme.find_invocation_for_target(target_str)
    result = computation_runner.describe_compute_target(scheme, obj, comp)
    print "Target: %s\nBuild spec: %s" % (target_str, result)
    if scheme.is_locked(target_str):
        print "WARNING: Target is locked!"
    return STEP_RUN_OK

# -------------- Main computing function -----------------

def do_step_to_target(scheme, computation_runner, final_target_str):
    # Is there such a target at all?
    if not scheme.target_exists(final_target_str):
        return TARGET_NOT_FOUND
    # Is target ready?
    if scheme.is_done(final_target_str):
        return TARGET_READY
    # Is there some next step to do?
    next_target = scheme.find_next_step_to(final_target_str)
    if next_target is None:
        return NO_STEPS_AVAILABLE
    # Else, lock target and invoke the step
    return compute_target(scheme, computation_runner, next_target)

def view_step_to_target(scheme, computation_runner, final_target_str):
    # Is there such a target at all?
    if not scheme.target_exists(final_target_str):
        print "Target %s does not exist" % final_target_str
        return TARGET_NOT_FOUND
    # Is target ready?
    if scheme.is_done(final_target_str):
        print "Target %s is already done. Nothing to be made." % final_target_str
        return TARGET_READY
    # Is there some next step to do?
    next_target = scheme.find_next_step_to(final_target_str)
    if next_target is None:
        print "No next steps are available either because all matching targets " + \
              "are locked and being built or because one of the intermediate steps is not specified."
        return NO_STEPS_AVAILABLE

    (obj, comp) = scheme.find_invocation_for_target(next_target)
    result = computation_runner.describe_compute_target(scheme, obj, comp)
    print "Next target: %s\nBuild spec: %s" % (next_target, result)
    return STEP_RUN_OK

# ----------------- Standard cmdarg parser ----------------------- #
def pycex_parse_cmdline(version="1.0"):
    USAGE = "%prog <action> [param]"
    VERSION = "%%prog version %s" % (version)
    SYNOPSIS = """Manages the computations of the experiment. The <action> parameter can take one of the following values:

    * targetfile [target]
        shows the name of the file containing a given target,

    * dependency [target]
        show dependency tree of a given target,

    * dependencystat [target]
        show an abridged dependency tree with stats,

    * stepto [target]
        performs the next single computation needed to reach target,

    * viewstepto [target]
        just tells what would be the next computation to do,

    * compute [target]
        invokes the computation assigned to build target,

    * viewcompute [target]
        just tells what is the computation needed to build target,

    * makefile [python-command]
        outputs a Makefile script that can be used with -j<n> for parallel building.
        The [python-command] argument must be a command you use to invoke this script.
        For example: "python script.py". The whole command would then be something like
        $ python script.py makefile "python script.py",

    * list
        lists all targets,

    * stat
        lists all targets with information about them being ready or locked,

    * listfiles
        lists all output files correspondign to the results.
    """

    parser = optparse.OptionParser(usage=USAGE + "\n\n" + SYNOPSIS, version=VERSION, description="", formatter=optparse.TitledHelpFormatter())
    (options, args) = parser.parse_args()
    if len(args) == 0:
        parser.print_help()
        sys.exit(2)
    elif args[0] in ["dependency", "dependencystat", "stepto", "viewstepto", "viewcompute", "targetfile", "compute", "makefile"]:
        if len(args) < 2:
            parser.error("Parameter expected")
        elif len(args) > 2:
            parser.error("Too many parameters")
    elif args[0] in ["list", "stat", "listfiles"]:
        if len(args) != 1:
            parser.error("Too many arguments")
    else:
        parser.error("Invalid arguments")

    return (options, args)

# ----------------- Standard 'main' ----------------------- #
# Usage: replace "def main(): ..." with main = pycex_default_main(scheme, runner)
def pycex_default_main(scheme, runner, version="1.0"):
    def main():
        (options, args) = pycex_parse_cmdline(version)
        arg = args[0]
        if arg == "makefile":
            scheme.save_makefile(args[1] + " compute")
        elif arg == "compute":
            result = compute_target(scheme, runner, args[1])
            print "Result: " + COMPUTE_TARGET_RESULT_MSG[result]
        elif arg == "viewcompute":
            result = view_compute_target(scheme, runner, args[1])
        elif arg == "list":
            print_target_list(scheme)
        elif arg == "stat":
            print_target_list_with_stats(scheme)
        elif arg == "listfiles":
            print_files_list(scheme)
        elif arg == "dependency":
            print_dependency_list(scheme, args[1])
        elif arg == "dependencystat":
            print_dependencytodo_list(scheme, args[1])
        elif arg == "stepto":
            result = do_step_to_target(scheme, runner, args[1])
            print "Result: " + COMPUTE_TARGET_RESULT_MSG[result]
        elif arg == "viewstepto":
            result = view_step_to_target(scheme, runner, args[1])
        elif arg == "targetfile":
            print_target_filename(scheme, args[1])
        else:
            print "Invalid parameters"
            sys.exit(1)
    return main


# ----------------- Standard annotation-style definition ----------------------- #
# Usage: @pycex_experiment(runner=SYSTEM_RUNNER, cache_dir=CACHE_DIR)
# def specify_experiment(scheme):
#     .... scheme.data_object.result = scheme.computation.compute(....)
#
# if __name__ == "__main__":
#    specify_experiment()
def pycex_experiment(runner=SYSTEM_RUNNER, cache_dir='.', version="1.0"):
    def transform_function(original_function):
        def new_function():
            scheme = ComputationScheme(cache_dir)
            original_function(scheme)
            main = pycex_default_main(scheme, runner, version)
            main()
        return new_function
    return transform_function

