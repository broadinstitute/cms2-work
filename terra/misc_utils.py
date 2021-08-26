#!/usr/bin/env python3

"""Miscellaneous utilities.

Some are adapted from https://github.com/broadinstitute/viral-core/blob/master/util/{misc,file}.py
"""

# * Preamble

import argparse
import builtins
import csv
import collections
import collections.abc
import concurrent.futures
import contextlib
import copy
import errno
import functools
import glob
import gzip
import io
import json
import logging
import multiprocessing
import operator
import os
import os.path
import pathlib
import random
import re
import shutil
import subprocess
import sys
import tempfile
import time

# third-party imports
import yaml
import firecloud.api as fapi

# * Utils

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

MAX_INT32 = (2 ** 31)-1

def dump_file(fname, value):
    """store string in file"""
    with open(fname, 'w')  as out:
        out.write(str(value))

def pretty_print_json(json_val, sort_keys=True):
    """Return a pretty-printed version of a dict converted to json, as a string."""
    return json.dumps(json_val, indent=4, separators=(',', ': '), sort_keys=sort_keys)

def mkdir_p(dirpath):
    ''' Verify that the directory given exists, and if not, create it.
    '''
    try:
        os.makedirs(dirpath)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(dirpath):
            pass
        else:
            raise

def chk(condition, message='Check failed', exc=RuntimeError):
    """Check a condition, raise an exception if bool(condition)==False, else return `condition`."""
    if not condition:
        raise exc(message)
    return condition

def touch_p(path, times=None):
    '''Touch file, making parent directories if they don't exist.'''
    mkdir_p(os.path.dirname(path))
    touch(path, times=times)

def write_json(fname, json_val):
    dump_file(fname=fname, value=pretty_print_json(json_val))

def replace_ext(fname, new_ext):
    """Replace file extension of `fname` with `new_ext`."""
    return os.path.splitext(fname)[0] + new_ext

def maps(obj, *keys):
    """Return True if `obj` is a mapping that contains al `keys`."""
    return isinstance(obj, collections.Mapping) and all(k in obj for k in keys)

_str_type = basestring if hasattr(builtins, 'basestring') else str

def is_str(obj):
    """Test if obj is a string type, in a python2/3 compatible way.
    From https://stackoverflow.com/questions/4232111/stringtype-and-nonetype-in-python3-x
    """
    return isinstance(obj, _str_type)

# * json_to_org

def _json_to_org(val, org_file, depth=1, heading='root', title=None, json_file=None):
    """Transform a parsed json structure to an Org mode outliner file (see https://orgmode.org/ ).
    """
    _log.debug(f'Loaded json, writing org to {org_file}...')
    with open(org_file, 'w') as out:
        if title:
            out.write('#+TITLE: {}\n\n'.format(title))
        if json_file:
            out.write('json file: [[{}]]\n\n'.format(json_file))
        def _recurse(val, heading, depth):
            def _header(s): out.write('*'*depth + ' ' + str(s) + '\n')
            def _line(s): out.write(' '*depth + str(s) + '\n')
            out.write('*'*depth + ' ' + heading)
            if isinstance(val, list):
                out.write(' - list of ' + str(len(val)) + '\n')
                if len(val):
                    for i, v in enumerate(val):
                        _recurse(v, heading=str(i), depth=depth+2)
            elif maps(val, '$git_link'):
                rel_path = val['$git_link']
                out.write(' - [[file:{}][{}]]\n'.format(rel_path, os.path.basename(rel_path)))
            elif is_str(val) and os.path.isabs(val) and os.path.isdir(val):
                out.write(' - [[file+emacs:{}][{}]]\n'.format(val, os.path.basename(val)))
            elif isinstance(val, collections.Mapping):
                out.write(' - map of ' + str(len(val)) + '\n')
                if len(val):
                    for k, v in val.items():
                        _recurse(v, heading='_'+k+'_', depth=depth+2)
            else:
                out.write(' - ' + str(val) + '\n')
        _recurse(val=val, heading=heading, depth=depth)
# end: def _json_to_org(val, org_file, depth=1, heading='root')

def json_to_org(json_fname, org_fname=None, maxSizeMb=500):
    """Transform a parsed json structure to an Org mode outliner file (see https://orgmode.org/ ).
    """
    org_fname = org_fname or replace_ext(json_fname, '.org')
    _log.debug(f'converting {json_fname} to {org_fname}')
    _json_to_org(val=json_loadf(json_fname, maxSizeMb=maxSizeMb), org_file=org_fname, json_file=json_fname)
    _log.debug(f'converted {json_fname} to {org_fname}')

def write_json_and_org(fname, **json_dict):
    dump_file(fname=fname, value=pretty_print_json(json_dict))
    json_to_org(json_fname=fname)

def load_dict_sorted(d):
    return collections.OrderedDict(sorted(d.items()))

def json_loads(s):
    return json.loads(s.strip(), object_hook=load_dict_sorted, object_pairs_hook=collections.OrderedDict)

def json_loadf(fname, *args, **kw):
    return json_loads(slurp_file(fname, *args, **kw))

def slurp_file(fname, maxSizeMb=300):
    """Read entire file into one string.  If file is gzipped, uncompress it on-the-fly.  If file is larger
    than `maxSizeMb` megabytes, throw an error; this is to encourage proper use of iterators for reading
    large files.  If `maxSizeMb` is None or 0, file size is unlimited."""
    fileSize = os.path.getsize(fname)
    if maxSizeMb  and  fileSize > maxSizeMb*1024*1024:
        raise RuntimeError('Tried to slurp large file {} (size={}); are you sure?  Increase `maxSizeMb` param if yes'.
                           format(fname, fileSize))
    with open_or_gzopen(fname) as f:
        return f.read()

def open_or_gzopen(fname, *opts, **kwargs):
    mode = 'r'
    open_opts = list(opts)
    assert type(mode) == str, "open mode must be of type str"

    # 'U' mode is deprecated in py3 and may be unsupported in future versions,
    # so use newline=None when 'U' is specified
    if len(open_opts) > 0:
        mode = open_opts[0]
        if sys.version_info[0] == 3:
            if 'U' in mode:
                if 'newline' not in kwargs:
                    kwargs['newline'] = None
                open_opts[0] = mode.replace("U","")

    # if this is a gzip file
    if fname.endswith('.gz'):
        # if text read mode is desired (by spec or default)
        if ('b' not in mode) and (len(open_opts)==0 or 'r' in mode):
            # if python 2
            if sys.version_info[0] == 2:
                # gzip.open() under py2 does not support universal newlines
                # so we need to wrap it with something that does
                # By ignoring errors in BufferedReader, errors should be handled by TextIoWrapper
                return io.TextIOWrapper(io.BufferedReader(gzip.open(fname)))

        # if 't' for text mode is not explicitly included,
        # replace "U" with "t" since under gzip "rb" is the
        # default and "U" depends on "rt"
        gz_mode = str(mode).replace("U","" if "t" in mode else "t")
        gz_opts = [gz_mode]+list(opts)[1:]
        return gzip.open(fname, *gz_opts, **kwargs)
    else:
        return open(fname, *open_opts, **kwargs)

def available_cpu_count():
    """
    Return the number of available virtual or physical CPUs on this system.
    The number of available CPUs can be smaller than the total number of CPUs
    when the cpuset(7) mechanism is in use, as is the case on some cluster
    systems.

    Adapted from http://stackoverflow.com/a/1006301/715090
    """

    cgroup_cpus = MAX_INT32
    try:
        def get_cpu_val(name):
            return float(slurp_file('/sys/fs/cgroup/cpu/cpu.'+name).strip())
        cfs_quota = get_cpu_val('cfs_quota_us')
        if cfs_quota > 0:
            cfs_period = get_cpu_val('cfs_period_us')
            _log.debug('cfs_quota %s, cfs_period %s', cfs_quota, cfs_period)
            cgroup_cpus = max(1, int(cfs_quota / cfs_period))
    except Exception as e:
        pass

    proc_cpus = MAX_INT32
    try:
        with open('/proc/self/status') as f:
            status = f.read()
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$', status)
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                proc_cpus = res
    except IOError:
        pass

    _log.debug('cgroup_cpus %d, proc_cpus %d, multiprocessing cpus %d',
               cgroup_cpus, proc_cpus, multiprocessing.cpu_count())
    return min(cgroup_cpus, proc_cpus, multiprocessing.cpu_count())

def load_yaml_or_json(fname):
    '''Load a dictionary from either a yaml or a json file'''
    _log.debug(f'load_yaml_or_json: {fname=}')
    with open(fname) as f:
        if fname.upper().endswith('.YAML') or fname.upper().endswith('.YML'):
            return yaml.safe_load(f) or {}
        if fname.upper().endswith('.JSON'):
            return json.load(f) or {}
        raise TypeError('Unsupported dict file format: ' + fname)


def load_config(cfg, include_directive='include', std_includes=(), param_renamings=None):
    '''Load a configuration, with support for some extra functionality that lets project configurations evolve
    without breaking backwards compatibility.

    The configuration `cfg` is either a dict (possibly including nested dicts) or a yaml/json file containing one.
    A configuration parameter or config param is a sequence of one or more keys; the value of the corresponding
    parameter is accessed as "cfg[k1][k2]...[kN]".  Note, by "parameter" we denote the entire sequence of keys.

    This function implements extensions to the standard way of specifying configuration parameters via (possibly nested)
    dictionaries.  These extensions make it easier to add or rename config params without breaking backwards
    compatibility.

    One extension lets config files include other config files, and lets you specify "standard" config file(s) to
    be included before all others.  If the "default" config file from the project distribution is made a standard
    include, new parameters can be added to it (and referenced from project code) without breaking compatibility
    with old config files that omit these parameters.

    Another extension lets you, when loading a config file, recognize parameters specified under old or legacy names.
    This lets you change parameter names in new program versions while still accepting legacy config files that
    use older names.

    Args:
       cfg: either a config mapping, or the name of a file containing one (in yaml or json format).
         A config mapping is just a dict, possibly including nested dicts, specifying config params.
         The mapping can include an entry pointing to other config files to include.
         The key of the entry is `include_directive`, and the value is a filename or list of filenames of config files.
         Relative filenames are interpreted relative to the directory containing `cfg`, if `cfg` is a filename,
         else relative to the current directory.  Any files from `std_includes` are prepended to the list of
         included config files.  Parameter values from `cfg` override ones from any included files, and parameter values
         from included files listed later in the include list override parameter values from included files listed
         earlier.

       include_directive: key used to specify included config files
       std_includes: config file(s) implicitly included before all others and before `cfg`
       param_renamings: optional map of old/legacy config param names to new ones.  'Param names' here are
         either keys or sequences of keys.  Example value: {'trinity_kmer_size': ('de_novo_assembly', 'kmer_size')};
         new code can access the parameter as cfg["de_novo_assembly"]["kmer_size"] while legacy users can keep
         specifying it as "trinity_kmer_size: 31".
    '''

    param_renamings = param_renamings or {}

    result = dict()

    base_dir_for_includes = None
    if isinstance(cfg, str):
        cfg_fname = os.path.realpath(cfg)
        base_dir_for_includes = os.path.dirname(cfg_fname)
        cfg = load_yaml_or_json(cfg_fname)

    def _update_config(config, overwrite_config):
        """Recursively update dictionary config with overwrite_config.

        Adapted from snakemake.utils.
        See
        http://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
        for details.

        Args:
          config (dict): dictionary to update
          overwrite_config (dict): dictionary whose items will overwrite those in config

        """

        def _update(d, u):
            def fix_None(v): return {} if v is None else v
            for (key, value) in u.items():
                if (isinstance(value, collections.abc.Mapping)):
                    d[key] = _update(fix_None(d.get(key, {})), value)
                else:
                    d[key] = fix_None(value)
            return d

        _update(config, overwrite_config)


    includes = make_seq(std_includes) + make_seq(cfg.get(include_directive, []))
    for included_cfg_fname in includes:
        if (not os.path.isabs(included_cfg_fname)) and base_dir_for_includes:
            included_cfg_fname = os.path.join(base_dir_for_includes, included_cfg_fname)
        _update_config(result, load_config(cfg=included_cfg_fname, 
                                           include_directive=include_directive,
                                           param_renamings=param_renamings))

    # mappings in the current (top-level) config override any mappings from included configs
    _update_config(result, cfg)

    # load any params specified under legacy names, for backwards compatibility
    param_renamings_seq = dict(map(lambda kv: map(make_seq, kv), param_renamings.items()))

    for old_param, new_param in param_renamings_seq.items():

        # handle chains of param renamings
        while new_param in param_renamings_seq:
            new_param = param_renamings_seq[new_param]

        old_val = functools.reduce(lambda d, k: d.get(k, {}), old_param, result)
        new_val = functools.reduce(lambda d, k: d.get(k, {}), new_param, result)

        if old_val != {} and new_val == {}:
            _update_config(result, functools.reduce(lambda d, k: {k: d}, new_param[::-1], old_val))
            log.warning('Config param {} has been renamed to {}; old name accepted for now'.format(old_param, new_param))

    return result

def is_nonstr_iterable(x, str_types=str):
    '''Tests whether `x` is an Iterable other than a string.  `str_types` gives the type(s) to treat as strings.'''
    return isinstance(x, collections.abc.Iterable) and not isinstance(x, str_types)

def make_seq(x, str_types=str):
    '''Return a tuple containing the items in `x`, or containing just `x` if `x` is a non-string iterable.  Convenient
    for uniformly writing iterations over parameters that may be passed in as either an item or a tuple/list of items.
    Note that if `x` is an iterator, it will be concretized.  `str_types` gives the type(s) to treat as strings.'
    '''
    return tuple(x) if is_nonstr_iterable(x, str_types) else (x,)

def as_type(val, types):
    """Try converting `val`to each of `types` in turn, returning the first one that succeeds."""
    errs = []
    for type in make_seq(types):
        try:
            return type(val)
        except Exception as e:
            errs.append(e)
            pass
    raise TypeError('Could not convert {} to any of {}: {}'.format(val, types, errs))

def subdict(d, keys):
    """Return a newly allocated shallow copy of a mapping `d` restricted to keys in `keys`."""
    d = dict(d)
    keys = set(keys)
    return {k: v for k, v in d.items() if k in keys}

def execute(action, **kw):
    succeeded = False
    try:
        _log.debug('Running command: %s', action)
        subprocess.check_call(action, shell=True, **kw)
        succeeded = True
    finally:
        _log.debug('Returned from running command: succeeded=%s, command=%s', succeeded, action)

def _get_pathconf(file_system_path, param_suffix, default):
    """Return a pathconf parameter value for a filesystem.
    """
    param_str = [s for s in os.pathconf_names if s.endswith(param_suffix)]
    if len(param_str) == 1:
        try:
            return os.pathconf(file_system_path, param_str[0])
        except OSError:
            pass
    return default

def max_file_name_length(file_system_path):
    """Return the maximum valid length of a filename (path component) on the given filesystem."""
    return _get_pathconf(file_system_path, '_NAME_MAX', 80)-1

def max_path_length(file_system_path):
    """Return the maximum valid length of a path on the given filesystem."""
    return _get_pathconf(file_system_path, '_PATH_MAX', 255)-1

def string_to_file_name(string_value, file_system_path=None, length_margin=0):
    """Constructs a valid file name from a given string, replacing or deleting invalid characters.
    If `file_system_path` is given, makes sure the file name is valid on that file system.
    If `length_margin` is given, ensure a string that long can be added to filename without breaking length limits.
    """
    replacements_dict = {
        "\\": "-", # win directory separator
        "/": "-", # posix directory separator
        os.sep: "-", # directory separator
        "^": "_", # caret
        "&": "_and_", # background
        "\"": "", # double quotes
        r"'": "", # single quotes
        r":": "_", # colon (problem for ntfs)
        r" ": "_", # spaces
        r"|": "-", # shouldn't confuse a vertical bar for a shell pipe
        r"!": ".", # not a bash operator
        r";": ".", # not a terminator
        r"?": "_", # could be mistaken for a wildcard
        r"*": "_", # could be mistaken for a wildcard
        r"`": "_", # no subshells
        r" -": "_-", # could be mistaken for an argument
        r" --": "_--", # could be mistaken for an argument
        r">": "_", # no redirect chars
        r"<": "_", # no redirect chars
        r"(": "__", # special character
        r")": "__", # special character
        r"\\x": "_", # hex char
        r"\\o": "_", # octal char
        #r"\\u": "", # unicode char
        #"": "", # other illegal strings to replace
    }

    # group of ascii control and non-printable characters
    control_chars = ''.join( map(chr, list(range(0,32)) + list(range(127,160)) ) )
    control_char_re = re.compile('[%s]' % re.escape(control_chars))
    string_value = control_char_re.sub("_", string_value)

    # replacements from the dictionary above
    strs_to_replace_re = re.compile(r'|'.join(re.escape(key) for key in replacements_dict.keys()))
    string_value = strs_to_replace_re.sub(lambda x: replacements_dict.get(x.group(), "_"), string_value)

    # condense runs of underscores
    double_underscore_re = re.compile(r'_{2,}')
    string_value = double_underscore_re.sub("_", string_value)

    # condense runs of dashes
    double_dash_re = re.compile(r'-{2,}')
    string_value = double_dash_re.sub("-", string_value)

    # remove leading or trailing periods (no hidden files (*NIX) or missing file extensions (NTFS))
    string_value = string_value.strip(".")

    # comply with file name length limits
    if file_system_path is not None:
        max_len = max(1, max_file_name_length(file_system_path) - length_margin)
        string_value = string_value[:max_len]
        while len(string_value.encode('utf-8')) > max_len:
            string_value = string_value[:-1]

    # ensure all the character removals did not make the name empty
    string_value = string_value or '_'

    return string_value

def subparsers_helper(cli):
    subparsers = cli.add_subparsers(dest="subcommand")

    def argument(*name_or_flags, **kwargs):
        """Convenience function to properly format arguments to pass to the
        subcommand decorator.

        """
        return (list(name_or_flags), kwargs)


    def subcommand(args=[], parent=subparsers):
        """Decorator to define a new subcommand in a sanity-preserving way.
        The function will be stored in the ``func`` variable when the parser
        parses arguments so that it can be called directly like so::

            args = cli.parse_args()
            args.func(args)

        Usage example::

            @subcommand([argument("-d", help="Enable debug mode", action="store_true")])
            def subcommand(args):
                print(args)

        Then on the command line::

            $ python cli.py subcommand -d

        """
        def decorator(func):
            parser = parent.add_parser(func.__name__, description=func.__doc__)
            for arg in args:
                parser.add_argument(*arg[0], **arg[1])
            parser.set_defaults(func=func)
        return decorator

    def parse():
        args = cli.parse_args()
        if args.subcommand is None:
            cli.print_help()
        else:
            args.func(args)

    return argument, subcommand, parse
# end: def subparsers_helper(cli)
