#!/usr/bin/env python3

"""Miscellaneous utilities.

Some are adapted from https://github.com/broadinstitute/viral-core/blob/master/util/misc.py
"""

# * Preamble

import argparse
import csv
import collections
import collections.abc
import concurrent.futures
import contextlib
import copy
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

def write_json(fname, json_val):
    dump_file(fname=fname, value=_pretty_print_json(json_val))

def load_dict_sorted(d):
    return collections.OrderedDict(sorted(d.items()))

def json_loads(s):
    return json.loads(s.strip(), object_hook=_load_dict_sorted, object_pairs_hook=collections.OrderedDict)

def json_loadf(fname):
    return json_loads(slurp_file(fname))

def slurp_file(fname, maxSizeMb=50):
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

