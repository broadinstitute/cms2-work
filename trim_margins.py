#!/usr/bin/env python3

"""Given a tsv file listing in each row some values for a genomic position in a genomic region,
trips lines within given distance of start and end of the genomic region.  The trimmed file replaces the original file.
"""

# * imports etc

import platform

if not tuple(map(int, platform.python_version_tuple())) >= (3,8):
    raise RuntimeError('Python >=3.8 required')

import argparse
import csv
import collections
import concurrent.futures
import contextlib
import copy
import functools
import glob
import gzip
import hashlib
import io
import itertools
import json
import logging
import multiprocessing
import os
import os.path
import pathlib
import random
import re
import shutil
import string
import subprocess
import sys
import tempfile
import time

import numpy as np
import pandas as pd

# * Utils

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

MAX_INT32 = (2 ** 31)-1

def dump_file(fname, value):
    """store string in file"""
    with open(fname, 'w')  as out:
        out.write(str(value))

def _pretty_print_json(json_val, sort_keys=True):
    """Return a pretty-printed version of a dict converted to json, as a string."""
    return json.dumps(json_val, indent=4, separators=(',', ': '), sort_keys=sort_keys)

def _write_json(fname, json_val):
    dump_file(fname=fname, value=_pretty_print_json(json_val))

def _load_dict_sorted(d):
    return collections.OrderedDict(sorted(d.items()))

def _json_loads(s):
    return json.loads(s.strip(), object_hook=_load_dict_sorted, object_pairs_hook=collections.OrderedDict)

def _json_loadf(fname):
    return _json_loads(slurp_file(fname))


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

def execute(cmd, retries=0, retry_delay=0, **kw):
    succeeded = False
    attempt = 0
    while not succeeded:
        try:
            attempt += 1
            _log.debug(f'Running command ({attempt=}, {kw=}): {cmd}')
            subprocess.check_call(cmd, shell=True, **kw)
            succeeded = True
        except Exception as e:
            if retries > 0:
                retries -= 1
                _log.warning(f'Retrying command {cmd} after failure {e}; {retries=} left, {retry_delay=}')
                retry_delay_here = retry_delay * (.9 + .2 * random.random())
                _log.debug(f'Sleeping before retrying for {retry_delay_here}s')
                time.sleep(retry_delay_here)
            else:
                raise
        finally:
            _log.debug(f'Returned from running command: {succeeded=}, {cmd=}')

def chk(cond, msg='condition failed'):
    if not cond:
        raise RuntimeError(f'Error: {msg}') 

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

def parse_file_list(z):
    z_orig = copy.deepcopy(z)
    z = list(z or [])
    result = []
    while z:
        f = z.pop()
        if not f.startswith('@'):
            result.append(f)
        else:
            z.extend(slurp_file(f[1:]).strip().split('\n'))
    _log.info(f'parse_file_list: parsed {z_orig} as {result}')
    return result[::-1]

# * Parsing args

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--region_tsvs', required=True, nargs='+', help='tsv files giving values for genomic regions')
    parser.add_argument('--trim_margin_bp', required=True, type=int, help='margin, in basepairs, to trim from each end of region')
    parser.add_argument('--pos_col', required=True, type=int, help='column number (1-based) of column giving the genomic position')
    parser.add_argument('--has_header_line', action='store_true', help='input has header line')
    parser.add_argument('--tmp-dir', default='.', help='directory for temp files')
    return parser.parse_args()

# * def do_trim_margins(args)
def do_trim_margins(args):
    for region_tsv in parse_file_list(args.region_tsvs):
        trimmed_tsv_tmp = region_tsv + ".trimmed.tmp"
        beg_pos, end_pos, n_cols = None, None, None
        with open(region_tsv) as tsv_in:
            for line_num, line in enumerate(tsv_in):
                fields = line.strip().split('\t')
                if n_cols is None:
                    n_cols = len(fields)
                elif len(fields) != n_cols:
                    raise RuntimeError(f'Malformed tsv file: n_cols changed from {len(fields)} to {n_cols} at {region_tsv}:{line_num}')
                if line_num == 0 and args.has_header_line: continue
                pos_here = int(fields[args.pos_col-1])
                if beg_pos is None:
                    beg_pos = pos_here
                elif not (pos_here > end_pos):
                    raise RuntimeError(f'Genomic positions not increasing in {region_tsv}: {end_pos}, {pos_here}')
                end_pos = pos_here

        _log.info(f'{beg_pos=} {end_pos=} {n_cols=}')
        with open(region_tsv) as tsv_in, open(trimmed_tsv_tmp, 'w') as tsv_out:
            for line_num, line in enumerate(tsv_in):
                if line_num == 0 and args.has_header_line: 
                    tsv_out.write(line)
                    continue
                fields = line.strip().split()
                pos_here = int(fields[args.pos_col-1])
                if pos_here < (beg_pos + args.trim_margin_bp): continue
                if pos_here > (end_pos - args.trim_margin_bp): break
                tsv_out.write(line)
        os.remove(region_tsv)
        os.rename(trimmed_tsv_tmp, region_tsv)


if __name__=='__main__':
  do_trim_margins(parse_args())
