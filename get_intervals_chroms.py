#!/usr/bin/env python3

"""Computes list of chromosomes used in a set of .bed files
"""

import platform

if not tuple(map(int, platform.python_version_tuple())) >= (3,8):
    raise RuntimeError('Python >=3.8 required')

import argparse
import base64
import csv
import collections
import concurrent.futures
import contextlib
import copy
import datetime
import functools
import glob
import gzip
import io
import json
import logging
import multiprocessing
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
import urllib
import urllib.request

# third-party imports
import dominate
import dominate.tags
import dominate.util
import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

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

def execute(action, **kw):
    succeeded = False
    try:
        _log.debug('Running command: %s', action)
        subprocess.check_call(action, shell=True, **kw)
        succeeded = True
    finally:
        _log.debug('Returned from running command: succeeded=%s, command=%s', succeeded, action)

def chk(cond, msg='condition failed'):
    if not cond:
        raise RuntimeError(f'Error: {msg}') 

def is_int(val):
    try:
        x = int(str(val))
        return True
    except Exception as e:
        return False

def reverse_dict(d):
    result = {v: k for k, v in d.items()}
    chk(len(result) == len(d), f'reverse_dict: non-unique values in dict {d}')
    return result

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--intervals-files', required=True, nargs='+', help='intervals files on which to generate a report')
    parser.add_argument('--out-intervals-chroms-txt', required=True, help='output file for intervals chroms list')

    return parser.parse_args()

@contextlib.contextmanager
def create_html_page(html_fname, title=''):
    tags = dominate.tags
    doc = dominate.document(title=title)

    with doc.head:
        tags.meta(charset="UTF-8")
        tags.meta(name="viewport", content="width=device-width, initial-scale=1.0")
        tags.meta(http_equiv="Expires", content="Thu, 1 June 2000 23:59:00 GMT")
        tags.meta(http_equiv="pragma", content="no-cache")
        tags.style('table, th, td {border: 1px solid black;}')
        tags.style('th {background-color: lightblue;}')
        #tags.base(href=os.path.basename(html_fname))

        tags.script(src="http://code.jquery.com/jquery-1.7.1.min.js")

        tags.link(href="https://unpkg.com/tabulator-tables@4.9.3/dist/css/tabulator.min.css", rel="stylesheet")
        tags.script(type="text/javascript", src="https://unpkg.com/tabulator-tables@4.9.3/dist/js/tabulator.min.js")
        
        # tags.link(href="https://raw.githubusercontent.com/olifolkerd/tabulator/4.9.3/dist/css/tabulator.min.css",
        #           rel="stylesheet")
        # tags.script(type='text/javascript',
        #             src="https://raw.githubusercontent.com/olifolkerd/tabulator/4.9.3/dist/js/tabulator.min.js")
        #tags.script(src='https://www.brainbell.com/javascript/download/resizable.js')

    def txt(v): return dominate.util.text(str(v)) if not hasattr(v, 'raw_html') else dominate.util.raw(v.data)
    def trow(vals, td_or_th=tags.td): return tags.tr((td_or_th(txt(val)) for val in vals), __pretty=False)
    raw = dominate.util.raw
    
    def raw_s(v):
        s = collections.UserString(v)
        s.raw_html = True
        return s
    
    with doc:
        raw("""<script  type="text/javascript">
         $(function() {
       $('a[href*="#"]:not([href="#"])').click(function() {
         if (location.pathname.replace(/^\//,'') == this.pathname.replace(/^\//,'') && location.hostname == this.hostname) {
           var target = $(this.hash);
           target = target.length ? target : $('[name=' + this.hash.slice(1) +']');
           if (target.length) {
             $('html, body').animate({
               scrollTop: target.offset().top
             }, 1000);
             return false;
           }
         }
       });
     });
        </script>""")

        tags.div(cls='header').add(txt(datetime.datetime.now()))
        with tags.div(cls='body'):
            tags.h1(title)

        yield (doc, tags, txt, trow, raw, raw_s)

    tags.div(cls='footer').add(txt(datetime.datetime.now()))

    with open(html_fname, 'w') as out:
        out.write(doc.render())

def html_insert_fig(tags):

    my_stringIObytes = io.BytesIO()
    plt.savefig(my_stringIObytes, format='jpg')
    my_stringIObytes.seek(0)
    my_base64_jpgData = base64.b64encode(my_stringIObytes.read())

    encoded = my_base64_jpgData
    #print('type:', type(encoded))
    tags.img(src='data:image/png;base64,'+encoded.decode())

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

def get_intervals_chroms(args):
    for intervals_file in parse_file_list(args.intervals_files):
        interval_chroms = set()
        with open(intervals_file) as intervals_file_in:
            for line in intervals_file_in:
                chrom, beg, end, *rest = line.strip().split()
                interval_chroms.add(chrom)

    with open(args.out_intervals_chroms_txt, 'wt') as out:
        out.write('\n'.join(sorted(interval_chroms)) + '\n')

if __name__ == '__main__':
    get_intervals_chroms(parse_args())

