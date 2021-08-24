#!/usr/bin/env python3

"""Constructs PopsInfo struct for 1KG pops.
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

# * Parsing args

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--pops-data-url', default='ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/20131219.populations.tsv',
                        help='info on pops and superpops')
    #parser.add_argument('--pops-outgroups-json', required=True, help='map from pop to pops to use as outgroups')
    parser.add_argument('--superpop-to-representative-pop-json', required=True,
                        help='map from superpop to representative sub-pop used in model-fitting')
    parser.add_argument('--empirical-regions-bed', required=True, help='empirical regions bed file')

    parser.add_argument('--out-pops-info-json', required=True, help='json file to which to write the popsinfo structure')
    return parser.parse_args()

# * def compute_outgroup_pops(pops_data, superpop_to_representative_pop)
def compute_outgroup_pops(pops_data, superpop_to_representative_pop):
    """For each pop and superpop, compute list of pops that serve as outgroups"""
    
    pop2outgroup_pops = collections.defaultdict(list)

    superpop_to_representative_outgroup_pops = {}
    for superpop in superpop_to_representative_pop:
        pop2outgroup_pops[superpop] = sorted(set(superpop_to_representative_pop) - set([superpop]))
        superpop_to_representative_outgroup_pops[superpop] = [superpop_to_representative_pop[outgroup_superpop]
                                                              for outgroup_superpop in pop2outgroup_pops[superpop]]

    _log.debug(f'{pops_data.columns=}')
    for pops_data_row in pops_data.rename(columns={c: c.replace(' ', '_') for c in pops_data.columns}).itertuples(index=False):
        if pops_data_row.Super_Population in superpop_to_representative_pop:
            pop2outgroup_pops[pops_data_row.Population_Code] = superpop_to_representative_outgroup_pops[pops_data_row.Super_Population]

    _log.debug(f'{pop2outgroup_pops=}')

    return dict(pop2outgroup_pops)
# end: def compute_outgroup_pops(pops_data, superpop_to_representative_pop)

def load_empirical_regions_pops(empirical_regions_bed):
    """Load the list of pops used in empirical regions containing putatively selected variants"""
    empirical_regions_pops = set()

    with open(empirical_regions_bed) as empirical_regions_bed_in:
        for line in empirical_regions_bed_in:
            chrom, beg, end, region_name_ignored, sel_pop = line.strip().split('\t')
            empirical_regions_pops.add(sel_pop)
    return sorted(empirical_regions_pops)

# * construct_pops_info
def construct_pops_info(pop2outgroup_pops, empirical_regions_pops):
    """Construct a PopsInfo object (see structs.wdl) for the 1KG populations (including superpopulations)."""
    
    pops_info = {}
    #pops_info['pop_ids'] = list(pops_data['Population Code'].dropna())
    pop_ids = sorted(pop2outgroup_pops)
    pops_info['pop_ids'] = pop_ids
    pops_info['pop_names'] = copy.deepcopy(pop_ids)
    pops_info['pops'] = [{'pop_id': pop_id} for pop_id in pops_info['pop_ids']]

    pops_info['pop_id_to_idx'] = {pop_id: i for i, pop_id in enumerate(pop_ids)}
    pops_info['pop_alts'] = copy.deepcopy(pop2outgroup_pops)

    pops_info['pop_alts_used'] = [[pop_id_2 in pops_info['pop_alts'][pop_id_1] for pop_id_2 in pop_ids] for pop_id_1 in pop_ids]

    pops_info['sel_pop_ids'] = empirical_regions_pops

    return pops_info
# end: def construct_pops_info(pop2outgroup_pops)
        
# * construct_pops_info_for_1KG
def construct_pops_info_for_1KG(args):
    """Constructs PopsInfo struct for 1KG pops."""

    # TODO:
    #   - remove related individuals (see ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/20140625_related_individuals.txt
    #         and later files, and based on the relationships in the .ped file)
    #   - record region coords as offset from start, to keep region coord values low

    #   - split into separate tasks?   e.g. filtering of related individuals, choosing individuals for each pop,
    #     determining ancestral alleles?   [OTOH to get majority alleel have to fetch data for all pops]
    #   - use ancient dna data to get ancestral allele?

    # https://www.internationalgenome.org/faq/how-do-i-get-a-genomic-region-sub-section-of-your-files/
    
    # ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/
    # ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/20140625_related_individuals.txt
    # Sample NA20318 was recorded in 'integrated_call_samples_v2.20130502.ALL.ped' as being unrelated and assigned family ID 2480a. Based on    # information from Coriell and IBD data, we believe that this sample is part of family 2480 and related to samples NA20317 and NA20319.

    # Ancestral allele (based on 1000 genomes reference data).
    # The following comes from its original README file: ACTG -
    # high-confidence call, ancestral state supproted by the other
    # two sequences actg - low-confindence call, ancestral state
    # supported by one sequence only N - failure, the ancestral
    # state is not supported by any other sequence =- - the extant
    # species contains an insertion at this postion . - no coverage
    # in the alignment

    # genetic maps: ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130507_omni_recombination_rates/

    # recent genetic maps:
    # https://advances.sciencemag.org/content/5/10/eaaw9206.full
    # https://drive.google.com/file/d/17KWNaJQJuldfbL9zljFpqj5oPfUiJ0Nv/view?usp=sharing

    # which samples are in which pops; pedigree information:
    # ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_g1k.ped

    # populations and superpopulations:
    # ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/README_populations.md
    # ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/20131219.populations.tsv
    # ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/20131219.superpopulations.tsv

    pops_data = pd.read_table(args.pops_data_url)
    superpop_to_representative_pop = _json_loadf(args.superpop_to_representative_pop_json)
    pop2outgroup_pops = compute_outgroup_pops(pops_data=pops_data,
                                              superpop_to_representative_pop=superpop_to_representative_pop)

    empirical_regions_pops = load_empirical_regions_pops(empirical_regions_bed=args.empirical_regions_bed)

    pops_info = construct_pops_info(pop2outgroup_pops=pop2outgroup_pops, empirical_regions_pops=empirical_regions_pops)
    _write_json(fname=args.out_pops_info_json, json_val=dict(pops_info=pops_info))
# end: def construct_pops_info_for_1KG(args)

if __name__=='__main__':
  #compute_component_scores(parse_args())
  construct_pops_info_for_1KG(parse_args())

