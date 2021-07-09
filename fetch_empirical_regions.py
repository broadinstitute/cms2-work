#!/usr/bin/env python3

"""Fetches specified empirical regions for 1KG, converts to hapset format (for each region a .tar.gz of tpeds for that region
and a json file of metadata).
"""

import argparse
import csv
import collections
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

# * Parsing args

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--empirical-regions-bed', required=True, help='empirical regions bed file')
    parser.add_argument('--phased-vcfs-url-template',
                        default='ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/' \
                        'ALL.chr${chrom}.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz',
                        help='URL template for phased vcfs for each chromosome; ${chrom} will be replaced with chrom name')
    parser.add_argument('--pedigree-data-url'
                        default='ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_g1k.ped',
                        help='URL for the file mapping populations to sample IDs and giving relationships between samples')
    parser.add_argument('--related-individuals-url',
                        default='ftp://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/20140625_related_individuals.txt',
                        help='list of individuals related to other 1KG individuals, to be dropped from analysis')
    parser.add_argument('--tmp-dir', default='.', help='directory for temp files')
    return parser.parse_args()


def load_empirical_regions(empirical_regions_bed):
    """Load the list of empirical regions containing putatively selected variants"""
    chrom2regions = collections.defaultdict(collections.OrderedDict)

    with open(empirical_regions_bed) as empirical_regions_bed_in:
        for line in empirical_regions_bed_in:
            chrom, beg, end, region_name, sel_pop = line.strip().split('\t')
            chrom2regions[chrom][(beg, end)] = (region_name, sel_pop)
    return chrom2regions

def fetch_one_chrom_regions_phased_vcf(chrom, regions, phased_vcfs_url_template, tmp_dir):
    """Fetch phased vcf subset for the empirical regions on one chromosome"""
    _log.info(f'Processing chrom {chrom}: {len(regions)=}')
    chrom_regions_deduped_bed = os.path.realpath(f'{tmp_dir}/chrom_{chrom}_sel_regions_deduped.bed')
    with open(chrom_regions_deduped_bed, 'w') as chrom_regions_out:
        for region in sorted(regions):
            chrom_regions_out.write(f'{chrom}\t{region[0]}\t{region[1]}\n')
    chrom_regions_vcf = os.path.realpath(f'{tmp_dir}/chrom_{chrom}_regions_vcf.vcf')
    chrom_phased_vcf_url = string.Template(phased_vcfs_url_template).substitute(chrom=chrom)
    execute(f'tabix -h --separate-regions -R {chrom_regions_deduped_bed} {chrom_phased_vcf_url} > {chrom_regions_vcf}',
            cwd=os.path.realpath(tmp_dir), retries=5, retry_delay=10)
    return chrom_regions_vcf

def gather_unrelated_individuals(ped_data, related_individuals_url):
    """Pick a subset of the 1KG individuals such that no two are known to be related."""

    related_individuals = pd.read_table(related_individuals_url)


    orig_len = len(ped_data)
    ped_data = ped_data.rename(columns={c: c.replace(' ', '_') for c in ped_data.columns})
    ped_data = ped_data[(~ped_data['Individual_ID'].isin(related_individuals['Sample '])) & \
                        (ped_data['Paternal_ID'] == '0') & \
                        (ped_data['Maternal_ID'] == '0') & \
                        (ped_data['Relationship'] != 'child')]


    _log.debug(f'{len(ped_data)-orig_len=}')

    keep = []
    related_to_kept = set()
    skip_reasons = collections.defaultdict(list)
    _log.debug(ped_data.columns)
    for row in ped_data.itertuples():
        r = row._asdict()
        this_id = r['Individual_ID']
        keep_this = False
        if this_id not in related_to_kept:
            keep_this = True
            for col in ('Siblings', 'Second_Order', 'Third_Order'):
                val = r[col].strip()
                if val != '0':
                    if val.startswith('"'):
                        val = val[1:]
                    if val.endswith('"'):
                        val = val[:-1]
                    for rel in val.split(','):
                        related_to_kept.add(rel)
                        skip_reasons[rel].append(this_id)
        if not keep_this:
            _log.debug(f'skipping {this_id} due to {skip_reasons[this_id]}')
        keep.append(keep_this)
    ped_data = ped_data[keep]
    _log.debug(f'{len(ped_data)-orig_len=}')

    return ped_data

def fetch_empirical_regions(args):
    """Fetch data for empirical regions thought to have been under selection, and construct hapsets for them."""

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

    ped_data = pd.read_table(args.pedigree_data_url)
    ped_data = gather_unrelated_individuals(ped_data, args.related_individuals_url)
    
    chrom2regions = load_empirical_regions(args.empirical_regions_bed)

    for chrom in sorted(chrom2regions):
        chrom_regions_vcf = fetch_one_chrom_regions_phased_vcf(chrom, chrom2regions[chrom], args.phased_vcfs_url_template, args.tmp-dir)

        with open(chrom_all_regions_vcf) as chrom_all_regions_vcf_in:
            for vcf_line in chrom_all_regions_vcf_in:
                if vcf_line.startswith('##'): continue
                if vcf_line.startswith('#CHROM'):
                    vcf_cols = vcf_line.strip().split('\t')
                    continue
                

if __name__=='__main__':
  #compute_component_scores(parse_args())
  fetch_empirical_regions(parse_args())

