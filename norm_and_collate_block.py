#!/usr/bin/env python3

"""Normalize component scores, and collate the results.
"""

import argparse
import csv
import collections
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

def execute(action, **kw):
    succeeded = False
    try:
        _log.debug('Running command: %s', action)
        subprocess.check_call(action, shell=True, **kw)
        succeeded = True
    finally:
        _log.debug('Returned from running command: succeeded=%s, command=%s', succeeded, action)

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

def chk(cond, msg='condition failed'):
    if not cond:
        raise RuntimeError(f'Error: {msg}') 

def find_one_file(glob_pattern):
    """If exactly one file matches `glob_pattern`, returns the path to that file, else fails."""
    matching_files = list(glob.glob(glob_pattern))
    if len(matching_files) == 1:
        return os.path.realpath(matching_files[0])
    raise RuntimeError(f'find_one_file({glob_pattern}): {len(matching_files)} matches - {matching_files}')

# * Parsing args

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--input-json', required=True, help='inputs passed as json')
    #parser.add_argument('--replica-id-str', required=True, help='replica id string')
    #parser.add_argument('--out-normed-collated', required=True, help='output file for normed and collated results')

    return parser.parse_args()

# * orig_main

def orig_main(args):
    cmsdir = args.cmsdir # "/data/ilya-work/proj/cms2-work/cms/cms/cms/"
    writedir = args.writedir # "/data/mytmp/run5/"
    #"/idi/sabeti-scratch/jvitti/remodel/run2/"
    simRecomFile =  args.simRecomFile # "/idi/sabeti-scratch/jvitti/params/test_recom.recom"
    pops = args.pops or [1,2,3,4]

    #basedir = writedir + model + "_" + regime + "_sel" + str(selpop) + "/"
    basedir = writedir + model + "_" + regime + "/"
    
    tped_dir = basedir + "tpeds/"
    thispop = selpop

    replicate_idstring = "rep" + str(irep)
    replicate_idstring2 = replicate_idstring + "_pop" + str(thispop)
    
    tped_filename = tped_dir + replicate_idstring + "_0_" + str(selpop) + ".tped"
    if not os.path.isfile(tped_filename):
        tped_filename += ".gz"                    
    if not os.path.isfile(tped_filename):
        print(('missing: ', tped_filename))    
        sys.exit(0)                

    ihh12_commandstring = "/ilya/miniconda3/envs/selscan-env/bin/selscan"
    #"python " + cmsdir + "scans.py selscan_ihh12" 
    ihh12_unnormedfileprefix = basedir + "ihh12/" + replicate_idstring2
    #ihh12_argstring = tped_filename + " " + ihh12_unnormedfileprefix + " --threads 7 "
    ihh12_unnormedfilename = ihh12_unnormedfileprefix + ".ihh12.out"
    
    ihh12_argstring = " --ihh12 --tped " + tped_filename + " --out " + ihh12_unnormedfileprefix + " --threads 8"
    ihh12_fullcmd = ihh12_commandstring + " " + ihh12_argstring
    
    print(ihh12_fullcmd)
    if not os.path.isfile(ihh12_unnormedfilename)  and not os.path.isfile(ihh12_unnormedfilename + ".gz"):
        execute(ihh12_fullcmd)


    ihs_commandstring = "/ilya/miniconda3/envs/selscan-env/bin/selscan"
    #ihs_commandstring = "python " + cmsdir + "scans.py selscan_ihs"
    ihs_outfileprefix = basedir + "ihs/" + replicate_idstring2
    ihs_unnormedfile = ihs_outfileprefix + ".ihs.out"
    #ihs_argstring = tped_filename + " " + ihs_outfileprefix + " --threads 7 "
    ihs_argstring = " --ihs --ihs-detail --tped " + tped_filename + " --out " + ihs_outfileprefix + " --threads 8"
    ihs_fullcmd = ihs_commandstring + " " + ihs_argstring
    #ihs_normedfile = ihs_unnormedfile + ".norm"
    print(ihs_fullcmd)
    if not os.path.isfile(ihs_unnormedfile) and not os.path.isfile(ihs_unnormedfile + ".gz"):
        execute(ihs_fullcmd)
    
    delihh_commandstring = "python " + cmsdir + "composite.py delihh_from_ihs"
    delihh_unnormedfile =  basedir + "delihh/" + replicate_idstring2
    delihh_argstring = ihs_unnormedfile + " "+ delihh_unnormedfile
    delihh_fullcmd = delihh_commandstring + " " + delihh_argstring 
    delihh_normedfile = delihh_unnormedfile + ".norm"
    print(delihh_fullcmd)
    if not os.path.isfile(delihh_unnormedfile) and not os.path.isfile(delihh_unnormedfile + ".gz"):
        execute(delihh_fullcmd)        
    
    nsl_commandstring = "/ilya/miniconda3/envs/selscan-env/bin/selscan"
    #nsl_commandstring = "python " + cmsdir + "scans.py selscan_nsl" 
    nsl_unnormedfileprefix = basedir + "nsl/" + replicate_idstring2
    #nsl_argstring = tped_filename + " " + nsl_unnormedfileprefix
    nsl_argstring = " --nsl --tped " + tped_filename + " --out " + nsl_unnormedfileprefix + " --threads 8"
    nsl_fullcmd = nsl_commandstring + " " + nsl_argstring
    nsl_unnormedfilename = nsl_unnormedfileprefix + ".nsl.out"
    print(nsl_fullcmd)
    if not os.path.isfile(nsl_unnormedfilename)  and not os.path.isfile(nsl_unnormedfilename + ".gz"):
        execute(nsl_fullcmd)

    tpeddir = tped_dir


    altpops = pops[:]
    altpops.remove(int(thispop))
    for altpop in altpops:
        #xpehh_commandstring = "python " + cmsdir + "scans.py selscan_xpehh --threads 7"
        xpehh_commandstring = "/ilya/miniconda3/envs/selscan-env/bin/selscan"
        #tped2 = tpeddir + "rep" + str(irep) + "_" + str(altpop) + ".tped"
        #tped_filename2 = get_tped_filename(selpop, irep, ancestralpop, altpop, model, tped_dir)
        tped_filename2 = tped_dir + replicate_idstring + "_0_" + str(altpop) + ".tped"
        if not os.path.isfile(tped_filename2):
            tped_filename2 += ".gz"                    
    
        xpehh_outfileprefix = basedir + "xpehh/" + replicate_idstring2 + "_vs" + str(altpop)
        xpehh_unnormedfile = basedir + "xpehh/" + replicate_idstring2 + "_vs" + str(altpop) + ".xpehh.out"
        #xpehh_argumentstring = tped_filename + " " + xpehh_outfileprefix + " " + tped_filename2 + " --threads 8"
        xpehh_argumentstring = " --xpehh --tped " + tped_filename + " --out " + xpehh_outfileprefix + " --threads 8 --tped-ref " + tped_filename2
        xpehh_fullcmd = xpehh_commandstring + " " + xpehh_argumentstring
        print(xpehh_fullcmd)
        if not os.path.isfile(xpehh_unnormedfile) and not os.path.isfile(xpehh_unnormedfile + ".gz"):
            execute(xpehh_fullcmd)

        #fstdeldaf_commandstring = "python " + cmsdir + "composite.py freqscores"
        #fstdeldaf_outfilename = basedir + "freqs/"  + replicate_idstring2 + "_vs" + str(altpop)
        #fstdeldaf_argumentstring = tped_filename + " " + tped_filename2 + " " + simRecomFile + " " + fstdeldaf_outfilename 
        #fstdeldaf_fullcmd = fstdeldaf_commandstring + " " + fstdeldaf_argumentstring 
        #print(fstdeldaf_fullcmd)
        #if not os.path.isfile(fstdeldaf_outfilename):
        #    execute(fstdeldaf_fullcmd)

def normalize_and_collate_scores_for_one_hapset(inps, inps_idx):

    def descr_df(df, msg):
        """Describe a DataFrame"""
        return
        print('===BEG=======================\n', msg)
        print(df.describe(), '\n')
        df.info(verbose=True, null_counts=True)
        print('===END=======================\n', msg)

    def make_local(inp):
        """Creates a symlink to the input file *inp* in the current directory,
        and returns the symlink.   Necessary because the program 'norm' (part of selscan)
        writes the normalized output file to the same directory as its input file,
        and the directory containing the original imput file may not be writable,
        while the current directory (execution directory) is guaranteed to be writable."""
        if isinstance(inps[inp], list):
            new_inps = []
            for f in inps[inp]:
                local_fname = os.path.basename(f)
                if os.path.exists(local_fname):
                    raise RuntimeError(f'make_local: error localizing {f} to {local_fname} -- already exists')
                os.symlink(f, local_fname)
                new_inps.append(local_fname)
            inps[inp] = new_inps
        else:
            local_fname = os.path.basename(inps[inp])
            if os.path.exists(local_fname):
                raise RuntimeError(f'make_local: error localizing {f} to {local_fname} -- already exists')
            os.symlink(inps[inp], local_fname)
            inps[inp] = local_fname
        return inps[inp]

    for component in ('ihs_out', 'delihh_out', 'nsl_out', 'ihh12_out', 'xpehh_out', 'derFreq_out', 'iSAFE_out'):
        make_local(component)

    def chk_idx(pd, name):
        chk(pd.index.is_unique, f'Bad {name} index: has non-unique values')
        chk(pd.index.is_monotonic_increasing, f'Bad {name} index: not monotonically increasing')
        descr_df(pd, name)

    collated = None

    derFreq = pd.read_table(inps["derFreq_out"], low_memory=False, index_col='pos')
    chk_idx(derFreq, 'derFreq')

    collated = derFreq if collated is None else collated.join(derFreq, how='outer')

    execute(f'norm --ihs --bins {inps["component_computation_params"]["n_bins_ihs"]} --load-bins {inps["norm_bins_ihs"]} '
            f'--files {inps["ihs_out"]} '
            f'--log {inps["ihs_out"]}.{inps["component_computation_params"]["n_bins_ihs"]}bins.norm.log ')

    ihs_normed = pd.read_table(f'{inps["ihs_out"]}.{inps["component_computation_params"]["n_bins_ihs"]}bins.norm',
                               names='id pos p1 ihh1 ihh2 ihs ihsnormed ihs_outside_cutoff'.split(),
                               index_col='pos',
                               low_memory=False).add_prefix('ihs_')
    chk_idx(ihs_normed, 'ihs_normed')

    collated = collated.join(ihs_normed, how='outer')

    execute(f'norm --ihs --bins {inps["component_computation_params"]["n_bins_delihh"]} --load-bins {inps["norm_bins_delihh"]} '
            f'--files {inps["delihh_out"]} '
            f'--log {inps["delihh_out"]}.{inps["component_computation_params"]["n_bins_delihh"]}bins.norm.log ')

    delihh_normed = pd.read_table(f'{inps["delihh_out"]}.{inps["component_computation_params"]["n_bins_delihh"]}bins.norm',
                                  names='id pos p1 ihh1 ihh2 delihh delihhnormed delihh_outside_cutoff'.split(),
                                  index_col='pos',
                                  low_memory=False).add_prefix('delihh_')
    chk_idx(delihh_normed, 'delihh_normed')

    collated = collated.join(delihh_normed, how='outer')

    execute(f'norm --nsl --bins {inps["component_computation_params"]["n_bins_nsl"]} --load-bins {inps["norm_bins_nsl"]} '
            f'--files {inps["nsl_out"]} '
            f'--log {inps["nsl_out"]}.{inps["component_computation_params"]["n_bins_nsl"]}bins.norm.log ')

    nsl_normed = pd.read_table(f'{inps["nsl_out"]}.{inps["component_computation_params"]["n_bins_nsl"]}bins.norm',
                               names='id pos p1 ihh1_nsl ihh2_nsl nsl nslnormed nsl_outside_cutoff'.split(),
                               index_col='pos',
                               low_memory=False).add_prefix('nsl_')
    chk_idx(nsl_normed, 'nsl_normed')
    collated = collated.join(nsl_normed, how='outer')
    chk_idx(collated, 'collated after nsl_normed')

    execute(f'norm --ihh12 --bins 1 --load-bins {inps["norm_bins_ihh12"]} '
            f'--files {inps["ihh12_out"]} '
            f'--log {inps["ihh12_out"]}.norm.log ')

    ihh12_normed = pd.read_table(f'{inps["ihh12_out"]}.norm', index_col='pos',
                                 low_memory=False).add_prefix('ihh12_')
    chk_idx(ihh12_normed, 'ihh12_normed')
    collated = collated.join(ihh12_normed, how='outer')
    chk_idx(collated, 'collated after ihh12_normed')

    for other_pop_idx, (xpehh_out, norm_bins_xpehh) in enumerate(zip(inps["xpehh_out"], inps["norm_bins_xpehh"])):
        execute(f'norm --xpehh --bins 1 --load-bins {norm_bins_xpehh} --files {xpehh_out} '
                f'--log {xpehh_out}.norm.log ')
        xpehh_normed = pd.read_table(xpehh_out+".norm", index_col='pos',
                                     low_memory=False).add_suffix(f'_{other_pop_idx}').add_prefix('xpop_')
        chk_idx(xpehh_normed, f'xpehh_normed_{other_pop_idx}')
        collated = collated.join(xpehh_normed, how='outer')
        chk_idx(collated, f'collated after xpehh_normed_{other_pop_idx}')

    for other_pop_idx, fst_and_delDAF_out in enumerate(inps["fst_and_delDAF_out"]):
        execute(f'norm --xpehh --bins 1 --load-bins {norm_bins_xpehh} --files {xpehh_out} '
                f'--log {xpehh_out}.norm.log ')
        fst_and_delDAF_tsv = \
            pd.read_table(fst_and_delDAF_out, index_col='physPos',
                          low_memory=False).rename_axis('pos').add_suffix(f'_{other_pop_idx}')\
              .add_prefix('fst_and_delDAF_')
        chk_idx(fst_and_delDAF_tsv, f'fst_and_delDAF_tsv_{other_pop_idx}')
        collated = collated.join(fst_and_delDAF_tsv, how='outer')
        chk_idx(collated, f'collated after fst_and_delDAF_tsv_{other_pop_idx}')

    collated['max_xpehh'] = collated.filter(like='normxpehh').max(axis='columns')
    collated['mean_fst'] = collated.filter(like='Fst').mean(axis='columns')
    collated['mean_delDAF'] = collated.filter(like='delDAF').mean(axis='columns')

    isafe = pd.read_table(f'{inps["iSAFE_out"]}',
                               index_col='POS',
                               low_memory=False).rename_axis('pos').add_prefix('iSAFE_')
    chk_idx(isafe, 'isafe')
    collated = collated.join(isafe, how='outer')

    replica_id_str = os.path.basename(inps['ihs_out'])
    if replica_id_str.endswith('.ihs.out'):
        replica_id_str = replica_id_str[:-len('.ihs.out')]
    collated['hapset_id'] = replica_id_str
    descr_df(collated, 'collated final')
    collated.reset_index().to_csv(f'{inps_idx:06}.{replica_id_str}.normed_and_collated.tsv', sep='\t', na_rep='nan', index=False)
    _write_json(fname=f'{inps_idx:06}.{replica_id_str}.normed_and_collated.replicaInfo.json', json_val=_json_loadf(inps['replica_info']))

# end: def normalize_and_collate_scores_for_one_hapset(inps, inps_idx)

def normalize_and_collate_scores(args):
    inps_orig = _json_loadf(args.input_json)
    for i in range(len(inps_orig['replica_info'])):
        inps = copy.deepcopy(inps_orig)


        hapset_components_scores_file = inps['one_pop_component_scores'][i]
        chk(hapset_components_scores_file.endswith('.tar.gz'))
        hapset_dirname = os.path.realpath(f'{i:04}_' + \
                                          os.path.basename(hapset_components_scores_file)[:-len('.tar.gz')])
        hapset_dirname_1pop = os.path.join(hapset_dirname, '1pop')
        mkdir_p(hapset_dirname_1pop)

        execute(f'tar -xvzf {hapset_component_scores_file} -C {hapset_dirname_1pop}')
        manifest_1pop = json_loadf(find_one_file(os.path.join(hapset_dirname_1pop, '*.manifest.json')))

        replica_info = os.path.join(hapset_dirname_1pop, manifest_1pop['replicaInfo'])
        ihs_out = os.path.join(hapset_dirname_1pop, manifest_1pop['ihs'])
        nsl_out = os.path.join(hapset_dirname_1pop, manifest_1pop['nsl'])
        ihh12_out = os.path.join(hapset_dirname_1pop, manifest_1pop['ihh12'])
        delihh_out = os.path.join(hapset_dirname_1pop, manifest_1pop['delihh'])
        derFreq_out = os.path.join(hapset_dirname_1pop, manifest_1pop['derFreq'])
        iSAFE_out = os.path.join(hapset_dirname_1pop, manifest_1pop['iSAFE'])

        xpehh_out = []
        fst_and_delDAF_out = []
        for two_pop_idx, two_pop_scores_file in enumerate([v[i] for v in in inps['two_pop_component_scores']]):
            two_pop_scores_dirname = os.path.join(hapset_dirname, '2pop', f'{two_pop_idx:04}')
            mkdir_p(two_pop_scores_dirname)
            execute(f'tar -xvzf {two_pop_scores_file} -C {two_pop_scores_dirname}')
            manifest_2pop = json_loadf(find_one_file(os.path.join(two_pop_scores_dirname, '*.manifest.json')))
            xpehh_out.append(os.path.join(two_pop_scores_dirname, manifest_2pop['xpehh']))
            fst_and_delDAF_out.append(os.path.join(two_pop_scores_dirname, manifest_2pop['fst_and_delDAF']))

        inps_i = dict(replica_info=replica_info,
                      sel_pop=inps['sel_pop'],
                      ihs_out=ihs_out,
                      nsl_out=nsl_out,
                      ihh12_out=ihh12_out,
                      delihh_out=delihh_out,
                      derFreq_out=derFreq_out,
                      iSAFE_out=iSAFE_out,
                      xpehh_out=xpehh_out,
                      fst_and_delDAF_out=fst_and_delDAF_out,
                      norm_bins_ihs=inps['norm_bins_ihs'],
                      norm_bins_nsl=inps['norm_bins_nsl'],
                      norm_bins_ihh12=inps['norm_bins_ihh12'],
                      norm_bins_delihh=inps['norm_bins_delihh'],
                      norm_bins_xpehh=inps['norm_bins_xpehh'],
                      component_computation_params=inps['component_computation_params'])
        _log.info(f'calling normalize_and_collate_scores_for_one_hapset {i}: {inps_i}')
        normalize_and_collate_scores_for_one_hapset(inps_i, i)

if __name__=='__main__':
  normalize_and_collate_scores(parse_args())
