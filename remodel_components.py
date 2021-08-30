##    run new, heterogenous demography model set; generate component scores
##    02.21.2019

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
import math
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

def find_one_file(glob_pattern):
    """If exactly one file matches `glob_pattern`, returns the path to that file, else fails."""
    matching_files = list(glob.glob(glob_pattern))
    if len(matching_files) == 1:
        return matching_files[0]
    raise RuntimeError(f'find_one_file({glob_pattern}): {len(matching_files)} matches - {matching_files}')

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

def chk(cond, msg):
    if not cond:
        raise RuntimeError(f'chk failed: {msg}')

def calc_delihh(readfilename, writefilename):
    """given a selscan iHS file, parses it and writes delihh file"""
    with open_or_gzopen(readfilename) as readfile, open(writefilename, 'w') as writefile:
        for line in readfile:
            entries = line.strip().split()
            chk(len(entries) == 10, 'malformed ihh line')
            # entries are: locus, phys, freq_1, ihh_1, ihh_0, ihs_unnormed, der_ihh_l, der_ihh_r, anc_ihh_l, anc_ihh_r
            
            # handle input with/without ihh details
            # ihh_1 is derived, ihh_0 is ancestral
            if len(entries) == 6:
                    locus, phys, freq_1, ihh_1, ihh_0, ihs_unnormed = entries
            elif len(entries) == 10:
                    locus, phys, freq_1, ihh_1, ihh_0, ihs_unnormed, der_ihh_l, der_ihh_r, anc_ihh_l, anc_ihh_r  = entries
            unstand_delIHH = math.fabs(float(ihh_1) - float(ihh_0)) 
            
            # write 6 columns for selscan norm
            writefile.write('\t'.join([locus, phys, freq_1, ihh_1, ihh_0, str(unstand_delIHH)]) + '\n')
# end: def calc_delihh(readfilename, writefilename):

def calc_derFreq(in_tped, out_derFreq_tsv):
    """Calculate the derived allele frequency for each SNP in one population"""
    with open(in_tped) as tped, open(out_derFreq_tsv, 'w') as out:
        out.write('\t'.join(['chrom', 'snpId', 'pos', 'derFreq']) + '\n')
        for line in tped:
            chrom, snpId, genPos_cm, physPos_bp, alleles = line.strip().split(maxsplit=4)
            n = [alleles.count(i) for i in ('0', '1')]
            derFreq = n[0] / (n[0] + n[1])
            out.write('\t'.join([chrom, snpId, physPos_bp, f'{derFreq:.2f}']) + '\n')

# * Parsing args

def parse_args():
    parser = argparse.ArgumentParser()
    # parser.add_argument('model')
    # parser.add_argument('selpop', type=int)
    # parser.add_argument('irep', type=int)
    # parser.add_argument('--cmsdir', required=True)
    # parser.add_argument('--writedir', required=True)
    # parser.add_argument('--simRecomFile')
    # parser.add_argument('--pops', nargs='+')
    #parser.add_argument('--replica-info')
    #parser.add_argument('--replica-id-string')
    parser.add_argument('--hapsets', nargs='+',
                        required=True, help='list of .tar.gz files where each contains the haps for one hapset')
    #parser.add_argument('--out-basename', required=True, help='base name for output files')
    parser.add_argument('--sel-pop', required=True, help='test for selection in this population')
    parser.add_argument('--alt-pop', help='for two-pop tests, compare with this population')
    parser.add_argument('--components', required=True, choices=('ihs', 'ihh12', 'nsl', 'delihh', 'xpehh', 'fst', 'delDAF', 'derFreq'),
                        nargs='+', help='which component tests to compute')
    parser.add_argument('--threads', type=int, default=1, help='selscan threads')
    parser.add_argument('--checkpoint-file', help='file used for checkpointing')
    #parser.add_argument('--out-json', required=True, help='json file describing the manifest of each file')

    # parser.add_argument('--ihs-bins', help='use ihs bins for normalization')
    # parser.add_argument('--nsl-bins', help='use nsl bins for normalization')
    # parser.add_argument('--ihh12-bins', help='use ihh12 bins for normalization')
    # parser.add_argument('--n-bins-ihs', type=int, default=100, help='number of ihs bins')
    # parser.add_argument('--n-bins-nsl', type=int, default=100, help='number of nsl bins')
    
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
    altpops.remove(thispop)
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

# * compute_component_scores

def add_file_to_checkpoint(checkpoint_file, fname):
    #if not os.path.isfile(checkpoint_file):
    _log.info(f'add_file_to_checkpoint: checkpoint_file={checkpoint_file} fname={fname}')
    if not checkpoint_file:
        _log.info(f'No checkpoint file -- not adding {fname} to checkpoint')
        return
    checkpoint_file_tmp = checkpoint_file + '.tmp.tar'
    if not os.path.isfile(checkpoint_file_tmp):
        execute(f'cp {checkpoint_file} {checkpoint_file_tmp}')

    fname_rel = os.path.relpath(fname)
    execute(f'tar -rvf {checkpoint_file_tmp} {fname_rel}')
    os.rename(checkpoint_file_tmp, checkpoint_file)
    execute(f'ls -l {checkpoint_file}')
    _log.info(f'checkpoint file {checkpoint_file} after adding {fname}:')
    execute(f'tar -tvf {checkpoint_file} 1>&2')

def execute_with_checkpoint(out_fname, cmd, cwd, checkpoint_file):
    """Run the given command to create a given file, and compress it.
    Use the checkpoint file to avoid redoing work.
    """
    #fname_gz = fname + '.gz'
    _log.info(f'execute_with_checkpoint: out_fname={out_fname} cmd={cmd} '
              f'cwd={cwd} checkpoint_file={checkpoint_file}')
    out_fname = os.path.join(cwd, out_fname)
    if os.path.isfile(out_fname):
        _log.info(f'Reusing {out_fname} from checkpoint file {checkpoint_file}; not running {cmd}')
    else:
        _log.info(f'Not Reusing {out_fname} from checkpoint file {checkpoint_file}; running {cmd}')
        execute(cmd, cwd=cwd)
        #execute(f'gzip {fname}', cwd=cwd)
        add_file_to_checkpoint(checkpoint_file=checkpoint_file, fname=out_fname)
    
def compute_component_scores_for_one_hapset(*, args, hapset_haps_tar_gz, hapset_num, checkpoint_file):

    # TODO: check the presence of sentinel file (or a checksum file?) before each operation.
    # TODO: before saving the checkpoint file at the end, move current one away, then move new one in in an atomic operation.
    # (note that this would also take care of compressing the results).

    # TODO: uniformize things, so that for each component there is its own method?

    # TODO: add an (optional?) thread that monitors the memory and load at regular intervals,
    # maybe using psutils, and add to the output.  [is this monitoring feature of cromwell supported by terra?]

    if os.path.getsize(hapset_haps_tar_gz) == 0:
        raise RuntimeError(f'Skipping failed sim {hapset_haps_tar_gz} hapset_num={hapset_num}')

    hapset_dir = os.path.realpath(f'hapset{hapset_num:06}')
    execute(f'mkdir -p {hapset_dir}')
    execute(f'tar -zvxf {hapset_haps_tar_gz} -C {hapset_dir}/')

    out_basename = os.path.basename(hapset_haps_tar_gz) + '__selpop_' + str(args.sel_pop)
    if args.alt_pop:
        out_basename += '__altpop_' + str(args.alt_pop)

    args.threads = min(args.threads, available_cpu_count())
    #shutil.copyfile(args.replica_info, f'{args.replica_id_string}.replica_info.json')
    replicaInfo = _json_loadf(find_one_file(f'{hapset_dir}/*.replicaInfo.json'))
    pop_id_to_idx = dict([(pop_id, idx) for idx, pop_id in enumerate(replicaInfo['popIds'])])
    sel_pop_idx = pop_id_to_idx[args.sel_pop]
    sel_pop_tped = os.path.realpath(os.path.join(hapset_dir, replicaInfo["tpedFiles"][sel_pop_idx]))
    if args.alt_pop:
        alt_pop_idx = pop_id_to_idx[args.alt_pop]
        alt_pop_tped = os.path.realpath(os.path.join(hapset_dir, replicaInfo["tpedFiles"][alt_pop_idx]))
        
    selscan_cmd_base = \
        f'selscan --threads {args.threads} --tped {sel_pop_tped} ' \
        f'--out {out_basename}'
    for component in args.components:
        if component in ('ihs', 'ihh12', 'nsl', 'xpehh'):
            alt_pop_tped_opt = '' if component not in ('xpehh',) else \
                f' --tped-ref {alt_pop_tped} '
            ihs_detail = '' if component != 'ihs' else ' --ihs-detail '
            cmd = f'{selscan_cmd_base} {alt_pop_tped_opt} --{component} {ihs_detail}'
            #execute(cmd, cwd=hapset_dir)
            execute_with_checkpoint(cmd=cmd, out_fname=f'{out_basename}.{component}.out', cwd=hapset_dir, checkpoint_file=checkpoint_file)

    if 'delihh' in args.components:
        if 'ihs' not in args.components:
            raise RuntimeError('To compute delihh must first compute ihs')
        calc_delihh(readfilename=f'{hapset_dir}/{out_basename}.ihs.out',
                    writefilename=f'{hapset_dir}/{out_basename}.delihh.out')

    if 'fst' in args.components or 'delDAF' in args.components:
        fst_and_delDAF_out_fname = os.path.join(hapset_dir, out_basename + '.fst_and_delDAF.tsv')
        cmd = \
            f'freqs_stats {sel_pop_tped} {alt_pop_tped} ' \
            f' {fst_and_delDAF_out_fname}'
        execute_with_checkpoint(cmd=cmd, out_fname=f'{out_basename}.fst_and_delDAF.tsv', cwd=hapset_dir, checkpoint_file=checkpoint_file)

    if 'derFreq' in args.components:
        calc_derFreq(in_tped=sel_pop_tped, out_derFreq_tsv=f'{hapset_dir}/{out_basename}.derFreq.tsv')

    # if False:
    #     execute(f'selscan --threads {args.threads} --ihh12 --tped {replicaInfo["tpedFiles"][sel_pop_idx]} '
    #             f'--out {args.replica_id_string} ')
    #     execute(f'selscan --threads {args.threads} --ihs --ihs-detail --tped {replicaInfo["tpedFiles"][sel_pop_idx]} '
    #             f'--out {args.replica_id_string} ')
    #     execute(f'selscan --threads {args.threads} --nsl --tped {replicaInfo["tpedFiles"][sel_pop_idx]} '
    #             f'--out {args.replica_id_string} ')

    # if False:
    #     for alt_pop in replicaInfo['popIds']:
    #         if alt_pop == args.sel_pop: continue
    #         alt_pop_idx = pop_id_to_idx[alt_pop]
    #         execute(f'selscan --threads {args.threads} --xpehh --tped {replicaInfo["tpedFiles"][this_pop_idx]} '
    #                 f'--tped-ref {replicaInfo["tpedFiles"][alt_pop_idx]} '
    #                 f'--out {args.replica_id_string}__altpop_{alt_pop} ')

    # if args.ihs_bins:
    #     execute(f'norm --ihs --bins {args.n_bins_ihs} --load-bins {args.ihs_bins} --files {args.replica_id_string}.ihs.out '
    #             f'--log {args.replica_id_string}.ihs.out.{args.n_bins_ihs}bins.norm.log ')
    # else:
    #     execute(f'touch {args.replica_id_string}.ihs.out.{args.n_bins_ihs}bins.norm '
    #             f'{args.replica_id_string}.ihs.out.{args.n_bins_ihs}bins.norm.log')

    # if args.nsl_bins:
    #     execute(f'norm --nsl --bins {args.n_bins_nsl} --load-bins {args.nsl_bins} --files {args.replica_id_string}.nsl.out '
    #             f'--log {args.replica_id_string}.nsl.out.{args.n_bins_nsl}bins.norm.log ')
    # else:
    #     execute(f'touch {args.replica_id_string}.nsl.out.{args.n_bins_nsl}bins.norm '
    #             f'{args.replica_id_string}.nsl.out.{args.n_bins_nsl}bins.norm.log')

    # if args.ihh12_bins:
    #     execute(f'norm --ihh12 --load-bins {args.ihh12_bins} --files {args.replica_id_string}.ihh12.out '
    #             f'--log {args.replica_id_string}.ihh12.out.norm.log ')
    # else:
    #     execute(f'touch {args.replica_id_string}.ihh12.out.norm {args.replica_id_string}.ihh12.out.norm.log')

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

def compute_component_scores(args):
    _log.info(f'Starting compute_component_scores: args={args}')
    if args.checkpoint_file:
        if os.path.isfile(args.checkpoint_file) and os.path.getsize(args.checkpoint_file) > 0:
            checkpoint_file_size = os.path.getsize(args.checkpoint_file)
            _log.info(f'Checkpoint file found! Restoring from {args.checkpoint_file} '
                      f'of size {checkpoint_file_size}')
            execute(f'tar -xvf {args.checkpoint_file} 1>&2')
        else:
            _log.info(f'Checkpoint file NOT found; creating checkpoint file {args.checkpoint_file}')
            execute(f'rm -f {args.checkpoint_file}')
            execute(f'touch dummy.dat')
            execute(f'tar cvf {args.checkpoint_file} dummy.dat')

    for hapset_num, f in enumerate(parse_file_list(args.hapsets)):
        compute_component_scores_for_one_hapset(args=copy.deepcopy(args),
                                                hapset_haps_tar_gz=f, hapset_num=hapset_num,
                                                checkpoint_file=args.checkpoint_file)
        
if __name__=='__main__':
  compute_component_scores(parse_args())
