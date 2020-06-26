#!/usr/bin/env python3

# * Preamble

"""Run cosi2 simulation for one block"""

__author__="ilya_shl@alum.mit.edu"

# * imports

import argparse
import collections
import concurrent.futures
import contextlib
import functools
import gzip
import io
import json
import logging
import multiprocessing
import os
import random
import re
import subprocess
import sys
import time

# * Utils

_log = logging.getLogger(__name__)

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

# * run_one_sim

def run_one_replica(replicaNum, args, paramFile):
    """Run one cosi2 replica; return a ReplicaInfo struct (defined in Dockstore.wdl).

    Note: replicaNum must be first arg, to facilitate concurrent.futures.Executor.map() over range of replicaNums.
    """

    time_beg = time.time()

    randomSeed = random.SystemRandom().randint(0, MAX_INT32)

    repStr = f"rep{replicaNum}"
    blkStr = f"{args.simBlockId}.{repStr}"
    tpedPrefix = f"{blkStr}"
    trajFile = f"{blkStr}.traj"
    sweepInfoFile = f"{blkStr}.sweepinfo.tsv"
    _run = functools.partial(subprocess.check_call, shell=True)
    emptyFile = f"{blkStr}.empty"
    dump_file(emptyFile, '')
    cosi2_cmd = (
        f'(env COSI_NEWSIM=1 COSI_MAXATTEMPTS={args.maxAttempts} COSI_SAVE_TRAJ={trajFile} '
        f'COSI_SAVE_SWEEP_INFO={sweepInfoFile} coalescent -R {args.recombFile} -p {paramFile} '
        f'-v -g -r {randomSeed} --genmapRandomRegions '
        f'--drop-singletons .25 --tped {tpedPrefix} )'
        )

    def _load_sweep_info():
        simNum, selPop, selGen, selBegPop, selBegGen, selCoeff, selFreq = map(float, slurp_file(sweepInfoFile).strip().split())
        return dict(selPop=int(selPop), selGen=selGen, selBegPop=int(selBegPop), 
                    selBegGen=selBegGen, selCoeff=selCoeff, selFreq=selFreq)

    replicaInfo = dict(modelId=args.modelId, blockNum=args.blockNum,
                       replicaNum=replicaNum, succeeded=False, randomSeed=randomSeed,
                       tpeds=emptyFile, traj=emptyFile, selPop=0, selGen=0., selBegPop=0, selBegGen=0., selCoeff=0., selFreq=0.)
    try:
        _run(cosi2_cmd)
        # TODO: parse param file for list of pops, and check that we get all the files.
        tpeds_tar_gz = f"{blkStr}.tpeds.tar.gz"
        _run(f'tar cvfz {tpeds_tar_gz} {tpedPrefix}_*.tped', timeout=args.repTimeoutSeconds)
        replicaInfo.update(succeeded=True, tpeds=tpeds_tar_gz, traj=trajFile, **_load_sweep_info())
    except subprocess.SubprocessError as subprocessError:
        _log.warning(f'command "{cosi2_cmd}" failed with {subprocessError}')

    replicaInfo.update(duration=time.time()-time_beg)

    return replicaInfo

# * main

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--paramFileCommon', required=True, help='the common part of all parameter files')
    parser.add_argument('--paramFile', required=True, help='the variable part of all parameter files')
    parser.add_argument('--recombFile', required=True, help='the recombination file')
    parser.add_argument('--modelId', required=True, help='demographic model id')
    parser.add_argument('--simBlockId', required=True, help='string ID of the simulation block')
    parser.add_argument('--blockNum', type=int, required=True, help='number of the block of simulations')
    parser.add_argument('--numRepsPerBlock', type=int, required=True, help='number of replicas in the block')
    parser.add_argument('--maxAttempts', type=int, default=10000000,
                        help='max # of times to try simulating forward frequency trajectory before giving up')
    parser.add_argument('--repTimeoutSeconds', type=int, required=True, help='max time per replica')

    parser.add_argument('--outJson', required=True, help='write output json to this file')
    return parser.parse_args()

def constructParamFile(args):
    """Combine common and variable pars of cosi2 param file"""

    paramFileCombined = 'paramFileCombined.par'
    dump_file(fname=paramFileCombined, value=slurp_file(args.paramFileCommon)+slurp_file(args.paramFile))
    return paramFileCombined

def do_main():
    """Parse args and run cosi"""

    args = parse_args()

    with contextlib.ExitStack() as exit_stack:
        executor = exit_stack.enter_context(concurrent.futures.ThreadPoolExecutor(max_workers=min(args.numRepsPerBlock,
                                                                                                  available_cpu_count())))
        replicaInfos = list(executor.map(functools.partial(run_one_replica, args=args, paramFile=constructParamFile(args)),
                                         range(args.numRepsPerBlock)))
    _write_json(args.outJson, dict(replicaInfos=replicaInfos))
    
if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s - %(module)s:%(lineno)d:%(funcName)s - %(levelname)s - %(message)s")
    do_main()
