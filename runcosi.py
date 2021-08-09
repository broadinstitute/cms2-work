#!/usr/bin/env python3

# * Preamble

"""Run cosi2 simulation for one block of replicas."""

__author__="ilya_shl@alum.mit.edu"

# * imports

import argparse
import csv
import collections
import concurrent.futures
import contextlib
import copy
import functools
import gzip
import io
import json
import logging
import multiprocessing
import os
import os.path
import random
import re
import shutil
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

    def getPopsFromParamFile(paramFile):
        pop_ids = []
        pop_names = []
        with open(paramFile) as paramFileHandle:
            for line in paramFileHandle:
                if line.startswith('pop_define'):
                    pop_define, pop_id, pop_name = line.strip().split()
                    pop_ids.append(pop_id)
                    pop_names.append(pop_name)
        return pop_ids, pop_names

    popIds, popNames = getPopsFromParamFile(paramFile)
    _log.debug(f'popIds={popIds} popNames={popNames}')

    randomSeed = random.SystemRandom().randint(0, MAX_INT32)

    repStr = f"rep_{replicaNum}"
    blkStr = f"{args.simBlockId}__{repStr}"
    tpedPrefix = f"{blkStr}"
    trajFile = f"{blkStr}.traj"
    sweepInfoFile = f"{blkStr}.sweepinfo.tsv"
    tpeds_tar_gz = f"{args.tpedPrefix}__tar_gz__{repStr}"
    replicaInfoJsonFile =f'{tpedPrefix}.replicaInfo.json'
    paramFileCopyFile =f'{tpedPrefix}.cosiParams.par'
    _run = functools.partial(subprocess.check_call, shell=True)
    cosi2_cmd = (
        f'(env COSI_NEWSIM=1 COSI_MAXATTEMPTS={args.maxAttempts} COSI_SAVE_TRAJ={trajFile} '
        f'COSI_SAVE_SWEEP_INFO={sweepInfoFile} coalescent -R {args.recombFile} -p {paramFile} '
        f'-v -g -r {randomSeed} --genmapRandomRegions '
        f'--drop-singletons .25 --tped {tpedPrefix} )'
        )

    no_sweep = dict(selPop=0, selGen=0., selBegPop=0, selBegGen=0., selCoeff=0., selFreq=0.,)

    def _load_sweep_info():
        result = copy.deepcopy(no_sweep)
        try:
            simNum, selPop, selGen, selBegPop, selBegGen, selCoeff, selFreq = map(float, slurp_file(sweepInfoFile).strip().split())
            result = dict(selPop=str(int(selPop)), selGen=selGen, selBegPop=str(int(selBegPop)),
                          selBegGen=selBegGen, selCoeff=selCoeff, selFreq=selFreq)
        except Exception as e:
            _log.warning(f'Could not load sweep info file {sweepInfoFile}: {e}')
        return result

    replicaInfo = dict(replicaId=dict(blockNum=args.blockNum,
                                      replicaNumInBlock=replicaNum,
                                      replicaNumGlobal=args.blockNum * args.numRepsPerBlock + replicaNum,
                                      replicaNumGlobalOutOf=args.numBlocks*args.numRepsPerBlock,
                                      randomSeed=randomSeed),
                       succeeded=False,
                       region_haps_tar_gz=tpeds_tar_gz,
                       modelInfo=dict(modelId=args.modelId,
                                      modelIdParts=[os.path.basename(args.paramFileCommon),
                                                    os.path.basename(args.paramFile)],
                                      popIds=popIds, popNames=popNames,
                                      sweepInfo=copy.deepcopy(no_sweep)))
    try:
        _run(cosi2_cmd, timeout=args.repTimeoutSeconds)
        # TODO: parse param file for list of pops, and check that we get all the files.
        sweepInfo = _load_sweep_info()
        replicaInfo['modelInfo'].update(sweepInfo=sweepInfo)
        tpedFiles = [f'{tpedPrefix}_0_{popId}.tped' for popId in popIds]
        replicaInfoCopy = copy.deepcopy(replicaInfo)
        replicaInfoCopy.update(succeeded=True)
        _write_json(fname=replicaInfoJsonFile,
                    json_val=dict(replicaInfo=replicaInfoCopy,
                                  cosi2Cmd=cosi2_cmd,
                                  popIds=popIds, popNames=popNames,
                                  tpedFiles=tpedFiles,
                                  trajFile=trajFile,
                                  paramFile=paramFileCopyFile))
        shutil.copyfile(src=paramFile, dst=paramFileCopyFile)
        tpedFilesJoined = " ".join(tpedFiles)
        trajFile_which = trajFile if os.path.isfile(trajFile) else ''
        _run(f'tar cvfz {tpeds_tar_gz} {tpedFilesJoined} {trajFile_which} '
             f'{paramFileCopyFile} {replicaInfoJsonFile}')
        replicaInfo.update(succeeded=True)
    except subprocess.SubprocessError as subprocessError:
        _log.warning(f'command "{cosi2_cmd}" failed with {subprocessError}')
        dump_file(tpeds_tar_gz, '')

    replicaInfo.update(durationSeconds=round(time.time()-time_beg, 2))

    return replicaInfo
# end: def run_one_replica(replicaNum, args, paramFile)

# * main

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('--paramFileCommon', required=True, help='the common part of all parameter files')
    parser.add_argument('--paramFile', required=True, help='the variable part of all parameter files')
    parser.add_argument('--recombFile', required=True, help='the recombination file')
    parser.add_argument('--modelId', required=True, help='demographic model id')
    parser.add_argument('--simBlockId', required=True, help='string ID of the simulation block')
    parser.add_argument('--blockNum', type=int, required=True, help='number of the block of simulations')
    parser.add_argument('--numBlocks', type=int, required=True, help='total number of blocks in the simulations')
    parser.add_argument('--numRepsPerBlock', type=int, required=True, help='number of replicas in the block')
    parser.add_argument('--maxAttempts', type=int, default=10000000,
                        help='max # of times to try simulating forward frequency trajectory before giving up')
    parser.add_argument('--repTimeoutSeconds', type=int, required=True, help='max time per replica')

    parser.add_argument('--tpedPrefix', required=True, help='prefix for tpeds')
    #parser.add_argument('--outTsv', help='write output objects to this file')
    parser.add_argument('--outJson', required=True, help='write output objects to this file')
    return parser.parse_args()

def constructParamFileCombined(paramFileCommon, paramFileVarying):
    """Combine common and variable pars of cosi2 param file"""

    paramFileCombined = 'paramFileCombined.par'
    dump_file(fname=paramFileCombined, value=slurp_file(paramFileCommon)+slurp_file(paramFileVarying))
    return paramFileCombined


def writeOutput(outTsv, replicaInfos):
    with open(outTsv, 'w', newline='') as tsvfile:
        fieldnames = list(replicaInfos[0].keys())
        writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        for replicaInfo in replicaInfos:
            writer.writerow(replicaInfo)

def do_main():
    """Parse args and run cosi"""

    args = parse_args()

    with contextlib.ExitStack() as exit_stack:
        executor = exit_stack.enter_context(concurrent.futures.ThreadPoolExecutor(max_workers=min(args.numRepsPerBlock,
                                                                                                  available_cpu_count())))
        paramFileCombined=constructParamFileCombined(paramFileCommon=args.paramFileCommon, paramFileVarying=args.paramFile)
        replicaInfos = list(executor.map(functools.partial(run_one_replica, args=args, paramFile=paramFileCombined),
                                         range(args.numRepsPerBlock)))

    if args.outJson:
        _write_json(fname=args.outJson,
                    json_val=dict(replicaInfos=replicaInfos))
        
    #if args.outTsv:
    #    writeOutput(args.outTsv, replicaInfos)
    
if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s - %(module)s:%(lineno)d:%(funcName)s - %(levelname)s - %(message)s")
    do_main()
