#!/usr/bin/env python3

import os
import os.path
import sys
import json
import subprocess
import tempfile

def fetch_terra_files():
    with open(sys.argv[1]) as json_f:
        json_data = json.load(json_f)

    def gather_paths(val, paths=set()):
        if isinstance(val, str):
            if val.startswith('/cromwell_root'):
                paths.add(val)
        elif isinstance(val, dict):
            for k, v in val.items():
                gather_paths(v, paths)
        elif isinstance(val, list):
            for v in val:
                gather_paths(v, paths)
        elif not isinstance(val, (int, float)):
            raise RuntimeError(f'Unknown node type: {type(val)=} {val=}')
        return paths

    all_paths = gather_paths(json_data)
    cmds = []
    for i, p in enumerate(all_paths):
        p_dir = os.path.dirname(p)
        cmd = f'mkdir -p {p_dir} && gsutil cp {p.replace("/cromwell_root/","gs://")} {p_dir}/'
        print(f'{i:04d} of {len(all_paths)}: {cmd}')
        cmds.append(cmd)
    with tempfile.NamedTemporaryDirector() as t_dir:
        cmds_list_fname = os.path.join(t_dir, 'cmds.txt')
        with open(cmds_list_fname) as cmds_out:
            cmds_out.write('\n'.join(cmds))
        subprocess.check_call(f'cat {cmds_list_fname} | xargs -P 8', shell=True)

if __name__ == '__main__':
    fetch_terra_files()
