#!/usr/bin/env python3

import glob
import json
import logging
import os
import os.path
import re
import subprocess

import misc_utils

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

def quay_tag_exists(quay_tag, quay_repo='ilya_broad/cms', quay_token=os.environ['QUAY_CMS_TOKEN']):
    curl_cmd = f'curl -H "Authorization: Bearer {quay_token}" -X GET ' \
        f'"https://quay.io/api/v1/repository/{quay_repo}/tag/{quay_tag}/images?owned=true"'
    curl_out = subprocess.check_output(curl_cmd, shell=True).decode()
    curl_parsed = json.loads(curl_out.strip())
    return not ('images' not in curl_parsed and curl_parsed['status'] == 404 and curl_parsed['error_type'] == 'not_found')

def update_docker_images():
    quay_repo = 'ilya_broad/cms'
    docker_dirs = subprocess.check_output(f'git ls-tree HEAD:docker', shell=True).decode()
    docker_dir_to_docker_tag = {}
    for line in docker_dirs.strip().split('\n'):
        mode, git_obj_type, git_hash, docker_dir = line.strip().split()
        if git_obj_type == 'tree' and os.path.isfile(os.path.join('docker', docker_dir, 'Dockerfile')):
            _log.debug(f'looking at {docker_dir} {git_hash}')
            docker_tag = f'{docker_dir}-{git_hash}'
            docker_tag_exists = quay_tag_exists(docker_tag, quay_repo=quay_repo)
            _log.debug(f'{docker_tag=} {docker_tag_exists=}')
            if not docker_tag_exists:
                misc_utils.execute(f'docker build -t quay.io/{quay_repo}:{docker_tag} .',
                                   cwd=os.path.realpath(os.path.join('docker', docker_dir)))
                misc_utils.execute(f'docker push quay.io/{quay_repo}:{docker_tag}')
                misc_utils.chk(quay_tag_exists(docker_tag, quay_repo=quay_repo), f'{docker_tag=} still not in {quay_repo}!')
            docker_dir_to_docker_tag[docker_dir] = docker_tag
    return docker_dir_to_docker_tag

def do_update_dockers():

    docker_dir_to_docker_tag = update_docker_images()

    for wdl_fname in glob.glob('*.wdl'):
        wdl = misc_utils.slurp_file(wdl_fname)
        wdl_repl = wdl

        for docker_dir, docker_tag in docker_dir_to_docker_tag.items():
            git_hash_re = '[0-9a-f]{40}'
            wdl_repl = re.sub(f'docker: "quay.io/ilya_broad/cms:{docker_dir}-{git_hash_re}"',
                              f'docker: "quay.io/ilya_broad/cms:{docker_tag}"',
                              wdl_repl, flags=re.MULTILINE)
        
        if wdl_repl != wdl:
            _log.debug(f'Replaced in file {wdl_fname}')
            misc_utils.dump_file(wdl_fname, wdl_repl)

if __name__ == '__main__':
    do_update_dockers()
 
