#!/usr/bin/env python3

import json
import os
import os.path
import subprocess

import misc_utils

def quay_tag_exists(quay_tag, quay_repo='ilya_broad/cms', quay_token='6W2IQJ716UB18S5PZP3MJI9AM67XK8ISNJLGVCCIN7DD9HN7LUP9613M8WDO83I9'):
    curl_cmd = f'curl -H "Authorization: Bearer {quay_token}" -X GET "https://quay.io/api/v1/repository/{quay_repo}/tag/{quay_tag}/images?owned=true"'
    curl_out = subprocess.check_output(curl_cmd, shell=True).decode()
    curl_parsed = json.loads(curl_out.strip())
    return not ('images' not in curl_parsed and curl_parsed['status'] == 404 and curl_parsed['error_type'] == 'not_found')

if __name__ == '__main__':
    quay_repo = 'ilya_broad/cms'
    docker_dirs = subprocess.check_output(f'git ls-tree HEAD:docker', shell=True).decode()
    for line in docker_dirs.strip().split('\n'):
        mode, git_obj_type, git_hash, docker_dir = line.strip().split()
        if git_obj_type == 'tree' and os.path.isfile(os.path.join('docker', docker_dir, 'Dockerfile')):
            print(f'looking at {docker_dir} {git_hash}')
            docker_tag = f'{docker_dir}-{git_hash}'
            docker_tag_exists = quay_tag_exists(docker_tag, quay_repo=quay_repo)
            print(f'{docker_tag=} {docker_tag_exists=}')
            if not docker_tag_exists:
                misc_utils.execute(f'docker build -t quay.io/{quay_repo}:{docker_tag} .',
                                   cwd=os.path.realpath(os.path.join('docker', docker_dir)))
                misc_utils.execute(f'docker push quay.io/{quay_repo}:{docker_tag}')
                misc_utils.chk(quay_tag_exists(docker_tag, quay_repo=quay_repo), f'{docker_tag=} still not in {quay_repo}!')

    #z = misc_utils.json_loadf('/data/ilya-work/proj/dockstore-tool-cms2/cms2_empirical-test.wdl.json')
    #result = { k: json.dumps(v, sort_keys=True) for k, v in z.items() }
    #print(misc_utils.pretty_print_json(result))




