#!/usr/bin/env python3

"""Utility commands for using Terra ( https://terra.bio/ )
"""

# * Preamble

import argparse
import csv
import collections
import collections.abc
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
import operator
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

# third-party imports
import yaml
import firecloud.api as fapi

# our imports
import misc_utils

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(message)s')

# * Params/constants definitions

def customize_wdls_for_git_commit():
    """Upload files from this commit to the cloud, and create customized WDLs referencing this commit's version of files.   """

    TERRA_GS_BUCKET='fc-21baddbc-5142-4983-a26e-7d85a72c830b'
    TRAVIS_COMMIT=os.environ['TRAVIS_COMMIT']
    TRAVIS_BRANCH=os.environ['TRAVIS_BRANCH']
    TRAVIS_REPO_SLUG=os.environ['TRAVIS_REPO_SLUG']
    STAGING_TRAVIS_REPO_SLUG='notestaff/cms2-staging'
    STAGING_BRANCH=f'staging-{TRAVIS_BRANCH}--{TRAVIS_COMMIT}'
    GITHUB_REPO=TRAVIS_REPO_SLUG.split('/')[1]

    TERRA_DEST=f'gs://{TERRA_GS_BUCKET}/{GITHUB_REPO}/{TRAVIS_BRANCH}/{TRAVIS_COMMIT}/'
    GH_TOKEN=os.environ['GH_TOKEN']

    for wdl_fname in glob.glob('*.wdl'):
        wdl = misc_utils.slurp_file(wdl_fname)
        wdl_repl = re.sub(r'import "./([^"]+)"',
                          f'import "https://raw.githubusercontent.com/{STAGING_TRAVIS_REPO_SLUG}/{STAGING_BRANCH}/\\1"',
                          wdl, flags=re.MULTILINE)
        if wdl_repl != wdl:
            _log.debug(f'Replaced in file {wdl_fname}')
            misc_utils.dump_file(wdl_fname, wdl_repl)

    execute = misc_utils.execute
    execute(f'sed -i "s#\\"./#\\"{TERRA_DEST}#g" *.wdl *.wdl.json')
    execute(f'gsutil -m cp *.py *.wdl *.cosiParams *.par *.recom {TERRA_DEST}')
    execute('git config --global user.email "travis@travis-ci.org"')
    execute('git config --global user.name "Travis CI"')
    execute('git status')
    execute(f'git checkout -b {STAGING_BRANCH}')
    execute(f'git add .')
    execute('git status')
    execute(f'git commit -m "created staging branch {STAGING_BRANCH}"')

    execute(f'git remote rm origin-staging || true')
    execute(f'git remote add origin-staging "https://{GH_TOKEN}@github.com/{STAGING_TRAVIS_REPO_SLUG}.git"')
    execute(f'git push --set-upstream origin-staging {STAGING_BRANCH}')

# * def do_deploy_to_terra()
def do_deploy_to_terra(args):
    """Create Terra method and configuration for the workflow."""

    _log.debug(f'do_deploy_to_terra: {args=}')
    terra_config = misc_utils.load_config(args.config_file)['terra']
    _log.debug(f'{terra_config=}')

    SEL_NAMESPACE = terra_config['namespace']
    SEL_WORKSPACE = terra_config['workspace']
    for root_workflow_name, root_workflow_def in terra_config['root_workflows'].items():
        _log.debug(f'{root_workflow_name=} {root_workflow_def=}')
        TERRA_METHOD_NAME=root_workflow_def['method_name']
        TERRA_CONFIG_NAME=root_workflow_def['config_name']

        z = fapi.update_repository_method(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME,
                                          synopsis=root_workflow_def['synopsis'],
                                          wdl=os.path.abspath(root_workflow_def['wdl']))
        
        def _log_json(heading, json_val):
            orig_val = ''
            if hasattr(json_val, 'json'):
                orig_val = str(json_val) + ' '
                json_val = json_val.json()
            _log.debug(f'{heading}: {orig_val}{misc_utils.pretty_print_json(json_val)}')

        new_method = z.json()
        _log.debug(f'{misc_utils.pretty_print_json(new_method)=}')

        snapshot_id = new_method['snapshotId']

        acl_updates = [{'role': 'OWNER', 'user': user} for user in terra_config['users']]
        _log_json('acl_updates', acl_updates)
        z = fapi.update_repository_method_acl(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, snapshot_id=snapshot_id,
                                              acl_updates=acl_updates)
        _log_json('ACL UPDATE result', z)
        z = fapi.get_repository_method_acl(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, snapshot_id=snapshot_id)
        _log_json('ACL AFTER UPDATE', z)

        z = fapi.get_config_template(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, version=snapshot_id)
        config_template = z.json()

        TERRA_CONFIG_NAME += f'_cfg_{snapshot_id}' 
        config_json = copy.copy(config_template)
        config_json['methodConfigVersion'] = snapshot_id
        config_json['namespace'] = SEL_NAMESPACE   # configuration namespace
        config_json['name'] = TERRA_CONFIG_NAME
        config_json.pop('rootEntityType', None)

        inputs = dict(misc_utils.json_loadf(root_workflow_def['test_data']))
        config_json['inputs'].update(inputs)

        _log_json('AFTER UPDATING METHODCONFIGVERSION config_json is', config_json)

        z = fapi.create_workspace_config(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, body=config_json)
        _log_json('CREATED CONFIG WITH OUR INPUTS:', z)

        z = fapi.validate_config(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, cnamespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME)
        _log_json('VALIDATE_CONFIG:', z)

        z = fapi.get_repository_config_acl(namespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME, snapshot_id=1)
        _log_json('REPO CONFIG ACL:', z)

        z = fapi.get_workspace_acl(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE)
        _log_json('WORKSPACE ACL:', z)

        z = fapi.get_workspace_config(workspace=SEL_WORKSPACE, namespace=SEL_NAMESPACE,
                                      config=TERRA_CONFIG_NAME, cnamespace=SEL_NAMESPACE)

        _log_json('CONFIG_NOW_IS_2', z)

        z = fapi.create_submission(wnamespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE,
                                   cnamespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME)
        _log_json('SUBMISSION IS', z)

    # end: for root_workflow in terra_config['root_workflows']
# end: def do_deploy_to_terra(args)

def get_workflow_metadata_gz(namespace, workspace, submission_id, workflow_id):
    """Request the metadata for a workflow in a submission.

    Args:
        namespace (str): project to which workspace belongs
        workspace (str): Workspace name
        submission_id (str): Submission's unique identifier
        workflow_id (str): Workflow's unique identifier.

    Swagger:
        https://api.firecloud.org/#!/Submissions/workflowMetadata
    """
    uri = "workspaces/{0}/{1}/submissions/{2}/workflows/{3}".format(namespace, workspace, submission_id, workflow_id)
    try:
        headers = copy.deepcopy(fapi._fiss_agent_header())
        headers.update({'Accept-Encoding': 'gzip', 'User-Agent': 'gzip'})
    except Exception as e:
        _log.warning(f'get_workflow_metadata_gz: error getting default fiss agent headers - {e}')
        headers = None
    return fapi.__get(uri, headers=headers)
# end: def get_workflow_metadata_gz(namespace, workspace, submission_id, workflow_id)

def do_list_submissions(args):
    """Lists workflow submissions in Terra"""

    misc_utils.mkdir_p(args.tmp_dir)

    terra_config = misc_utils.load_config(args.config_file)['terra']
    SEL_NAMESPACE = terra_config['namespace']
    SEL_WORKSPACE = terra_config['workspace']

    #print('ENTITIES ARE', fapi.list_entity_types(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE).json())
    z = fapi.list_submissions(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE)
    #print('SUBMISSIONS ARE', z, z.json())
    misc_utils.write_json_and_org(f'{args.tmp_dir}/submissions.json', **{'result': list(z.json())})
    tot_time = 0
    for submission_idx, s in enumerate(sorted(list(z.json()), key=operator.itemgetter('submissionDate'), reverse=True)):
        _log.info(f'looking at submission from {s["submissionDate"]}')
        submission_date = s['submissionDate']
        if not submission_date.startswith(args.submission_date): 
            _log.info(f'skipping submission date {submission_date}')
            continue

        _log.info('====================================================')
        _log.info(f'{s=}')
        _log.info(f'getting submission')
        submission_id = s['submissionId']
        y = fapi.get_submission(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=submission_id).json()
        _log.info('got submission')
        misc_utils.write_json_and_org(f'{args.tmp_dir}/{submission_date}.{submission_idx}.{submission_id}.subm.json', **y)
        if 'workflowId' not in y['workflows'][0]:
            _log.warning(f'workflow ID missing from submission!')
            continue
        _log.info(f"getting workflow metadata for workflow id {y['workflows'][0]['workflowId']}")
        beg = time.time()

        workflow_ids = [y['workflows'][0]['workflowId']]
        while workflow_ids:
            workflow_id = workflow_ids.pop()
            _log.info(f'PROCESSING WORKFLOW: {workflow_id}')

            zz_result = get_workflow_metadata_gz(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=submission_id,
                                                 workflow_id=workflow_id)
            # #print('ZZ_RESULT: ', type(zz_result), dir(zz_result), zz_result)
            # for f in dir(zz_result):
            #     _log.debug(f'  {f} = {getattr(zz_result, f)}')
            # #print('ZZ_RESULT.raw: ', type(zz_result.raw), dir(zz_result.raw), zz_result.raw)
            # for f in dir(zz_result.raw):
            #     _log.debug(f'  {f} = {getattr(zz_result.raw, f)}')
            # _log.debug(f'converting workflow metadata to json')
            try:
                zz = zz_result.json()
            except Exception as e:
                _log.error(f'Error converting to json: {e}')
                zz = {}
            tot_time += (time.time() - beg)
            _log.debug(f'saving workflow metadata')
            workflow_name = zz.get('workflowName', 'no_wf_name')
            misc_utils.write_json_and_org(f'{args.tmp_dir}/{submission_date}.{submission_idx}.{submission_id}.{workflow_id}'
                                          f'.{workflow_name}.mdata.json', **zz)
            if 'submittedFiles' in zz:
                misc_utils.dump_file(fname=f'{args.tmp_dir}/{submission_date}.{submission_idx}.{submission_id}.{workflow_id}.workflow.wdl',
                                     value=zz['submittedFiles']['workflow'])

            jsons = [zz]
            while jsons:
                js = jsons.pop()
                #print('EXAMINING js: ', js)
                if isinstance(js, list):
                    jsons.extend(js)
                elif isinstance(js, dict):
                    if 'subWorkflowId' in js:
                        _log.debug(f'Found subworkflow id: {js["subWorkflowId"]}')
                        workflow_ids.append(js['subWorkflowId'])
                    jsons.extend(list(js.values()))
                else:
                    if 'subWorkflowId' in str(js):
                        raise RuntimeError(f'Missed subworkflowId in {js}')


        #succ = [v["succeeded"] for v in zz['outputs']["run_sims_cosi2.replicaInfos"]]
        #print(f'Succeeded: {sum(succ)} of {len(succ)}')

        # zzz = fapi.get_workflow_metadata(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId'],
        #                                 workflow_id='ad1e8271-fe66-4e05-9005-af570e9e5884').json()
        # _write_json('tmp/jz.json', **zzz)

    _log.info(f'{tot_time=}')
# end: def do_list_submissions(args)

# * Parsing command line args

# argparse subcommands setup from
# https://gist.github.com/mivade/384c2c41c3a29c637cb6c603d4197f9f

def parse_args():

    cli = argparse.ArgumentParser()

    cli.add_argument('--config-file', default='config/cms2_work_config.yml')
    cli.add_argument('--tmp-dir', default=os.path.realpath('tmp'))

    argument, subcommand, parse = misc_utils.subparsers_helper(cli)

    @subcommand()
    def deploy_to_terra(args):
        try:
            customize_wdls_for_git_commit()
            do_deploy_to_terra(args)
        except Exception as e:
            _log.error(f'deploy_to_terra: ERROR {e}')
            raise

    # @subcommand([argument("-d", help="Debug mode", action="store_true")])
    # def test(args):
    #     print(args)

    @subcommand([argument('-s', '--submission-date', default=datetime.datetime.now().strftime('%Y-%m-%d'), help='submission date')])
    def list_submissions(args):
        do_list_submissions(args)

    # @subcommand([argument("name", help="Name")])
    # def name(args):
    #     print(args.name)
    parse()
# end: def parse_args()

if __name__ == "__main__":
    parse_args()
