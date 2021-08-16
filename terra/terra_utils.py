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
            print('Replaced in file', wdl_fname)
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

    SEL_NAMESPACE=terra_config['namespace']
    SEL_WORKSPACE=terra_config['workspace']
    for root_workflow_name, root_workflow_def in terra_config['root_workflows'].items():
        _log.debug(f'{root_workflow_name=} {root_workflow_def=}')
        TERRA_METHOD_NAME=root_workflow_def['method_name']
        TERRA_CONFIG_NAME=root_workflow_def['config_name']

        z = fapi.update_repository_method(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME,
                                          synopsis=root_workflow_def['synopsis'],
                                          wdl=os.path.abspath(root_workflow_def['wdl']))
        #print('UPDATE IS', z, z.json())
        new_method = z.json()
        print('NEW_METHOD IS', new_method)

        #z = fapi.list_repository_methods(namespace=SEL_NAMESPACE, name=TERRA_METHOD_NAME).json()
        #print('METHODS LIST AFT', z)

        snapshot_id = new_method['snapshotId']

        # z = fapi.get_repository_method_acl(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, snapshot_id=snapshot_id)
        # print('ACL:', z, z.json())

        z = fapi.update_repository_method_acl(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, snapshot_id=snapshot_id,
                                              acl_updates=[{'role': 'OWNER', 'user': user} for user in terra_config['users']])
        print('ACL UPDATE:', z, z.json())
        z = fapi.get_repository_method_acl(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, snapshot_id=snapshot_id)
        print('ACL AFTER UPDATE:', z, z.json())

        z = fapi.get_config_template(namespace=SEL_NAMESPACE, method=TERRA_METHOD_NAME, version=snapshot_id)
        #print('CONFIG TEMPLATE AFT IS', z, z.json())
        config_template = z.json()

        #z = fapi.list_workspace_configs(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, allRepos=True).json()
        #print('LIST_WORKSPACE_CONFIGS allRepos', z)
        TERRA_CONFIG_NAME += f'_cfg_{snapshot_id}' 
        # z = fapi.get_workspace_config(workspace=SEL_WORKSPACE, namespace=SEL_NAMESPACE,
        #                               config=TERRA_CONFIG_NAME, cnamespace=SEL_NAMESPACE)

        # print('WORKSPACE_CONFIG_NOW_IS', z, z.json())

        config_json = copy.copy(config_template)
        #print('CONFIG_JSON before deleting rootEntityType', config_json)
        #del config_json['rootEntityType']
        #print('CONFIG_JSON after deleting rootEntityType', config_json)
        #print('CONFIG_JSON about to be updated with inputs:', inputs)
        #config_json.update(namespace=SEL_NAMESPACE, name=TERRA_METHOD_NAME, inputs=inputs, outputs={})
        #print('CONFIG_JSON AFTER UPDATING with inputs:', config_json)


        # orig_template = copy.copy(config_template)
        # print('ORIG_TEMPLATE is', orig_template)
        # del orig_template['rootEntityType']
        # z = fapi.create_workspace_config(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, body=orig_template)
        # print('CREATED CONFIG WITH ORIG TEMPLATE:', z, z.json())
        print('methodConfigVersion was', config_json['methodConfigVersion'])
        config_json['methodConfigVersion'] = snapshot_id
        print('methodConfigVersion now is', config_json['methodConfigVersion'])
        config_json['namespace'] = SEL_NAMESPACE   # configuration namespace
        config_json['name'] = TERRA_CONFIG_NAME
        config_json.pop('rootEntityType', None)

        inputs = dict(misc_utils.json_loadf(terra_config['test_data']))
        config_json['inputs'].update(inputs)

        print('AFTER UPDATING METHODCONFIGVERSION config_json is', config_json)

        z = fapi.create_workspace_config(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, body=config_json)
        print('CREATED CONFIG WITH OUR INPUTS:', z, z.json())

        z = fapi.validate_config(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, cnamespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME)
        print('VALIDATE_CONFIG:', z, z.json())

        z = fapi.get_repository_config_acl(namespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME, snapshot_id=1)
        print('REPO CONFIG ACL:', z, z.json())

        z = fapi.get_workspace_acl(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE)
        print('WORKSPACE ACL:', z, z.json())


        # z = fapi.overwrite_workspace_config(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE,
        #                                     cnamespace=SEL_NAMESPACE, configname=TERRA_CONFIG_NAME, body=config_json)
        # print('OVERWROTE', z, z.json())

        z = fapi.get_workspace_config(workspace=SEL_WORKSPACE, namespace=SEL_NAMESPACE,
                                      config=TERRA_CONFIG_NAME, cnamespace=SEL_NAMESPACE)

        print('CONFIG_NOW_IS_2', z, z.json())

        if True:
            z = fapi.create_submission(wnamespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE,
                                       cnamespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME)
            print('SUBMISSION IS', z, z.json())

    # end: for root_workflow in terra_config['root_workflows']
# end: def do_deploy_to_terra(args)


# * Parsing command line args

# argparse subcommands setup from
# https://gist.github.com/mivade/384c2c41c3a29c637cb6c603d4197f9f
cli = argparse.ArgumentParser()

cli.add_argument('--config-file', default='config/cms2_work_config.json')
subparsers = cli.add_subparsers(dest="subcommand")

def argument(*name_or_flags, **kwargs):
    """Convenience function to properly format arguments to pass to the
    subcommand decorator.

    """
    return (list(name_or_flags), kwargs)


def subcommand(args=[], parent=subparsers):
    """Decorator to define a new subcommand in a sanity-preserving way.
    The function will be stored in the ``func`` variable when the parser
    parses arguments so that it can be called directly like so::

        args = cli.parse_args()
        args.func(args)

    Usage example::

        @subcommand([argument("-d", help="Enable debug mode", action="store_true")])
        def subcommand(args):
            print(args)

    Then on the command line::

        $ python cli.py subcommand -d

    """
    def decorator(func):
        parser = parent.add_parser(func.__name__, description=func.__doc__)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        parser.set_defaults(func=func)
    return decorator


@subcommand()
def deploy_to_terra(args):
    customize_wdls_for_git_commit()
    do_deploy_to_terra(args)

# @subcommand([argument("-d", help="Debug mode", action="store_true")])
# def test(args):
#     print(args)


@subcommand([argument("-f", "--filename", help="A thing with a filename")])
def cmd_with_filename(args):
    print(args)

# @subcommand([argument("name", help="Name")])
# def name(args):
#     print(args.name)


if __name__ == "__main__":
    args = cli.parse_args()
    if args.subcommand is None:
        cli.print_help()
    else:
        args.func(args)
