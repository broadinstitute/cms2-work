#!/usr/bin/env python3

import argparse
import copy
#from firecloud import fiss
import json
import operator
import subprocess
import sys
import time

#print(fiss.meth_list(args=argparse.Namespace()))
import firecloud.api as fapi


SEL_NAMESPACE='um1-encode-y2s1'
SEL_WORKSPACE='selection-sim'

#dir(fapi)
#help(fapi)
z = fapi.list_workspace_configs(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, allRepos=True).json()
print(z)
z = fapi.get_workspace_config(workspace=SEL_WORKSPACE, namespace=SEL_NAMESPACE,
                              config='dockstore-tool-cms2', cnamespace=SEL_NAMESPACE)

print('CONFIG_IS', z, z.json())

def dump_file(fname, value):
    """store string in file"""
    with open(fname, 'w')  as out:
        out.write(str(value))

#z = fapi.create_submission(wnamespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE,
#                           cnamespace=SEL_NAMESPACE, config='dockstore-tool-cosi2')
#print('SUBMISSION IS', z, z.json())

#z = fapi.get_config_template(namespace='dockstore', method='dockstore-tool-cosi2', version=1)
#print(z.json())

def _pretty_print_json(json_dict, sort_keys=True):
    """Return a pretty-printed version of a dict converted to json, as a string."""
    return json.dumps(json_dict, indent=4, separators=(',', ': '), sort_keys=sort_keys)

def _write_json(fname, **json_dict):
    dump_file(fname=fname, value=_pretty_print_json(json_dict))
    print('converting', fname, 'to org')
    subprocess.check_call(f'./to_org.sh {fname}', shell=True)
    print('converted', fname, 'to org')

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
    uri = "workspaces/{0}/{1}/submissions/{2}/workflows/{3}".format(namespace,
                                            workspace, submission_id, workflow_id)
    headers = copy.deepcopy(fapi._fiss_agent_header())
    headers.update({'Accept-Encoding': 'gzip', 'User-Agent': 'gzip'})
    return fapi.__get(uri, headers=headers)

#print('ENTITIES ARE', fapi.list_entity_types(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE).json())
z = fapi.list_submissions(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE)
#print('SUBMISSIONS ARE', z, z.json())
_write_json('tmp/submissions.json', **{'result': list(z.json())})
tot_time = 0
for submission_idx, s in enumerate(sorted(list(z.json()), key=operator.itemgetter('submissionDate'), reverse=True)):
    print('looking at submission from', s['submissionDate'])
    submission_date = s['submissionDate']
    if not submission_date.startswith('2021-03-10'): 
        print('skipping submission date ', submission_date)
        continue

    print('====================================================')
    print(s)
    print('getting submission')
    submission_id = s['submissionId']
    y = fapi.get_submission(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=submission_id).json()
    print('got submission')
    _write_json(f'tmp/{submission_date}.{submission_idx}.{submission_id}.subm.json', **y)
    if 'workflowId' not in y['workflows'][0]:
        print('workflow ID missing from submission!')
        continue
    print('getting workflow metadata for workflow id ', y['workflows'][0]['workflowId'])
    beg = time.time()
    zz_result = get_workflow_metadata_gz(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=submission_id,
                                    workflow_id=y['workflows'][0]['workflowId'])
    print('ZZ_RESULT: ', type(zz_result), dir(zz_result), zz_result)
    for f in dir(zz_result):
        print('  ', f, ' = ', getattr(zz_result, f))
    print('ZZ_RESULT.raw: ', type(zz_result.raw), dir(zz_result.raw), zz_result.raw)
    for f in dir(zz_result.raw):
        print('  ', f, ' = ', getattr(zz_result.raw, f))
    print('converting workflow metadata to json')
    try:
        zz = zz_result.json()
    except Exception as e:
        print('Error converting to json:', e)
        zz = {}
    tot_time += (time.time() - beg)
    print('saving workflow metadata')
    _write_json(f'tmp/{submission_date}.{submission_idx}.{submission_id}.mdata.json', **zz)
    if 'submittedFiles' in zz:
        dump_file(fname=f'tmp/{submission_date}.{submission_idx}.{submission_id}.workflow.wdl', value=zz['submittedFiles']['workflow'])

    #succ = [v["succeeded"] for v in zz['outputs']["run_sims_cosi2.replicaInfos"]]
    #print(f'Succeeded: {sum(succ)} of {len(succ)}')

    # zzz = fapi.get_workflow_metadata(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId'],
    #                                 workflow_id='ad1e8271-fe66-4e05-9005-af570e9e5884').json()
    # _write_json('tmp/jz.json', **zzz)

print('tot_time=', tot_time, file=sys.stderr)
