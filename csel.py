import argparse
#from firecloud import fiss
import json
import operator

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



#print('ENTITIES ARE', fapi.list_entity_types(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE).json())
z = fapi.list_submissions(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE)
#print('SUBMISSIONS ARE', z, z.json())
_write_json('tmp/submissions.json', **{'result': list(z.json())})
for s in sorted(list(z.json()), key=operator.itemgetter('submissionDate'), reverse=True):
    print('looking at submission from', s['submissionDate'])
    if not s['submissionDate'].startswith('2020-12-23'): 
        print('skipping')
        continue

    print('====================================================')
    print(s)
    print('getting submission')
    y = fapi.get_submission(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId']).json()
    print('got submission')
    if 'workflowId' not in y['workflows'][0]:
        print('workflow ID missing from submission!')
        continue
    print('getting workflow metadata')
    zz = fapi.get_workflow_metadata(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId'],
                                    workflow_id=y['workflows'][0]['workflowId']).json()
    print('saving workflow metadata')
    _write_json('tmp/j3.json', **zz)
    if 'submittedFiles' in zz:
        dump_file(fname='tmp/w3.wdl', value=zz['submittedFiles']['workflow'])
    break
    #succ = [v["succeeded"] for v in zz['outputs']["run_sims_cosi2.replicaInfos"]]
    #print(f'Succeeded: {sum(succ)} of {len(succ)}')

    # zzz = fapi.get_workflow_metadata(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId'],
    #                                 workflow_id='ad1e8271-fe66-4e05-9005-af570e9e5884').json()
    # _write_json('tmp/jz.json', **zzz)














