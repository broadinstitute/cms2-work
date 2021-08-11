# sys.exit(0)

# def dump_file(fname, value):
#     """store string in file"""
#     with open(fname, 'w')  as out:
#         out.write(str(value))

# #z = fapi.create_submission(wnamespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE,
# #                           cnamespace=SEL_NAMESPACE, config=TERRA_CONFIG_NAME)
# #print('SUBMISSION IS', z, z.json())

# #z = fapi.get_config_template(namespace='dockstore', method=TERRA_CONFIG_NAME, version=1)
# #print(z.json())

# def _pretty_print_json(json_dict, sort_keys=True):
#     """Return a pretty-printed version of a dict converted to json, as a string."""
#     return json.dumps(json_dict, indent=4, separators=(',', ': '), sort_keys=sort_keys)

# def _write_json(fname, **json_dict):
#     dump_file(fname=fname, value=_pretty_print_json(json_dict))



# #print('ENTITIES ARE', fapi.list_entity_types(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE).json())
# z = fapi.list_submissions(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE)
# #print('SUBMISSIONS ARE', z, z.json())
# for s in sorted(list(z.json()), key=operator.itemgetter('submissionDate'), reverse=True)[:1]:
#     #if not s['submissionDate'].startswith('2020-06-29'): continue

#     print('====================================================')
#     print(s)
#     y = fapi.get_submission(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId']).json()

#     zz = fapi.get_workflow_metadata(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId'],
#                                     workflow_id=y['workflows'][0]['workflowId']).json()
#     _write_json('tmp/j2.json', **zz)
#     dump_file(fname='tmp/w.wdl', value=zz['submittedFiles']['workflow'])

#     # zzz = fapi.get_workflow_metadata(namespace=SEL_NAMESPACE, workspace=SEL_WORKSPACE, submission_id=s['submissionId'],
#     #                                 workflow_id='ad1e8271-fe66-4e05-9005-af570e9e5884').json()
#     # _write_json('tmp/jz.json', **zzz)
