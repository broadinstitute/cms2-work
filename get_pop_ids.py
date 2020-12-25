#!/usr/bin/env python3

"""Parse cosi2 param files to extract the population ids and names, and output in json format
various information about them that would be difficult to do in WDL.
"""

import argparse
import json
import re

def dump_file(fname, value):
    """store string in file"""
    with open(fname, 'w')  as out:
        out.write(str(value))

def _pretty_print_json(json_val, sort_keys=True):
    """Return a pretty-printed version of a dict converted to json, as a string."""
    return json.dumps(json_val, indent=4, separators=(',', ': '), sort_keys=sort_keys)

def _write_json(fname, json_val):
    dump_file(fname=fname, value=_pretty_print_json(json_val))

def chk(cond, msg):
    if not cond:
        raise RuntimeError(f'Error in {__name__}: {msg}')

def parse_args(args=None):
    parser = argparse.ArgumentParser(args)
    parser.add_argument('--dem-model', required=True, help='demographic model')
    parser.add_argument('--sweep-defs', nargs='*', required=True, help='sweep definitions')
    
    return parser.parse_args()

def do_get_pop_ids(args):
    pops_info = {}

    pop_ids = []
    pop_names = []
    with open(args.dem_model) as dem_model:
      for line in dem_model:
        m = re.search(r'^\s*pop_define\s+(?P<pop_id>\d+)\s+(?P<pop_name>\w+)', line)
        if m:
          pop_id = m.group('pop_id')
          pop_name = m.group('pop_name')
          pop_ids.append(pop_id)
          pop_names.append(pop_name)

    chk(len(pop_ids) > 0, 'No pops!')
    chk(len(set(pop_ids)) == len(pop_ids), f'duplicate pops: {pop_ids}')
    chk(len(set(pop_names)) == len(pop_names), f'duplicate pop names: {pop_names}')

    pops_info['pop_ids'] = pop_ids
    pops_info['pop_names'] = pop_names
    pops_info['pop_id2name'] = dict(zip(pop_ids, pop_names))
    pops_info['pop_name2id'] = dict(zip(pop_names, pop_ids))
    pops_info['pop_id2idx'] = {pop_id: i for i, pop_id in enumerate(pop_ids)}
    
    # with open('pop_ids.txt', 'w') as out:
    #   out.write('\n'.join(pop_ids))
    # with open('pop_names.txt', 'w') as out:
    #   out.write('\n'.join(pop_names))
    # _write_json('pop_id_to_idx.json', {pop_id: i for i, pop_id in enumerate(pop_ids)})


    pops_info['pop_pairs'] = [{"Left": pop_ids[i], "Right": pop_ids[j]} for i in range(len(pop_ids)) for j in range(i+1, len(pop_ids))]

    

    pop_pairs_1 = []
    pop_pairs_2 = []
    :
        for j in range(i+1, len(pop_ids)):
            pop_pairs_1.append(pop_ids[i])
            pop_pairs_2.append(pop_ids[j])

    

    _write_json(fname='pops_info.json', dict(pops_info=pops_info))



    with open('pop_pairs_pop1.txt', 'w') as out1:
        out1.write('\n'.join(pop_pairs_1))
    with open('pop_pairs_pop2.txt', 'w') as out2:
        out2.write('\n'.join(pop_pairs_2))

    #pop_pairs = [(pop_ids[i]), for i in range(len(pop_ids)) for j in range(i, len(pop_ids))]

    sel_pops = []
    for sweep_def in (args.sweep_defs or []):
      sel_pops_here = []
      with open(sweep_def) as f:
        for line in f:
          m = re.search(r'^\s*pop_event\s+sweep_mult(?:_standing)?\s+"[^"]*"\s+(?P<sel_pop_id>\d+)\s+', line) # "
          if m:
            sel_pops_here.append(m.group('sel_pop_id'))
      if len(sel_pops_here) != 1:
        raise RuntimeError(f"Could not find sole sweep in {sweep_def}")
      sel_pops.extend(sel_pops_here)
    with open('sel_pop_ids.txt', 'w') as out:
      out.write('\n'.join(sel_pops))

if __name__ == '__main__':
    do_get_pop_ids(parse_args())
