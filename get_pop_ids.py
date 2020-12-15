#!/usr/bin/env python3

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

def parse_args(args=None):
    parser = argparse.ArgumentParser(args)
    parser.add_argument('--dem-model', required=True, help='demographic model')
    parser.add_argument('--sweep-defs', nargs='*', required=True, help='sweep definitions')
    
    return parser.parse_args()

def do_get_pop_ids(args):
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
    with open('pop_ids.txt', 'w') as out:
      out.write('\n'.join(pop_ids))
    with open('pop_names.txt', 'w') as out:
      out.write('\n'.join(pop_names))
    _write_json('pop_id_to_idx.json', {pop_id: i for i, pop_id in enumerate(pop_ids)})

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
