#!/usr/bin/env python3

"""Convert .tped files to .vcf ."""

import contextlib
import itertools
import operator
import os
import os.path
import sys

import misc_utils

def hapset_to_vcf(hapset_manifest_json_fname, out_vcf_fname):
    """Convert a hapset to an indexed .vcf.gz"""
    hapset_dir = os.path.dirname(hapset_manifest_json_fname)

    hapset_manifest = misc_utils.json_loadf(hapset_manifest_json_fname)
    pops = hapset_manifest['popIds']

    with open(out_vcf_fname, 'w') as out_vcf:
        out_vcf.write('##fileformat=VCFv4.2\n')
        vcf_cols = ['#CHROM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT']
        for pop in pops:
            for hap_num_in_pop in range(hapset_manifest['pop_sample_sizes'][pop]):
                vcf_cols.append(f'{pop}_{hap_num_in_pop}')
        out_vcf.write('\t'.join(vcf_cols) + '\n')
        
        with contextlib.ExitStack() as exit_stack:
            tped_fnames = [os.path.join(hapset_dir, hapset_manifest['tpeds'][pop])
                           for pop in hapset_manifest['popIds']]
            tpeds = [exit_stack.enter_context(open(tped_fname)) for tped_fname in tped_fnames]
            def make_tuple(*args): return tuple(args)
            tped_lines_tuples = map(make_tuple, *map(iter, tpeds))
            for tped_lines_tuple in tped_lines_tuples:
                # make ref allele be A for ancestral and then D for derived?
                tped_lines_fields = [line.strip().split() for line in tped_lines_tuple]
                misc_utils.chk(len(set(map(operator.itemgetter(3), tped_lines_fields))) == 1,
                               'all tpeds in hapset must be for same pos')
                vcf_fields = ['1', tped_lines_fields[0][3], '.', 'A', 'D', '.', '.', '.', 'GT']
                for pop, tped_line_fields_list in zip(pops, tped_lines_fields):
                    for hap_num_in_pop in range(hapset_manifest['pop_sample_sizes'][pop]):
                        vcf_fields.append('0' if tped_line_fields_list[4 + hap_num_in_pop] == '1' \
                                          else '1')
                out_vcf.write('\t'.join(vcf_fields) + '\n')
            # end: for tped_lines_tuple in tped_lines_tuples
        # end: with contextlib.ExitStack() as exit_stack
    # end: with open(out_vcf_fname, 'w') as out_vcf
# end: def hapset_to_vcf(hapset_manifest_json_fname, out_vcf_fname)
            
if __name__ == '__main__':
    print(misc_utils.available_cpu_count())
    hapset_to_vcf(sys.argv[1], sys.argv[2])
