#!/usr/bin/env python3

import collections
import gzip

stats = collections.Counter()

with gzip.open('/data/ilya-work/proj/dockstore-tool-cms2/tmp/ALL.chr22.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz') as f, open('bad_anc.tsv', 'w') as bad_anc, open('good_anc.tsv', 'w') as good_anc:
    n_with_aa = 0
    n_without_aa = 0
    for i, line in enumerate(f):
        line = line.decode()
        if line.startswith('#'): continue
        chrom, pos, id_, ref, alt, qual, filter_, info, rest = line.strip().split(sep='\t', maxsplit=8)
        #print(info)
        #if i > 100000: break

        info_dict = {field[:2]:field[3:] for field in info.split(';')}
        
        if 'AA' not in info_dict:
            stats['no_aa'] += 1
        else:
            aa = info_dict['AA'].split('|')[0].upper()
            alleles = list(set(map(str.upper, [ref] + alt.split(','))))
            if aa in alleles:
                stats['n_with_aa'] += 1
                good_anc.write('\t'.join([chrom, pos, ref, alt, info_dict['AA']]) + '\n')
            else:
                stats['n_without_aa'] += 1
                bad_anc.write('\t'.join([chrom, pos, ref, alt, info_dict['AA']]) + '\n')
    print(f'{stats=}')




        
        
        
        
        
