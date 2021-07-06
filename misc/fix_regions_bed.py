#!/usr/bin/env python3

with open('test/pos_con__sorted.bed') as pos_con_f, open('test/pos_con_sorted.hg19.bed', 'w') as out:
    for line in pos_con_f:
        fields = line.strip().split('\t')
        if len(fields) == 4:
            chrom, beg, end, pop = fields
            if chrom.startswith('chr'):
                chrom = chrom[3:]
            out.write('\t'.join([chrom, beg, end, f'{chrom}_{beg}_{end}_{pop}', pop]) + '\n')

    
