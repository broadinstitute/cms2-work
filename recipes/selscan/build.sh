#!/bin/sh

cd src

make

install -d "${PREFIX}/bin"
install \
    selscan \
    norm \
    "${PREFIX}/bin/"
cd ../model
make
install \
    freqs_stats \
    calc_fst_deldaf \
    bootstrap_ld_popstats_regions \
    bootstrap_fst_popstats_regions \
    bootstrap_freq_popstats_regions \
    "${PREFIX}/bin/"
