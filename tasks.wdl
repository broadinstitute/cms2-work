version 1.0

# * task compute_one_pop_bin_stats_for_normalization
task compute_one_pop_bin_stats_for_normalization {
  meta {
    description: "Compute the means and stds of component scores on neutral sims, for the purpose of normalization"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    String out_fnames_base
    Int sel_pop
    Array[File]+ ihs_out
    Array[File]+ nsl_out
    Array[File]+ ihh12_out

    Int n_bins_ihs
    Int n_bins_nsl
    Int n_bins_ihh12

    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
    Int preemptible
  }

  command <<<
    norm --ihs --bins ~{n_bins_ihs} --files @~{write_lines(ihs_out)} --save-bins ~{out_fnames_base}.norm_bins_ihs.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_ihs.log
    norm --nsl --bins ~{n_bins_nsl} --files @~{write_lines(nsl_out)} --save-bins ~{out_fnames_base}.norm_bins_nsl.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_nsl.log
    norm --ihh12 --bins ~{n_bins_ihh12} --files @~{write_lines(ihh12_out)} --save-bins ~{out_fnames_base}.norm_bins_ihh12.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_ihh12.log
  >>>

  output {
    File norm_bins_ihs = out_fnames_base + ".norm_bins_ihs.dat"
    File norm_bins_nsl = out_fnames_base + ".norm_bins_nsl.dat"
    File norm_bins_ihh12 = out_fnames_base + ".norm_bins_ihh12.dat"
    File norm_bins_ihs_log = out_fnames_base + ".norm_bins_ihs.log"
    File norm_bins_nsl_log = out_fnames_base + ".norm_bins_nsl.log"
    File norm_bins_ihh12_log = out_fnames_base + ".norm_bins_ihh12.log"
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
    preemptible: preemptible
    memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
    cpu: threads
    disks: "local-disk " + local_disk_gb + " LOCAL"
  }
}

# * task compute_two_pop_bin_stats_for_normalization
task compute_two_pop_bin_stats_for_normalization {
  meta {
    description: "Compute the means and stds of component scores on neutral sims, for a pop pair, for the purpose of normalization"
  }
  input {
    String out_fnames_base
    Int sel_pop
    Int alt_pop
    Array[File]+ xpehh_out
    Int n_bins_xpehh

    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
    Int preemptible
  }

  String norm_bins_xpehh_fname = "${out_fnames_base}__selpop_${sel_pop}__altpop_${alt_pop}.norm_bins_xpehh.dat"
  String norm_bins_xpehh_log_fname = "${out_fnames_base}__selpop_${sel_pop}__altpop_${alt_pop}.norm_bins_xpehh.dat"

  String norm_bins_flip_pops_xpehh_fname = "${out_fnames_base}__selpop_${alt_pop}__altpop_${sel_pop}.norm_bins_xpehh.dat"
  String norm_bins_flip_pops_xpehh_log_fname = "${out_fnames_base}__selpop_${alt_pop}__altpop_${sel_pop}.norm_bins_xpehh.log"

  command <<<
    norm --xpehh --bins ~{n_bins_xpehh} --files @~{write_lines(xpehh_out)} --save-bins "~{norm_bins_xpehh_fname}" --only-save-bins \
        --log "~{norm_bins_xpehh_log_fname}"
    norm --xpehh --xpehh-flip-pops --bins ~{n_bins_xpehh} --files @~{write_lines(xpehh_out)} --save-bins "~{norm_bins_flip_pops_xpehh_fname}" \
        --only-save-bins \
        --log "~{norm_bins_flip_pops_xpehh_log_fname}"
  >>>

  output {
    File norm_bins_xpehh = norm_bins_xpehh_fname
    File norm_bins_xpehh_log = norm_bins_xpehh_log_fname
    
    File norm_bins_flip_pops_xpehh = norm_bins_flip_pops_xpehh_fname
    File norm_bins_flip_pops_xpehh_log = norm_bins_flip_pops_xpehh_log_fname
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
    preemptible: preemptible
    memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
    cpu: threads
    disks: "local-disk " + local_disk_gb + " LOCAL"
  }
}

# * task compute_normed_scores
# task compute_normed_scores {
#   meta {
#     description: "Compute CMS2 normed scores"
#     email: "ilya_shl@alum.mit.edu"
#   }
#   input {
#     Array[File] ihs_raw
#     File ihs_bins

#     Array[File] nsl_raw
#     File nsl_bins

#     Array[File] ihh12_raw
#     File ihh12_bins

#     Int n_bins_ihs
#     Int n_bins_nsl

#     Int threads
#     Int mem_base_gb
#     Int mem_per_thread_gb
#     Int local_disk_gb
#     String docker
#   }

#   String ihs_bins_log = ihs_bins + ".log"
#   String nsl_bins_log = nsl_bins + ".log"
#   String ihh12_bins_log = ihh12_bins + ".log"

#   command <<<
#     cp ~{write_lines(ihs_raw)} ihs_raw_files.list.txt
#     norm --ihs --files @ihs_raw_files.list.txt --load-bins ~{ihs_bins} --save-bins norm_bins_used_ihs.dat --log ~{ihs_bins_log}
#     cat ihs_raw_files.list.txt | xargs -I YYY -- sh -c 'mv YYY.~{n_bins_ihs}bins.normed normed_$(basename YYY)'

#     cp ~{write_lines(nsl_raw)} nsl_raw_files.list.txt
#     norm --nsl --files @nsl_raw_files.list.txt --load-bins ~{nsl_bins} --save-bins norm_bins_used_nsl.dat --log ~{nsl_bins_log}
#     cat nsl_raw_files.list.txt | xargs -I YYY -- sh -c 'mv YYY.~{n_bins_nsl}bins.normed normed_$(basename YYY)'

#     cp ~{write_lines(ihh12_raw)} ihh12_raw_files.list.txt
#     norm --ihh12 --files @ihh12_raw_files.list.txt --load-bins ~{ihh12_bins} --save-bins norm_bins_used_ihh12.dat --log ~{ihh12_bins_log}
#     cat ihh12_raw_files.list.txt | xargs -I YYY -- sh -c 'mv YYY.normed normed_$(basename YYY)'
#   >>>

#   output {
#     File norm_bins_ihs = "norm_bins_used_ihs.dat"
#     Array[File] normed_ihs_out = prefix("normed_", ihs_raw)
#     File norm_bins_ihs_log = ihs_bins_log
#     File norm_bins_nsl = "norm_bins_used_nsl.dat"
#     Array[File] normed_nsl_out = prefix("normed_", nsl_raw)
#     File norm_bins_nsl_log = nsl_bins_log
#     File norm_bins_ihh12 = "norm_bins_used_ihh12.dat"
#     Array[File] normed_ihh12_out = prefix("normed_", ihh12_raw)
#     File norm_bins_ihh12_log = ihh12_bins_log
#   }

#   runtime {
#     docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
#     memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
#     cpu: threads
#     disks: "local-disk " + local_disk_gb + " LOCAL"
#   }
# }

# struct ComponentComputationSpec {
#    String component
#    Int sel_pop
#    Int alt_pop
#  }

#  task compute_component_scores {
#    input {
#       File replica_output
#       Array[ComponentComputationSpec] component_specs
#       Array[String] out_fnames
#    }
#    output {
#      Array[File] out_files = prefix("components_", out_fnames)
#    }
# }

# * task compute_one_pop_cms2_components
task compute_one_pop_cms2_components {
  meta {
    description: "Compute one-pop CMS2 component scores assuming selection in a given pop"
  }
  input {
    File region_haps_tar_gz
    Int sel_pop

    # File? ihs_bins
    # File? nsl_bins
    # File? ihh12_bins
    # Int? n_bins_ihs
    # Int n_bins_nsl

    File script
    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
    Int preemptible
  }
#  String modelId = replicaInfo.modelInfo.modelId
#  Int replicaNumGlobal = replicaInfo.replicaId.replicaNumGlobal
#  String replica_id_string = "model_" + modelId + "__rep_" + replicaNumGlobal + "__selpop_" + sel_pop
  String out_basename = basename(region_haps_tar_gz, ".tar.gz") + "__selpop_" + sel_pop
  String script_used_name = "script-used." + basename(script)

  String ihs_out_fname = out_basename + ".ihs.out"
  String nsl_out_fname = out_basename + ".nsl.out"
  String ihh12_out_fname = out_basename + ".ihh12.out"
  # String ihs_normed_out_fname = out_basename + ".ihs.out." + n_bins_ihs + "bins.norm"
  # String ihs_normed_out_log_fname = ihs_normed_out_fname + ".log"
  # String nsl_normed_out_fname = out_basename + ".nsl.out." + n_bins_nsl + "bins.norm"
  # String nsl_normed_out_log_fname = nsl_normed_out_fname + ".log"
  # String ihh12_normed_out_fname = out_basename + ".ihh12.out.norm"
  # String ihh12_normed_out_log_fname = ihh12_normed_out_fname + ".log"

  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --replica-id-string "~{out_basename}" \
      --out-basename "~{out_basename}" --sel-pop ~{sel_pop} --threads ~{threads} --components ihs nsl ihh12
  >>>

  output {
    #Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
    File ihs = ihs_out_fname
    File nsl = nsl_out_fname
    File ihh12 = ihh12_out_fname
    #File ihs_normed = ihs_normed_out_fname
    #File ihs_normed_log = ihs_normed_out_log_fname
    #File nsl_normed = nsl_normed_out_fname
    #File nsl_normed_log = nsl_normed_out_log_fname
    #File ihh12_normed = ihh12_normed_out_fname
    #File ihh12_normed_log = ihh12_normed_out_log_fname
    #Array[File] xpehh = glob("*.xpehh.out")
    Int threads_used = threads
    File script_used = script_used_name
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
    preemptible: preemptible
    memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
    cpu: threads
    disks: "local-disk " + local_disk_gb + " LOCAL"
  }
}

# * task compute_two_pop_cms2_components_
task compute_two_pop_cms2_components {
  meta {
    description: "Compute cross-pop comparison CMS2 component scores"
  }
# ** inputs
  input {
#    ReplicaInfo replicaInfo
    File region_haps_tar_gz
    Int sel_pop
    Int alt_pop

    #File? xpehh_bins

    File script
    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
    Int preemptible
  }
#  String modelId = replicaInfo.modelInfo.modelId
#  Int replicaNumGlobal = replicaInfo.replicaId.replicaNumGlobal
#  String replica_id_string = "model_" + modelId + "__rep_" + replicaNumGlobal + "__selpop_" + sel_pop
  String out_basename = basename(region_haps_tar_gz) + "__selpop_" + sel_pop + "__altpop_" + alt_pop
  String script_used_name = out_basename + ".script-used." + basename(script)

  String xpehh_out_fname = out_basename + ".xpehh.out"
  String xpehh_log_fname = out_basename + ".xpehh.log"

  String fst_and_delDAF_out_fname = out_basename + ".fst_and_delDAF.tsv"

# ** command
  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --out-basename "~{out_basename}" \
        --replica-id-string "~{out_basename}" --sel-pop ~{sel_pop} --alt-pop ~{alt_pop} \
        --threads ~{threads} --components xpehh fst delDAF
  >>>

# ** outputs
  output {
    #Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
    File xpehh = xpehh_out_fname
    File xpehh_log = xpehh_log_fname
    File fst_and_delDAF = fst_and_delDAF_out_fname
    Int sel_pop_used = sel_pop
    Int alt_pop_used = alt_pop
    #Array[File] xpehh = glob("*.xpehh.out")
    Int threads_used = threads
    File script_used = script_used_name
  }

# ** runtime
  runtime {
    #docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
    docker: "quay.io/ilya_broad/cms@sha256:a02b540e5d5265a917d55ed80796893b448757a7cacb8b6e30212400e349489a"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
    cpu: threads
    disks: "local-disk " + local_disk_gb + " LOCAL"
  }
}

# * task create_tar_gz
task create_tar_gz {
  meta {
    description: "Combine files into a tar file"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    Array[File] files
    String out_basename = "out"
  }
  String out_fname_tar_gz = out_basename + ".tar.gz"
  command <<<
    tar cvfz ~{out_fname_tar_gz} ~{sep=" " files}
  >>>
  output {
    File out_tar_gz = out_fname_tar_gz
  }
  runtime {
    #docker: "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
    #docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    # docker: "ubuntu@sha256:c95a8e48bf88e9849f3e0f723d9f49fa12c5a00cfc6e60d2bc99d87555295e4c"
    memory: "500 MB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

# * task normalize_and_collate

struct NormalizeAndCollateInput {
    Array[Int] pop_ids
    Array[Pair[Int,Int]] pop_pairs
    String replica_id_str
    Int sel_pop
    File ihs_out
    File nsl_out
    File ihh12_out
    Array[File] xpehh_out
    Array[File] fst_and_delDAF_out

    File norm_bins_ihs
    File norm_bins_nsl
    File norm_bins_ihh12
    Array[File] norm_bins_xpehh

    Int n_bins_ihs
    Int n_bins_nsl
    Int n_bins_ihh12
    Int n_bins_xpehh
}

task normalize_and_collate {
  meta {
    description: "Normalize raw scores to neutral sims, and collate component scores into one table."
  }
  input {
    NormalizeAndCollateInput inp
    File normalize_and_collate_script
  }
  String normed_collated_stats_fname = inp.replica_id_str + ".normed_and_collated.tsv"
  command <<<
    python3 "~{normalize_and_collate_script}" --input-json "~{write_json(inp)}" --out-normed-collated "~{normed_collated_stats_fname}"
  >>>  
  output {
    File normed_collated_stats = normed_collated_stats_fname
  }
  runtime {
    # docker: "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    docker: "quay.io/ilya_broad/cms@sha256:5749323900fafc59b9091e9a94f5a9c56e79b0a9be58119f6540b318ca5708d9"  # selscan=1.3.0a06
    #docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    memory: "1 GB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

