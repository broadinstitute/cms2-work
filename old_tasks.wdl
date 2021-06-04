version 1.0

import "./structs.wdl"

# * task compute_one_pop_cms2_components
task compute_one_pop_cms2_components {
  meta {
    description: "Compute one-pop CMS2 component scores assuming selection in a given pop"
  }
  input {
    File region_haps_tar_gz
    Pop sel_pop

    File script
    String docker
    Int preemptible
    ComputeResources compute_resources
  }

  String out_basename = basename(region_haps_tar_gz, ".tar.gz") + "__selpop_" + sel_pop.pop_id
  String script_used_name = "script-used." + basename(script)

  String ihs_out_fname = out_basename + ".ihs.out"
  String nsl_out_fname = out_basename + ".nsl.out"
  String ihh12_out_fname = out_basename + ".ihh12.out"
  String delihh_out_fname = out_basename + ".delihh.out"
  String derFreq_out_fname = out_basename + ".derFreq.tsv"

  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --replica-id-string "~{out_basename}" \
      --out-basename "~{out_basename}" --sel-pop ~{sel_pop.pop_id} --threads ~{compute_resources.cpus} --components ihs nsl ihh12 delihh derFreq
  >>>

  output {
    File ihs = ihs_out_fname
    File nsl = nsl_out_fname
    File ihh12 = ihh12_out_fname
    File delihh = delihh_out_fname
    File derFreq = derFreq_out_fname

    File script_used = script_used_name
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: select_first([compute_resources.mem_gb, 4]) + " GB"
    cpu: select_first([compute_resources.cpus, 1])
    disks: "local-disk " + select_first([compute_resources.local_storage_gb, 50]) + " LOCAL"
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
    Pop sel_pop
    Pop alt_pop

    #File? xpehh_bins

    File script
    ComputeResources compute_resources
    String docker
    Int preemptible
  }
  String out_basename = basename(region_haps_tar_gz) + "__selpop_" + sel_pop.pop_id + "__altpop_" + alt_pop.pop_id
  String script_used_name = out_basename + ".script-used." + basename(script)

  String xpehh_out_fname = out_basename + ".xpehh.out"
  String xpehh_log_fname = out_basename + ".xpehh.log"

  String fst_and_delDAF_out_fname = out_basename + ".fst_and_delDAF.tsv"

# ** command
  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --out-basename "~{out_basename}" \
        --replica-id-string "~{out_basename}" --sel-pop ~{sel_pop.pop_id} --alt-pop ~{alt_pop.pop_id} \
        --threads ~{compute_resources.cpus} --components xpehh fst delDAF
  >>>

# ** outputs
  output {
    File xpehh = xpehh_out_fname
    File xpehh_log = xpehh_log_fname
    File fst_and_delDAF = fst_and_delDAF_out_fname
    Pop sel_pop_used = sel_pop
    Pop alt_pop_used = alt_pop
    File script_used = script_used_name
  }

# ** runtime
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: select_first([compute_resources.mem_gb, 4]) + " GB"
    cpu: select_first([compute_resources.cpus, 1])
    disks: "local-disk " + select_first([compute_resources.local_storage_gb, 50]) + " LOCAL"
  }
}

# * task compute_one_pop_bin_stats_for_normalization
task compute_one_pop_bin_stats_for_normalization {
  meta {
    description: "Compute the means and stds of component scores on neutral sims, for the purpose of normalization"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    String out_fnames_base
    Pop sel_pop
    Array[File]+ ihs_out
    Array[File]+ delihh_out
    Array[File]+ nsl_out
    Array[File]+ ihh12_out

    Int n_bins_ihs
    Int n_bins_nsl
    Int n_bins_ihh12
    Int n_bins_delihh

    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
    Int preemptible
  }

  command <<<
    norm --ihs --bins ~{n_bins_ihs} --files @~{write_lines(ihs_out)} --save-bins ~{out_fnames_base}.norm_bins_ihs.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_ihs.log
    norm --ihs --bins ~{n_bins_delihh} --files @~{write_lines(delihh_out)} --save-bins ~{out_fnames_base}.norm_bins_delihh.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_delihh.log
    norm --nsl --bins ~{n_bins_nsl} --files @~{write_lines(nsl_out)} --save-bins ~{out_fnames_base}.norm_bins_nsl.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_nsl.log
    norm --ihh12 --bins ~{n_bins_ihh12} --files @~{write_lines(ihh12_out)} --save-bins ~{out_fnames_base}.norm_bins_ihh12.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_ihh12.log
  >>>

  output {
    File norm_bins_ihs = out_fnames_base + ".norm_bins_ihs.dat"
    File norm_bins_nsl = out_fnames_base + ".norm_bins_nsl.dat"
    File norm_bins_ihh12 = out_fnames_base + ".norm_bins_ihh12.dat"
    File norm_bins_delihh = out_fnames_base + ".norm_bins_delihh.dat"
    File norm_bins_ihs_log = out_fnames_base + ".norm_bins_ihs.log"
    File norm_bins_nsl_log = out_fnames_base + ".norm_bins_nsl.log"
    File norm_bins_ihh12_log = out_fnames_base + ".norm_bins_ihh12.log"
    File norm_bins_delihh_log = out_fnames_base + ".norm_bins_delihh.log"
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
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
    Pop sel_pop
    Pop alt_pop
    Array[File]+ xpehh_out
    Int n_bins_xpehh

    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
    Int preemptible
  }

  String norm_bins_xpehh_fname = "${out_fnames_base}__selpop_${sel_pop.pop_id}__altpop_${alt_pop.pop_id}.norm_bins_xpehh.dat"
  String norm_bins_xpehh_log_fname = "${out_fnames_base}__selpop_${sel_pop.pop_id}__altpop_${alt_pop.pop_id}.norm_bins_xpehh.dat"

  String norm_bins_flip_pops_xpehh_fname = "${out_fnames_base}__selpop_${alt_pop.pop_id}__altpop_${sel_pop.pop_id}.norm_bins_xpehh.dat"
  String norm_bins_flip_pops_xpehh_log_fname = "${out_fnames_base}__selpop_${alt_pop.pop_id}__altpop_${sel_pop.pop_id}.norm_bins_xpehh.log"

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
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
    cpu: threads
    disks: "local-disk " + local_disk_gb + " LOCAL"
  }
}

# * task normalize_and_collate

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
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "1 GB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

struct collate_stats_and_metadata_for_all_sel_sims_input {
    String experimentId
    Array[File] sel_normed_and_collated
    Array[ReplicaInfo] replica_infos
}

task collate_stats_and_metadata_for_all_sel_sims {
  meta {
    description: "Collate component stats and metadata for all selection sims"
  }
  input {
    collate_stats_and_metadata_for_all_sel_sims_input inp
    File collate_stats_and_metadata_for_all_sel_sims_script = "./collate_stats_and_metadata_for_all_sel_sims.py"
  }
  command <<<
    python3 "~{collate_stats_and_metadata_for_all_sel_sims_script}" --input-json "~{write_json(inp)}" 
  >>>
  output {
    File all_hapsets_component_stats_h5 = inp.experimentId+".all_component_stats.h5"
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "16 GB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
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
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "500 MB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}
