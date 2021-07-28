version 1.0

import "./structs.wdl"

# * task compute_one_pop_cms2_components
task compute_one_pop_cms2_components {
  meta {
    description: "Compute one-pop CMS2 component scores assuming selection in a given pop"
  }
  input {
    Array[File] region_haps_tar_gzs
    Pop sel_pop

    File script = "./remodel_components.py"
    String docker
    Int preemptible
    ComputeResources compute_resources
  }

  #String out_basename = basename(region_haps_tar_gz, ".tar.gz") + "__selpop_" + sel_pop.pop_id

  # String ihs_out_fname = out_basename + ".ihs.out"
  # String nsl_out_fname = out_basename + ".nsl.out"
  # String ihh12_out_fname = out_basename + ".ihh12.out"
  # String delihh_out_fname = out_basename + ".delihh.out"
  # String derFreq_out_fname = out_basename + ".derFreq.tsv"

  command <<<
    python3 "~{script}" --region-haps-tar-gzs @~{write_lines(region_haps_tar_gzs)} \
      --sel-pop ~{sel_pop.pop_id} --threads ~{compute_resources.cpus} --components ihs nsl ihh12 delihh derFreq \
      --checkpoint-file "checkpoint.tar"
  >>>

  output {
    Array[File] replicaInfos = glob("hapset[0-9]*/*.replicaInfo.json")
    Array[File] ihs = glob("hapset[0-9]*/*.ihs.out")
    Array[File] nsl = glob("hapset[0-9]*/*.nsl.out")
    Array[File] ihh12 = glob("hapset[0-9]*/*.ihh12.out")
    Array[File] delihh = glob("hapset[0-9]*/*.delihh.out")
    Array[File] derFreq = glob("hapset[0-9]*/*.derFreq.tsv")
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: select_first([compute_resources.mem_gb, 4]) + " GB"
    cpu: select_first([compute_resources.cpus, 1])
    disks: "local-disk " + select_first([compute_resources.local_storage_gb, 50]) + " HDD"
    checkpointFile: "checkpoint.tar"
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
    Array[File] region_haps_tar_gzs
    Pop sel_pop
    Pop alt_pop

    #File? xpehh_bins

    File script
    ComputeResources compute_resources
    String docker
    Int preemptible
  }
  #String out_basename = basename(region_haps_tar_gz) + "__selpop_" + sel_pop.pop_id + "__altpop_" + alt_pop.pop_id

  #String xpehh_out_fname = out_basename + ".xpehh.out"
  #String xpehh_log_fname = out_basename + ".xpehh.log"

  #String fst_and_delDAF_out_fname = out_basename + ".fst_and_delDAF.tsv"

# ** command
  command <<<
    python3 "~{script}" --region-haps-tar-gzs @~{write_lines(region_haps_tar_gzs)} \
        --sel-pop ~{sel_pop.pop_id} --alt-pop ~{alt_pop.pop_id} \
        --threads ~{compute_resources.cpus} --components xpehh fst delDAF --checkpoint-file checkpoint.tar
  >>>

# ** outputs
  output {
    Array[File] replicaInfos = glob("hapset[0-9]*/*.replicaInfo.json")
    Array[File] xpehh = glob("hapset[0-9]*/*.xpehh.out")
    Array[File] xpehh_log = glob("hapset[0-9]*/*.xpehh.log")
    Array[File] fst_and_delDAF = glob("hapset[0-9]*/*.fst_and_delDAF.tsv")
    Pop sel_pop_used = sel_pop
    Pop alt_pop_used = alt_pop
  }

# ** runtime
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: select_first([compute_resources.mem_gb, 4]) + " GB"
    cpu: select_first([compute_resources.cpus, 1])
    disks: "local-disk " + select_first([compute_resources.local_storage_gb, 50]) + " HDD"
    checkpointFile: "checkpoint.tar"
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
    disks: "local-disk " + local_disk_gb + " HDD"
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
    disks: "local-disk " + local_disk_gb + " HDD"
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
  String replica_id_str = basename(inp.ihs_out, ".ihs.out")
  String normed_collated_stats_fname = replica_id_str + ".normed_and_collated.tsv"
  command <<<
    python3 "~{normalize_and_collate_script}" --input-json "~{write_json(inp)}" --replica-id-str "~{replica_id_str}" --out-normed-collated "~{normed_collated_stats_fname}"
  >>>  
  output {
    File replica_info = read_json(inp.replica_info_file)
    File normed_collated_stats = normed_collated_stats_fname
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "1 GB"
    cpu: 1
    disks: "local-disk 10 HDD"
  }
}

task normalize_and_collate_block {
  meta {
    description: "Normalize raw scores to neutral sims, and collate component scores into one table."
  }
  input {
    NormalizeAndCollateBlockInput inp
  }
  File normalize_and_collate_script = "./norm_and_collate_block.py"
  #String replica_id_str = basename(inp.ihs_out, ".ihs.out")
  #String normed_collated_stats_fname = replica_id_str + ".normed_and_collated.tsv"
  command <<<
    python3 "~{normalize_and_collate_script}" --input-json "~{write_json(inp)}"
  >>>  
  output {
    Array[File] replica_info = inp.replica_info
    Array[File] normed_collated_stats = glob("*.normed_and_collated.tsv")
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "1 GB"
    cpu: 1
    disks: "local-disk 10 HDD"
    preemptible: 1
  }
}

struct collate_stats_and_metadata_for_all_sel_sims_input {
    String experimentId
    Array[File] sel_normed_and_collated
    Array[File] replica_infos
}

task collate_stats_and_metadata_for_sel_sims_block {
  meta {
    description: "Collate component stats and metadata for a block of selection sims"
  }
  input {
    collate_stats_and_metadata_for_all_sel_sims_input inp
    File collate_stats_and_metadata_for_sel_sims_block_script = "./collate_stats_and_metadata_for_sel_sims_block.py"
  }
  #Int disk_size_gb = 2*size(inp.sel_normed_and_collated) + size(inp.replica_infos)
  #Int disk_size_max_gb = 4096
  #Int disk_size_capped_gb = if disk_size_gb < disk_size_max_gb then disk_size_gb else disk_size_max_gb
  command <<<
    python3 "~{collate_stats_and_metadata_for_sel_sims_block_script}" --input-json "~{write_json(inp)}" 
  >>>
  output {
    File hapsets_component_stats_h5 = inp.experimentId+".all_component_stats.h5"
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "4 GB"
    cpu: 1
    disks: "local-disk 25 HDD"
    preemptible: 1
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
    disks: "local-disk 1 HDD"
  }
}

task fetch_empirical_hapsets_from_1KG {
  meta {
    description: "Fetches empirical hapsets for specified regions from 1KG, converts to hapset format"
  }
  parameter_meta {
# ** inputs
    empirical_regions_bed: "(File) empirical regions to fetch.  Column 5 (score), if present, is interpreted as the name of the putatively selected population.  The same region may be listed multiple times to test for selection in multiple populations."
    # add: metadata to attach to all regions
    genetic_maps_tar_gz: "(File) genetic maps"
    pops_outgroups_json: "(File) map from each pop to the ones to compare it to"

# ** outputs
    empirical_hapset_tar_gzs: "(Array[File]) for each empirical region, a .tar.gz file containing one tped for each pop, and a *.replicaInfo.json file describing the hapset"
  }
  input {
    File empirical_regions_bed
    File genetic_maps_tar_gz = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/genetic_maps/hg19_maps.tar.gz"
    File pops_outgroups_json

    File fetch_empirical_regions_script = "./fetch_empirical_regions.py"
  }
  #Int disk_size_gb = 2*size(inp.sel_normed_and_collated) + size(inp.replica_infos)
  #Int disk_size_max_gb = 4096
  #Int disk_size_capped_gb = if disk_size_gb < disk_size_max_gb then disk_size_gb else disk_size_max_gb
  command <<<
    mkdir hsets
    python3 "~{fetch_empirical_regions_script}" --empirical-regions-bed "~{empirical_regions_bed}" --tmp-dir $PWD/hsets
  >>>
  output {
    Array[File] empirical_hapsets_tar_gzs = glob("hsets/*.hapset.tar.gz")
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    memory: "4 GB"
    cpu: 1
    disks: "local-disk 25 HDD"
    preemptible: 1
  }
}
