version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--c0584fc4afe9a0fd10838936725259cdc3acdd49/structs.wdl"

# * task compute_one_pop_cms2_components
task compute_one_pop_cms2_components {
  meta {
    description: "Compute one-pop CMS2 component scores assuming selection in a given pop"
  }
  input {
    Array[File]+ hapsets
    Pop sel_pop
  }
  File script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/compute_cms2_components.py"

  command <<<
    set -ex -o pipefail

    python3 "~{script}" --hapsets @~{write_lines(hapsets)} \
      --sel-pop ~{sel_pop.pop_id} --components ihs nsl ihh12 delihh derFreq \
      --checkpoint-file "checkpoint.tar"
  >>>

  output {
    Array[File]+ replicaInfos = glob("hapset[0-9]*/*.replicaInfo.json")
    Array[File]+ ihs = glob("hapset[0-9]*/*.ihs.out")
    Array[File]+ nsl = glob("hapset[0-9]*/*.nsl.out")
    Array[File]+ ihh12 = glob("hapset[0-9]*/*.ihh12.out")
    Array[File]+ delihh = glob("hapset[0-9]*/*.delihh.out")
    Array[File]+ derFreq = glob("hapset[0-9]*/*.derFreq.tsv")
    Pop sel_pop_used = sel_pop
    Boolean sanity_check = ((length(replicaInfos) == length(hapsets)) &&
                            (length(ihs) == length(hapsets)) &&
                            (length(nsl) == length(hapsets)) &&
                            (length(ihh12) == length(hapsets)) &&
                            (length(delihh) == length(hapsets)) &&
                            (length(derFreq) == length(hapsets)))
    Array[Int]+ sanity_check_assert = if sanity_check then [1] else []
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: 3
    memory: "4 GB"
    cpu: 1
    disks: "local-disk 50 HDD"
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
    Array[File]+ hapsets
    Pop sel_pop
    Pop alt_pop
  }

  File script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/compute_cms2_components.py"

# ** command
  command <<<
    set -ex -o pipefail

    python3 "~{script}" --hapsets "@~{write_lines(hapsets)}" \
        --sel-pop "~{sel_pop.pop_id}" --alt-pop "~{alt_pop.pop_id}" \
        --components xpehh fst delDAF --checkpoint-file checkpoint.tar
  >>>

# ** outputs
  output {
    Array[File]+ replicaInfos = glob("hapset[0-9]*/*.replicaInfo.json")
    Array[File]+ xpehh = glob("hapset[0-9]*/*.xpehh.out")
    Array[File]+ xpehh_log = glob("hapset[0-9]*/*.xpehh.log")
    Array[File]+ fst_and_delDAF = glob("hapset[0-9]*/*.fst_and_delDAF.tsv")
    Pop sel_pop_used = sel_pop
    Pop alt_pop_used = alt_pop

    Boolean sanity_check = ((length(replicaInfos) == length(hapsets)) &&
                            (length(xpehh) == length(hapsets)) &&
                            (length(xpehh_log) == length(hapsets)) &&
                            (length(fst_and_delDAF) == length(hapsets)))
    Array[Int]+ sanity_check_assert = if sanity_check then [1] else []
  }

# ** runtime
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: 3
    memory: "8 GB"
    cpu: 8
    disks: "local-disk 50 HDD"
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
    String out_fnames_prefix
    Pop sel_pop
    Array[File]+ ihs_out
    Array[File]+ delihh_out
    Array[File]+ nsl_out
    Array[File]+ ihh12_out

    Int n_bins_ihs
    Int n_bins_nsl
    Int n_bins_delihh
  }
  Int n_bins_ihh12 = 1

  command <<<
    set -ex -o pipefail

    norm --ihs --bins ~{n_bins_ihs} --files "@~{write_lines(ihs_out)}" --save-bins "~{out_fnames_prefix}.norm_bins_ihs.dat" \
        --only-save-bins --log "~{out_fnames_prefix}.norm_bins_ihs.log"
    norm --ihs --bins ~{n_bins_delihh} --files "@~{write_lines(delihh_out)}" --save-bins "~{out_fnames_prefix}.norm_bins_delihh.dat" \
        --only-save-bins --log "~{out_fnames_prefix}.norm_bins_delihh.log"
    norm --nsl --bins ~{n_bins_nsl} --files "@~{write_lines(nsl_out)}" --save-bins "~{out_fnames_prefix}.norm_bins_nsl.dat" \
        --only-save-bins --log "~{out_fnames_prefix}.norm_bins_nsl.log"
    norm --ihh12 --bins ~{n_bins_ihh12} --files "@~{write_lines(ihh12_out)}" --save-bins "~{out_fnames_prefix}.norm_bins_ihh12.dat" \
        --only-save-bins --log "~{out_fnames_prefix}.norm_bins_ihh12.log"
  >>>

  output {
    File norm_bins_ihs = out_fnames_prefix + ".norm_bins_ihs.dat"
    File norm_bins_nsl = out_fnames_prefix + ".norm_bins_nsl.dat"
    File norm_bins_ihh12 = out_fnames_prefix + ".norm_bins_ihh12.dat"
    File norm_bins_delihh = out_fnames_prefix + ".norm_bins_delihh.dat"
    File norm_bins_ihs_log = out_fnames_prefix + ".norm_bins_ihs.log"
    File norm_bins_nsl_log = out_fnames_prefix + ".norm_bins_nsl.log"
    File norm_bins_ihh12_log = out_fnames_prefix + ".norm_bins_ihh12.log"
    File norm_bins_delihh_log = out_fnames_prefix + ".norm_bins_delihh.log"
    Pop sel_pop_used = sel_pop
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: 2
    memory: "8 GB"
    cpu: 1
    disks: "local-disk 50 HDD"
  }
}

# * task compute_two_pop_bin_stats_for_normalization
task compute_two_pop_bin_stats_for_normalization {
  meta {
    description: "Compute the means and stds of component scores on neutral sims, for a pop pair, for the purpose of normalization"
  }
  input {
    String out_fnames_prefix
    Pop sel_pop
    Pop alt_pop
    Array[File]+ xpehh_out
  }
  Int n_bins_xpehh = 1

  String norm_bins_xpehh_fname = "${out_fnames_prefix}__selpop_${sel_pop.pop_id}__altpop_${alt_pop.pop_id}.norm_bins_xpehh.dat"
  String norm_bins_xpehh_log_fname = "${out_fnames_prefix}__selpop_${sel_pop.pop_id}__altpop_${alt_pop.pop_id}.norm_bins_xpehh.dat"

  String norm_bins_flip_pops_xpehh_fname = "${out_fnames_prefix}__selpop_${alt_pop.pop_id}__altpop_${sel_pop.pop_id}.norm_bins_xpehh.dat"
  String norm_bins_flip_pops_xpehh_log_fname = 
  "${out_fnames_prefix}__selpop_${alt_pop.pop_id}__altpop_${sel_pop.pop_id}.norm_bins_xpehh.log"

  command <<<
    set -ex -o pipefail

    norm --xpehh --bins ~{n_bins_xpehh} --files "@~{write_lines(xpehh_out)}" --save-bins "~{norm_bins_xpehh_fname}" --only-save-bins \
        --log "~{norm_bins_xpehh_log_fname}"
    norm --xpehh --xpehh-flip-pops --bins ~{n_bins_xpehh} --files "@~{write_lines(xpehh_out)}" \
        --save-bins "~{norm_bins_flip_pops_xpehh_fname}" \
        --only-save-bins \
        --log "~{norm_bins_flip_pops_xpehh_log_fname}"
  >>>

  output {
    File norm_bins_xpehh = norm_bins_xpehh_fname
    File norm_bins_xpehh_log = norm_bins_xpehh_log_fname
    
    File norm_bins_flip_pops_xpehh = norm_bins_flip_pops_xpehh_fname
    File norm_bins_flip_pops_xpehh_log = norm_bins_flip_pops_xpehh_log_fname

    Pop sel_pop_used = sel_pop
    Pop alt_pop_used = alt_pop

    Pop flip_pops_sel_pop_used = alt_pop
    Pop flip_pops_alt_pop_used = sel_pop
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: 2
    memory: "8 GB"
    cpu: 1
    disks: "local-disk 50 HDD"
  }
}

# * task normalize_and_collate_block

task normalize_and_collate_block {
  meta {
    description: "Normalize raw scores to neutral sims, and collate component scores into one table."
  }
  input {
    NormalizeAndCollateBlockInput inp
  }
  File normalize_and_collate_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/norm_and_collate_block.py"
  command <<<
    set -ex -o pipefail

    python3 "~{normalize_and_collate_script}" --input-json "~{write_json(inp)}"
  >>>  
  output {
    Array[File]+ replica_info = inp.replica_info
    Array[File]+ normed_collated_stats = glob("*.normed_and_collated.tsv")
    Pop sel_pop_used = inp.sel_pop
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
    String out_fnames_prefix
    Array[File]+ sel_normed_and_collated
    Array[File]+ replica_infos
}

task collate_stats_and_metadata_for_sel_sims_block {
  meta {
    description: "Collate component stats and metadata for a block of selection sims"
  }
  input {
    collate_stats_and_metadata_for_all_sel_sims_input inp
  }
  File collate_stats_and_metadata_for_sel_sims_block_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/collate_stats_and_metadata_for_sel_sims_block.py"
  Int max_hapset_id_len = 256
  String hapsets_component_stats_h5_fname = inp.out_fnames_prefix + ".all_component_stats.h5"
  String hapsets_metadata_tsv_gz_fname = inp.out_fnames_prefix + ".hapsets_metadata.tsv.gz"
  command <<<
    set -ex -o pipefail

    python3 "~{collate_stats_and_metadata_for_sel_sims_block_script}" --input-json "~{write_json(inp)}" \
       --max-hapset-id-len ~{max_hapset_id_len} --hapsets-component-stats-h5-fname "~{hapsets_component_stats_h5_fname}" \
       --hapsets-metadata-tsv-gz-fname "~{hapsets_metadata_tsv_gz_fname}"
  >>>
  output {
    File hapsets_component_stats_h5 = hapsets_component_stats_h5_fname
    File hapsets_metadata_tsv_gz = hapsets_metadata_tsv_gz_fname
  }
  runtime {
    docker: "quay.io/ilya_broad/cms:cms2-docker-component-stats-master--a785a15af758d87e457557ceb7528305d0beb42a"
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
    Array[File]+ files
    String out_basename = "out"
  }
  String out_fname_tar_gz = out_basename + ".tar.gz"
  command <<<
    set -ex -o pipefail

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

task construct_pops_info_for_1KG {
  meta {
    description: "Constructs a PopsInfo struct for 1KG populations"
  }
  parameter_meta {
# ** inputs
    superpop_to_representative_pop_json: "(File) map from superpop to the pop used to represent it in model-fitting simulations"
    empirical_regions_bed: "(File) empirical regions to fetch.  Column 5 (score), if present, is interpreted as the name of the putatively selected population.  The same region may be listed multiple times to test for selection in multiple populations."

# ** outputs
    pops_info: "(PopsInfo) a PopsInfo struct giving info for 1KG pops"
  }
  input {
    File superpop_to_representative_pop_json = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/resources/superpop-to-representative-pop.json"
    File empirical_regions_bed
  }
  File construct_pops_info_for_1KG_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/construct_pops_info_for_1KG.py"
  String pops_info_fname = "pops_info.1KG.json"
  command <<<
    set -ex -o pipefail

    mkdir "${PWD}/hapsets"
    python3 "~{construct_pops_info_for_1KG_script}" --superpop-to-representative-pop-json "~{superpop_to_representative_pop_json}" \
       --empirical-regions-bed "~{empirical_regions_bed}" \
       --out-pops-info-json "~{pops_info_fname}"
  >>>
  output {
    PopsInfo pops_info = read_json("${pops_info_fname}")["pops_info"]
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:c8727e20ba0bc058c5c5596c4fad1ee23bc20c59f4f337ed62edb10e3a646010"  # selscan=1.3.0a09 with tabix
    memory: "16 GB"
    cpu: 1
    disks: "local-disk 256 HDD"
    preemptible: 1
  }
}

task fetch_empirical_hapsets_from_1KG {
  meta {
    description: "Fetches empirical hapsets for specified regions from 1KG, converts to hapset format"
  }
  parameter_meta {
# ** inputs
    pops_info: "(PopsInfo) information about 1KG pops, including the outgroups to compare with each pop"
    empirical_regions_bed: "(File) empirical regions to fetch.  Column 5 (score), if present, is interpreted as the name of the putatively selected population.  The same region may be listed multiple times to test for selection in multiple populations."
    sel_pop_id: "(String?) if not given, assume empirical_regions_bed are neutral, else take only regions with selection in this pop"
    # add: metadata to attach to all regions
    genetic_maps_tar_gz: "(File) genetic maps"
    superpop_to_representative_pop_json: "(File) map from superpop to the pop used to represent it in model-fitting simulations"

# ** outputs
    empirical_hapsets: "(Array[File]) for each empirical region, a .tar.gz file containing one tped for each pop, and a *.replicaInfo.json file describing the hapset"
  }
  input {
    PopsInfo pops_info
    String? sel_pop_id
    File empirical_regions_bed
    String out_fnames_prefix
    File genetic_maps_tar_gz = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/genetic_maps/hg19_maps.tar.gz"
    File superpop_to_representative_pop_json = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/resources/superpop-to-representative-pop.json"
  }
  File fetch_empirical_hapsets_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/fetch_empirical_hapsets.py"

  command <<<
    set -ex -o pipefail

    mkdir "${PWD}/hapsets"
    python3 "~{fetch_empirical_hapsets_script}" --empirical-regions-bed "~{empirical_regions_bed}" \
       --genetic-maps-tar-gz "~{genetic_maps_tar_gz}" --superpop-to-representative-pop-json "~{superpop_to_representative_pop_json}" \
       --out-fnames-prefix "~{out_fnames_prefix}" \
       ~{"--sel-pop " + sel_pop_id} \
       --tmp-dir "${PWD}/hapsets"
    df -h
  >>>
  output {
    Array[File]+ empirical_hapsets = glob("hapsets/*.hapset.tar.gz")
  }
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:c8727e20ba0bc058c5c5596c4fad1ee23bc20c59f4f337ed62edb10e3a646010"  # selscan=1.3.0a09 with tabix
    memory: "16 GB"
    cpu: 1
    disks: "local-disk 256 HDD"
    preemptible: 1
  }
}

# task construct_empirical_neutral_regions_list_gazave14 {
#   meta {
#     description: "Construct a .bed file representing neutral empirical regions based on Gazave 2014 paper."
#   }
# }

task call_neutral_region_explorer {
  meta {
    description: "Calls Neutral Region Explorer webserver"
  }
  parameter_meta {
# ** inputs
# ** outputs
  }
  input {
    NeutralRegionExplorerParams nre_params
    String out_fnames_prefix = "nre"
  }
  File fetch_neutral_regions_nre_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210914-1648-add-nre-wdl/c0584fc4afe9a0fd10838936725259cdc3acdd49/fetch_neutral_regions_nre.py"
  String neutral_regions_tsv_fname = out_fnames_prefix + ".neutral_regions.tsv"
  String neutral_regions_bed_fname = out_fnames_prefix + ".neutral_regions.bed"
  String nre_submitted_form_html_fname = out_fnames_prefix + ".submitted_form.html"

  command <<<
    set -ex -o pipefail

    python3 "~{fetch_neutral_regions_nre_script}" --nre-params "~{write_json(nre_params)}" \
       --neutral-regions-tsv "~{neutral_regions_tsv_fname}" \
       --neutral-regions-bed "~{neutral_regions_bed_fname}" \
       --nre-submitted-form-html "~{nre_submitted_form_html_fname}" \
  >>>
  output {
    File neutral_regions_tsv = neutral_regions_tsv_fname
    File neutral_regions_bed = neutral_regions_bed_fname
    File nre_submitted_form_html = nre_submitted_form_html_fname
  }
  runtime {
    docker: "quay.io/ilya_broad/cms:webdriver-0.1"  # selscan=1.3.0a09 with tabix
    memory: "4 GB"
    cpu: 1
    disks: "local-disk 32 HDD"
    preemptible: 1
  }
}

