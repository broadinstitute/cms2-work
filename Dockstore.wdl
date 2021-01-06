version 1.0

# * Notes

#
# tofix:

#   - maybe do not store replicaInfos in metadata unless needed
#   - separate the component computation workflow since need to also do this for tpeds from real data
#     - though, could represent neutral real regions and neutral sims and selected real regions as selected sims, with the
#       putative selected pop indicated?   so, separate putative selpop for a sim, from the rest of sim info which
#       can be optional.

#   - compute normalization for both pop1-pop2 and pop2-pop1 since freq binning is only done on 

#   - add compression of scores files

#   - add proper automated CI with deployment to terra and tagging of workflow versions by code version and saving of version as output
#   - add ability to run things with caching locally

#
#   - add collation step
#   - add automatic generation of summary plots
#        - including for the distribution of present-day freqs, and other params chosen from distributions
#

#   - fix odd ihs norm scores
#   - add taking max of relevant xpehh scores

#   - add ability to define super-pops for comparison
#   - add steps to compute 

#
#   - break up computation of components
#

#
#   - fix norm program to:
#      - not load all files in memory
#      - use accurate accumulators for sum and sum-of-sq


#
# Terms and abbreviations:
#
#    haps - haplotypes
#    sims - simulations
#    reps - simulation replicas
#    comps, components - component test statistics of CMS
#

#
# jvitti's score norming incl xpehh
# /idi/sabeti-scratch/jvitti/cms/cms/dists/scores_func.py
#

#
#      - add norming of metrics beyond ihs
#

#   - fix workflow:
#      - group norm computation into blocks, then flatten
#

#
# WDL workflow: cms2_component_scores
#
# Computation of CMS2 component scores
#

# * Structs

struct SweepInfo {
  Int  selPop
  Float selGen
  Int selBegPop
  Float selBegGen
  Float selCoeff
  Float selFreq
}

struct ModelInfo {
  String modelId
  Array[String] modelIdParts
  Array[Int] popIds
  Array[String] popNames
  SweepInfo sweepInfo
}

struct ReplicaId {
  Int replicaNumGlobal
  Int replicaNumGlobalOutOf
  Int blockNum
  Int replicaNumInBlock
  Int randomSeed
}

struct ReplicaInfo {
  ReplicaId replicaId
  ModelInfo modelInfo

  File        region_haps_tar_gz

  Boolean succeeded
  Float durationSeconds
}

# * task cosi2_run_one_sim_block 
task cosi2_run_one_sim_block {
  meta {
    description: "Run one block of cosi2 simulations for one demographic model."
    email: "ilya_shl@alum.mit.edu"
  }

  parameter_meta {
    # Inputs
    ## required
    paramFile: "parts cosi2 parameter file (concatenated to form the parameter file)"
    recombFile: "recombination map"
    simBlockId: "an ID of this simulation block (e.g. block number in a list of blocks)."

    ## optional
    numRepsPerBlock: "number of simulations in this block"
    maxAttempts: "max number of attempts to simulate forward frequency trajectory before failing"

    # Outputs
    replicaInfos: "array of replica infos"
  }

  input {
    File         paramFileCommon
    File         paramFile
    File         recombFile
    String       simBlockId
    String       modelId
    Int          blockNum
    Int          numBlocks
    Int          numRepsPerBlock = 1
    Int          numCpusPerBlock = numRepsPerBlock
    Int          maxAttempts = 10000000
    Int          repTimeoutSeconds = 300
    String       cosi2_docker = "quay.io/ilya_broad/dockstore-tool-cosi2@sha256:11df3a646c563c39b6cbf71490ec5cd90c1025006102e301e62b9d0794061e6a"
    String       memoryPerBlock = "3 GB"
    Int          preemptible = 3
    File         taskScript
  }

  String tpedPrefix = "tpeds__${simBlockId}"

  command <<<
    python3 ~{taskScript} --paramFileCommon ~{paramFileCommon} --paramFile ~{paramFile} --recombFile ~{recombFile} \
      --simBlockId ~{simBlockId} --modelId ~{modelId} --blockNum ~{blockNum} --numRepsPerBlock ~{numRepsPerBlock} --numBlocks ~{numBlocks} --maxAttempts ~{maxAttempts} --repTimeoutSeconds ~{repTimeoutSeconds} --tpedPrefix ~{tpedPrefix} --outJson replicaInfos.json
  >>>

  output {
    Array[ReplicaInfo] replicaInfos = read_json("replicaInfos.json").replicaInfos
    Array[File] region_haps_tar_gzs = prefix(tpedPrefix + "__tar_gz__rep_", range(numRepsPerBlock))

#    String      cosi2_docker_used = ""
  }
  runtime {
#    docker: "quay.io/ilya_broad/cms-dev:2.0.1-15-gd48e1db-is-cms2-new"
    docker: cosi2_docker
    memory: memoryPerBlock
    cpu: numCpusPerBlock
    dx_instance_type: "mem1_ssd1_v2_x4"
    preemptible: preemptible
    volatile: true  # FIXME: not volatile if random seeds specified
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
    docker: docker
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
    docker: docker
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
#     docker: docker
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
    docker: docker
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

# ** command
  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --out-basename "~{out_basename}" \
        --replica-id-string "~{out_basename}" --sel-pop ~{sel_pop} --alt-pop ~{alt_pop} \
        --threads ~{threads} --components xpehh
  >>>

# ** outputs
  output {
    #Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
    File xpehh = xpehh_out_fname
    File xpehh_log = xpehh_log_fname
    Int sel_pop_used = sel_pop
    Int alt_pop_used = alt_pop
    #Array[File] xpehh = glob("*.xpehh.out")
    Int threads_used = threads
    File script_used = script_used_name
  }

# ** runtime
  runtime {
    docker: docker
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
    docker: "quay.io/ilya_broad/cms@sha256:a63e96a65ab6245e355b2dac9281908bed287a8d2cabb4668116198c819318c8"  # v1.3.0a04pd
    #docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    # docker: "ubuntu@sha256:c95a8e48bf88e9849f3e0f723d9f49fa12c5a00cfc6e60d2bc99d87555295e4c"
    memory: "500 MB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

# * task get_pops_info

#
# ** struct PopsInfo
#
# Information about population ids and names.
#
# Each pop has: a pop id (a small integer); a pop name (a string); a pop index
# (0-based index of the pop in the list of pop ids).
#
struct PopsInfo {
    Array[Int] pop_ids  # population IDs, used throughout to identify populations
    Array[String] pop_names
    Map[Int,Int] pop_id_to_idx  # map from pop id to its index in pop_ids
    Map[Int,Array[Int]] pop_alts  # map from pop id to list of all other pop ids
    Array[Pair[Int,Int]] pop_pairs # all two-pop sets, for cross-pop comparisons

    Array[Int] sel_pop_ids  # for each sweep definition in paramFiles_selection input to
    # workflow run_sims_and_compute_cms2_components, the pop id of the pop in which selection is defined.
}

# ** task get_pops_info implemenation
task get_pops_info {
  meta {
    description: "Extract population ids from cosi2 simulator param file"
  }
  input {
    File paramFile_demographic_model
    Array[File] paramFiles_selection

    File get_pops_info_script
  }
  String modelId = "model_"+basename(paramFile_demographic_model, ".par")
  String pops_info_fname = modelId + ".pops_info.json"
  command <<<
    python3 "~{get_pops_info_script}" --dem-model "~{paramFile_demographic_model}" \
       --sweep-defs ~{sep=" " paramFiles_selection} --out-pops-info "~{pops_info_fname}"
    touch empty_file
  >>>
  output {
    PopsInfo pops_info = read_json("${pops_info_fname}")["pops_info"]
    File empty_file = "empty_file"
  }
  runtime {
    #docker: "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    docker: "quay.io/ilya_broad/cms@sha256:a63e96a65ab6245e355b2dac9281908bed287a8d2cabb4668116198c819318c8"  # v1.3.0a04pd
    #docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
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
    docker: "quay.io/ilya_broad/cms@sha256:a63e96a65ab6245e355b2dac9281908bed287a8d2cabb4668116198c819318c8"  # v1.3.0a04pd
    #docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    memory: "1 GB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

# * struct CMS2_Components_Result
struct CMS2_Components_Result {
   ReplicaInfo replicaInfo
   File selection_sim_tar_gz
   File ihsout
   File ihsnormedout
   File nslout
   File nslnormedout
   File ihh12out
   File ihh12normedout
   #Array[File] xpehhout
}

# * workflow run_sims_and_compute_cms2_components
workflow run_sims_and_compute_cms2_components {
  meta {
    description: "Run simulations and compute CMS2 component scores"
    email: "ilya_shl@alum.mit.edu"
  }
# ** parameter_meta
  parameter_meta {
    experimentId: "String identifying this computational experiment; used to name output files."
    experiment_description: "Free-from string describing the analysis"
    paramFile_demographic_model: "The unvarying part of the parameter file"
    modelId: "String identifying the demographic model"
    paramFiles_selection: "The varying part of the parameter file, appended to paramFileCommon; first element represents neutral model."
    recombFile: "Recombination map from which map of each simulated region is sampled"
    nreps_neutral: "Number of neutral replicates to simulate"
    nreps: "Number of replicates for _each_ non-neutral file in paramFiles"
  }

# ** inputs
  input {
    #
    # Simulation params
    #

    String experimentId = "default"
    String experiment_description = "an experiment"
    File paramFile_demographic_model
    File paramFile_neutral
    String modelId = "model_"+basename(paramFile_demographic_model, ".par")
    Array[File] paramFiles_selection
    File recombFile
    Int nreps_neutral
    Int nreps
    Int maxAttempts = 10000000
    Int numRepsPerBlock = 1
    Int numCpusPerBlock = numRepsPerBlock
    Int repTimeoutSeconds = 600
    String       memoryPerBlock = "3 GB"
    String       cosi2_docker = "quay.io/ilya_broad/dockstore-tool-cosi2@sha256:11df3a646c563c39b6cbf71490ec5cd90c1025006102e301e62b9d0794061e6a"
    Int preemptible = 3
    File taskScript_simulation

    #
    # Component score computation params
    #

    #Array[File] region_haps_tar_gzs
    #Array[File] neutral_region_haps_tar_gzs

    File compute_components_script = "https://raw.githubusercontent.com/notestaff/dockstore-tool-cms2/1dd4ce3f72d0058abfbe3d8cd19001798b2c8390/remodel_components.py"

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    File get_pops_info_script = "https://raw.githubusercontent.com/notestaff/dockstore-tool-cms2/1dd4ce3f72d0058abfbe3d8cd19001798b2c8390/get_pops_info.py"
    File normalize_and_collate_script = "https://raw.githubusercontent.com/notestaff/dockstore-tool-cms2/1dd4ce3f72d0058abfbe3d8cd19001798b2c8390/norm_and_collate.py"
    #String docker = "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    String docker = "quay.io/ilya_broad/cms@sha256:a63e96a65ab6245e355b2dac9281908bed287a8d2cabb4668116198c819318c8"  # v1.3.0a04pd
    #String docker = "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
  }
  Int n_bins_ihh12 = 1
  Int n_bins_xpehh = 1

  #Array[String] paramFileCommonLines = read_lines(paramFileCommonLines)

# ** Bookkeeping calls
# *** call create_tar_gz as save_input_files
  call create_tar_gz as save_input_files {
    input:
       files = flatten([[paramFile_demographic_model, paramFile_neutral, recombFile, taskScript_simulation, compute_components_script,
                         get_pops_info_script, normalize_and_collate_script],
                        paramFiles_selection]),
       out_basename = modelId
  }

# *** call get_pops_info
  call get_pops_info {
    input:
       paramFile_demographic_model = paramFile_demographic_model,
       paramFiles_selection = paramFiles_selection,
       get_pops_info_script = get_pops_info_script
  }

  #PopsInfo pops_info = get_pops_info.pops_info
  # Array[Int] pop_ids = pops_info.pop_ids
  # Array[Int] pop_idxes = range(length(pop_ids))
  # Int n_pops = length(pop_ids)
  # Array[Pair[Int, Int]] pop_pairs = pops_info.pop_pairs
  # Int n_pop_pairs = length(pop_pairs)

  ####################################################
  # Run neutral sims
  ####################################################

# ** Run neutral sims
  Int numBlocksNeutral = nreps_neutral / numRepsPerBlock
  scatter(blockNum in range(numBlocksNeutral)) {
    call cosi2_run_one_sim_block as run_neutral_sims {
      input:
      paramFileCommon = paramFile_demographic_model,
      paramFile = paramFile_neutral,
      recombFile=recombFile,

      modelId=modelId+"_neutral",
      blockNum=blockNum,
      simBlockId=modelId+"_neutral__block_"+blockNum+"__of_"+numBlocksNeutral,
      numBlocks=numBlocksNeutral,

      maxAttempts=maxAttempts,
      repTimeoutSeconds=repTimeoutSeconds,
      numRepsPerBlock=numRepsPerBlock,
      numCpusPerBlock=numCpusPerBlock,
      memoryPerBlock=memoryPerBlock,
      cosi2_docker=cosi2_docker,
      preemptible=preemptible,
      taskScript=taskScript_simulation
    }
  }

# *** Gather successful neutral sims
  Array[Pair[ReplicaInfo,File]] neutral_sims = 
      zip(flatten(run_neutral_sims.replicaInfos), flatten(run_neutral_sims.region_haps_tar_gzs))

  scatter(neut_sim in neutral_sims) {
    if (neut_sim.left.succeeded) {
      File neut_sim_region_haps_tar_gz_maybe = neut_sim.right
    }
  }
  Array[File] neut_sim_region_haps_tar_gzs = select_all(neut_sim_region_haps_tar_gz_maybe)

  ####################################################
  # Run selection sims
  ####################################################

# ** Run selection sims
  Int numBlocks = nreps / numRepsPerBlock
  scatter(paramFile_blockNum in cross(paramFiles_selection, range(numBlocks))) {
    call cosi2_run_one_sim_block as run_selection_sims {
      input:
      paramFileCommon = paramFile_demographic_model,
      paramFile = paramFile_blockNum.left,
      recombFile=recombFile,
      modelId=modelId+"_"+basename(paramFile_blockNum.left, ".par"),
      blockNum=paramFile_blockNum.right,
      simBlockId=modelId+"_"+basename(paramFile_blockNum.left, ".par")+"__block_"+paramFile_blockNum.right+"__of_"+numBlocks,
      numBlocks=numBlocks,
      maxAttempts=maxAttempts,
      repTimeoutSeconds=repTimeoutSeconds,
      numRepsPerBlock=numRepsPerBlock,
      numCpusPerBlock=numCpusPerBlock,
      memoryPerBlock=memoryPerBlock,
      cosi2_docker=cosi2_docker,
      preemptible=preemptible,
      taskScript=taskScript_simulation
    }
  }

  ####################################################
  # Compute CMS2 component stats for neutral sims
  ####################################################

# ** Compute normalization stats
# *** Compute one-pop CMS2 components for neutral sims
  scatter(sel_pop in get_pops_info.pops_info.pop_ids) {
    scatter(neut_sim_region_haps_tar_gz in neut_sim_region_haps_tar_gzs) {
      call compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_neutral {
	input:
	sel_pop=sel_pop,
	region_haps_tar_gz=neut_sim_region_haps_tar_gz,

	script=compute_components_script,
	threads=threads,
	mem_base_gb=mem_base_gb,
	mem_per_thread_gb=mem_per_thread_gb,
	local_disk_gb=local_disk_gb,
	docker=docker,
	preemptible=preemptible
      }
    }

# *** Compute normalization stats for one-pop components for neutral sims
    call compute_one_pop_bin_stats_for_normalization {
      input:
      out_fnames_base = modelId + "__selpop_" + sel_pop,
      sel_pop=sel_pop,

      ihs_out=compute_one_pop_cms2_components_for_neutral.ihs,
      nsl_out=compute_one_pop_cms2_components_for_neutral.nsl,
      ihh12_out=compute_one_pop_cms2_components_for_neutral.ihh12,

      n_bins_ihs=n_bins_ihs,
      n_bins_nsl=n_bins_nsl,
      n_bins_ihh12=n_bins_ihh12,

      threads=1,
      mem_base_gb=64,
      mem_per_thread_gb=0,
      local_disk_gb=local_disk_gb,
      docker=docker,
      preemptible=preemptible
    }
  }

# *** Compute two-pop CMS2 components for neutral sims
   scatter(sel_pop_idx in range(length(get_pops_info.pops_info.pop_ids))) {
     scatter(alt_pop_idx in range(length(get_pops_info.pops_info.pop_ids))) {
       if (alt_pop_idx > sel_pop_idx) {
	 scatter(neut_sim_region_haps_tar_gz in neut_sim_region_haps_tar_gzs) {
	   call compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_neutral {
	     input:
	     sel_pop=get_pops_info.pops_info.pop_ids[sel_pop_idx],
	     alt_pop=get_pops_info.pops_info.pop_ids[alt_pop_idx],
	     region_haps_tar_gz=neut_sim_region_haps_tar_gz,
	     
	     script=compute_components_script,
	     threads=threads,
	     mem_base_gb=mem_base_gb,
	     mem_per_thread_gb=mem_per_thread_gb,
	     local_disk_gb=local_disk_gb,
	     docker=docker,
	     preemptible=preemptible
	   }
	 }

	 call compute_two_pop_bin_stats_for_normalization {
	   input:
	   out_fnames_base = modelId,
	   sel_pop=get_pops_info.pops_info.pop_ids[sel_pop_idx],
	   alt_pop=get_pops_info.pops_info.pop_ids[alt_pop_idx],

	   xpehh_out=compute_two_pop_cms2_components_for_neutral.xpehh,

	   n_bins_xpehh=n_bins_xpehh,

	   threads=1,
	   mem_base_gb=64,
	   mem_per_thread_gb=0,
	   local_disk_gb=local_disk_gb,
	   docker=docker,
	   preemptible=preemptible
	 }
       }
     }
  }

  scatter(sel_pop_idx in range(length(get_pops_info.pops_info.pop_ids))) {
    scatter(alt_pop_idx in range(length(get_pops_info.pops_info.pop_ids))) {
      if (alt_pop_idx != sel_pop_idx) {
	File norm_bins_xpehh_maybe = 
        select_first([
        compute_two_pop_bin_stats_for_normalization.norm_bins_xpehh[sel_pop_idx][alt_pop_idx],
        compute_two_pop_bin_stats_for_normalization.norm_bins_flip_pops_xpehh[alt_pop_idx][sel_pop_idx]
        ])
      }
    }
    Array[File] norm_bins_xpehh = select_all(norm_bins_xpehh_maybe)
  }

# ** Component stats for selection sims
  Array[Pair[ReplicaInfo,File]] selection_sims = zip(flatten(run_selection_sims.replicaInfos), flatten(run_selection_sims.region_haps_tar_gzs))

  scatter(sel_sim in selection_sims) {
    ReplicaInfo sel_sim_replicaInfo = sel_sim.left
    if (sel_sim_replicaInfo.succeeded) {
      Int sel_pop = sel_sim_replicaInfo.modelInfo.sweepInfo.selPop
      Int sel_pop_idx = pops_info.pop_id_to_idx[sel_pop]
      File sel_sim_region_haps_tar_gz = sel_sim.right
      String sel_sim_replica_id_str = modelId + "__selpop_" + sel_pop + "__rep_" + sel_sim_replicaInfo.replicaId.replicaNumGlobal
      call compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_selection {
	input:
	sel_pop=sel_pop,
	region_haps_tar_gz=sel_sim_region_haps_tar_gz,

	script=compute_components_script,
	threads=threads,
	mem_base_gb=mem_base_gb,
	mem_per_thread_gb=mem_per_thread_gb,
	local_disk_gb=local_disk_gb,
	docker=docker,
	preemptible=preemptible
      }
      scatter(alt_pop_idx in range(length(get_pops_info.pops_info.pop_ids))) {
	if (alt_pop_idx != sel_pop_idx) {
	  call compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_selection {
	    input:
	    sel_pop=sel_pop,
	    alt_pop=get_pops_info.pops_info.pop_ids[alt_pop_idx],
	    region_haps_tar_gz=sel_sim.right,

	    script=compute_components_script,
	    threads=threads,
	    mem_base_gb=mem_base_gb,
	    mem_per_thread_gb=mem_per_thread_gb,
	    local_disk_gb=local_disk_gb,
	    docker=docker,
	    preemptible=preemptible
	  }
	}
      }

      # should normalize_and_collate be done by blocks?
      call normalize_and_collate {
	input:
	  inp = object {
	    replica_id_str: sel_sim_replica_id_str,
	    pop_ids: get_pops_info.pops_info.pop_ids,
	    pop_pairs: get_pops_info.pops_info.pop_pairs,
	    sel_pop: sel_pop,

	    ihs_out: compute_one_pop_cms2_components_for_selection.ihs,
	    nsl_out: compute_one_pop_cms2_components_for_selection.nsl,
	    ihh12_out: compute_one_pop_cms2_components_for_selection.ihh12,

	    xpehh_out: select_all(compute_two_pop_cms2_components_for_selection.xpehh),

	    n_bins_ihs: n_bins_ihs,
	    n_bins_nsl: n_bins_nsl,
	    n_bins_ihh12: n_bins_ihh12,
	    n_bins_xpehh: n_bins_xpehh,

	    norm_bins_ihs: compute_one_pop_bin_stats_for_normalization.norm_bins_ihs[sel_pop_idx],
	    norm_bins_nsl: compute_one_pop_bin_stats_for_normalization.norm_bins_nsl[sel_pop_idx],
	    norm_bins_ihh12: compute_one_pop_bin_stats_for_normalization.norm_bins_ihh12[sel_pop_idx],

	    norm_bins_xpehh: norm_bins_xpehh[sel_pop_idx]
	  },
	  normalize_and_collate_script=normalize_and_collate_script
      }

      # CMS2_Components_Result sel_components_result = object {
      # 	replicaInfo: replicaInfo,
      # 	selection_sim_tar_gz: sel_sim.right,
      # 	ihsout: compute_cms2_components_for_selection.ihs,
      # 	ihsnormedout: compute_cms2_components_for_selection.ihs_normed,
      # 	nslout: compute_cms2_components_for_selection.nsl,
      # 	nslnormedout: compute_cms2_components_for_selection.nsl_normed,
      # 	ihh12out: compute_cms2_components_for_selection.ihh12,
      # 	ihh12normedout: compute_cms2_components_for_selection.ihh12_normed
      # 	#,
      # 	#xpehhout: compute_cms2_components_for_selection.xpehh
      # }
    }
  }

# ** Workflow outputs
  output {
# *** Bookkeeping outputs
    File saved_input_files = save_input_files.out_tar_gz
    PopsInfo pops_info = get_pops_info.pops_info
# *** Simulation outputs
    Array[File] neutral_sims_tar_gzs = flatten(run_neutral_sims.region_haps_tar_gzs)
    Array[File] selection_sims_tar_gzs = flatten(run_selection_sims.region_haps_tar_gzs)
    #Array[ReplicaInfo] neutral_sims_replica_infos = flatten(run_neutral_sims.replicaInfos)
    #Array[ReplicaInfo] selection_sims_replica_infos = flatten(run_selection_sims.replicaInfos)
    #Int n_neutral_sims_succeeded = length(select_all(compute_cms2_components_for_neutral.ihs[0]))
# *** Component scores
    Array[File?] sel_normed_and_collated = normalize_and_collate.normed_collated_stats
    Array[File?] sel_sim_region_haps_tar_gzs = sel_sim_region_haps_tar_gz
    #Array[CMS2_Components_Result?] sel_components_results = sel_components_result
  }
}
