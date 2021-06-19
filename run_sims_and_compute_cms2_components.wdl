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

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-origin-broad/is-add-missing-one-pop-stats--42796591a8020e5fd3341778085522371edaf24c/run_sims.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-origin-broad/is-add-missing-one-pop-stats--42796591a8020e5fd3341778085522371edaf24c/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-origin-broad/is-add-missing-one-pop-stats--42796591a8020e5fd3341778085522371edaf24c/compute_normalization_stats.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-origin-broad/is-add-missing-one-pop-stats--42796591a8020e5fd3341778085522371edaf24c/component_stats_for_sel_sims.wdl"

# * workflow run_sims_and_compute_cms2_components
workflow run_sims_and_compute_cms2_components_wf {
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
    File taskScript_simulation = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/origin-broad/is-add-missing-one-pop-stats/42796591a8020e5fd3341778085522371edaf24c/runcosi.py"

    #
    # Component score computation params
    #

    #Array[File] region_haps_tar_gzs
    #Array[File] neutral_region_haps_tar_gzs

    File compute_components_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/origin-broad/is-add-missing-one-pop-stats/42796591a8020e5fd3341778085522371edaf24c/remodel_components.py"

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    File get_pops_info_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/origin-broad/is-add-missing-one-pop-stats/42796591a8020e5fd3341778085522371edaf24c/get_pops_info.py"
    File normalize_and_collate_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/origin-broad/is-add-missing-one-pop-stats/42796591a8020e5fd3341778085522371edaf24c/norm_and_collate.py"
    #String docker = "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    String docker = "quay.io/ilya_broad/cms@sha256:1834a9e5eb9db5253b4cf051c39d23e51ca6c3f812b6d17f5d2c87d9506f5e8a"  # selscan=1.3.0a06
    #String docker = "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
  }
  Int n_bins_ihh12 = 1
  Int n_bins_xpehh = 1

  #Array[String] paramFileCommonLines = read_lines(paramFileCommonLines)

# ** Bookkeeping calls
# *** call create_tar_gz as save_input_files
  call tasks.create_tar_gz as save_input_files {
    input:
       files = flatten([[paramFile_demographic_model, paramFile_neutral, recombFile, taskScript_simulation, compute_components_script,
                         get_pops_info_script, normalize_and_collate_script],
                        paramFiles_selection]),
       out_basename = modelId
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


# ** Call the simulations
  call run_sims.run_sims_wf as sims_wf {
    input:
    experimentId = experimentId,
    experiment_description = experiment_description,
    paramFile_demographic_model = paramFile_demographic_model,
    paramFile_neutral = paramFile_neutral,
    modelId=modelId,
    paramFiles_selection=paramFiles_selection,
    recombFile=recombFile,
    nreps_neutral=nreps_neutral,
    nreps=nreps,
    maxAttempts=maxAttempts,
    numRepsPerBlock=numRepsPerBlock,
    numCpusPerBlock=numCpusPerBlock,
    repTimeoutSeconds=repTimeoutSeconds,
    memoryPerBlock=memoryPerBlock,
    cosi2_docker=cosi2_docker,
    preemptible=preemptible,
    taskScript_simulation=taskScript_simulation,
    threads=threads,
    mem_base_gb=mem_base_gb,
    mem_per_thread_gb=mem_per_thread_gb,
    local_disk_gb=local_disk_gb,
    get_pops_info_script=get_pops_info_script,
    docker=docker
  }

# ** Compute normalization stats
# *** Compute one-pop CMS2 components for neutral sims
  call compute_normalization_stats.compute_normalization_stats_wf {
    input:
    modelId=modelId,
    pops_info=sims_wf.pops_info,
    neut_sim_region_haps_tar_gzs=sims_wf.neut_sim_region_haps_tar_gzs,

    n_bins_ihs=n_bins_ihs,
    n_bins_nsl=n_bins_nsl,
    n_bins_ihh12=n_bins_ihh12,
    n_bins_xpehh=n_bins_xpehh,

    threads=threads,
    mem_base_gb=mem_base_gb,
    mem_per_thread_gb=mem_per_thread_gb,
    local_disk_gb=local_disk_gb,
    docker=docker,
    preemptible=preemptible
  }

# ** Component stats for selection sims
  call component_stats_for_sel_sims.component_stats_for_sel_sims_wf {
    input:
    modelId=modelId,
    selection_sims = sims_wf.selection_sims,
    pops_info = sims_wf.pops_info,

    n_bins_ihs=n_bins_ihs,
    n_bins_nsl=n_bins_nsl,
    n_bins_ihh12=n_bins_ihh12,
    n_bins_xpehh=n_bins_xpehh,

    norm_bins_ihs=compute_normalization_stats_wf.norm_bins_ihs,
    norm_bins_nsl=compute_normalization_stats_wf.norm_bins_nsl,
    norm_bins_ihh12=compute_normalization_stats_wf.norm_bins_ihh12,
    norm_bins_xpehh=compute_normalization_stats_wf.norm_bins_xpehh,

    threads=threads,
    mem_base_gb=mem_base_gb,
    mem_per_thread_gb=mem_per_thread_gb,
    local_disk_gb=local_disk_gb,
    docker=docker,
    preemptible=preemptible
  }

# ** Workflow outputs
  output {
# *** Bookkeeping outputs
    File saved_input_files = save_input_files.out_tar_gz
    PopsInfo pops_info = sims_wf.pops_info
# *** Simulation outputs
    Array[File] neutral_sims_tar_gzs = sims_wf.neutral_sims_tar_gzs
    Array[File] selection_sims_tar_gzs = sims_wf.selection_sims_tar_gzs
    #Array[ReplicaInfo] neutral_sims_replica_infos = flatten(run_neutral_sims.replicaInfos)
    #Array[ReplicaInfo] selection_sims_replica_infos = flatten(run_selection_sims.replicaInfos)
    #Int n_neutral_sims_succeeded = length(select_all(compute_cms2_components_for_neutral.ihs[0]))
# *** Component scores
    Array[File?] sel_normed_and_collated = component_stats_for_sel_sims_wf.sel_normed_and_collated
    Array[File?] sel_sim_region_haps_tar_gzs = component_stats_for_sel_sims_wf.sel_sim_region_haps_tar_gzs
    #Array[CMS2_Components_Result?] sel_components_results = sel_components_result
  }
}
