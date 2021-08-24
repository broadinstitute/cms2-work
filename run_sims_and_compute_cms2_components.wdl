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

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--8e703a1b8b8fcb357459097a0e392881257dd534/run_sims.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--8e703a1b8b8fcb357459097a0e392881257dd534/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--8e703a1b8b8fcb357459097a0e392881257dd534/compute_normalization_stats.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--8e703a1b8b8fcb357459097a0e392881257dd534/component_stats_for_sel_sims.wdl"

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
    File taskScript_simulation = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210822-2127-add-pop-pair-match-checks/8e703a1b8b8fcb357459097a0e392881257dd534/runcosi.py"

    #
    # Component score computation params
    #

    #Array[File] region_haps_tar_gzs
    #Array[File] neutral_region_haps_tar_gzs

    File compute_components_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210822-2127-add-pop-pair-match-checks/8e703a1b8b8fcb357459097a0e392881257dd534/remodel_components.py"

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20
    Int n_bins_delihh = 20

    Map[String,Boolean] include_components = {"ihs": true, "ihh12": true, "nsl": true, "delihh": true, "xpehh": true, "fst": true, "delDAF": true, "derFreq": true}

    Int hapset_block_size = 2
    
    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    File get_pops_info_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210822-2127-add-pop-pair-match-checks/8e703a1b8b8fcb357459097a0e392881257dd534/get_pops_info.py"
    File normalize_and_collate_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210822-2127-add-pop-pair-match-checks/8e703a1b8b8fcb357459097a0e392881257dd534/norm_and_collate.py"
    String docker = "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09

    ComputeResources compute_resources_for_compute_one_pop_cms2_components = object {
      mem_gb: 4,
      cpus: 1,
      local_storage_gb: 50
    }
    ComputeResources compute_resources_for_compute_two_pop_cms2_components = object {
      mem_gb: 4,
      cpus: 1,
      local_storage_gb: 50
    }

  }

# ** Bookkeeping calls
# *** call create_tar_gz as save_input_files
  call tasks.create_tar_gz as save_input_files {
    input:
       files = flatten([[paramFile_demographic_model, paramFile_neutral, recombFile, taskScript_simulation, compute_components_script,
                         get_pops_info_script, normalize_and_collate_script],
                        paramFiles_selection]),
       out_basename = modelId
  }

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
    get_pops_info_script=get_pops_info_script
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
    n_bins_delihh=n_bins_delihh,

    hapset_block_size=hapset_block_size,

    compute_resources_for_compute_one_pop_cms2_components=compute_resources_for_compute_one_pop_cms2_components,
    compute_resources_for_compute_two_pop_cms2_components=compute_resources_for_compute_two_pop_cms2_components,
    docker=docker,
    preemptible=preemptible
  }

# ** Component stats for selection sims
  call component_stats_for_sel_sims.component_stats_for_sel_sims_wf {
    input:
    experimentId=experimentId,
    modelId=modelId,
    selection_sims = sims_wf.selection_sims_tar_gzs,
    pops_info = sims_wf.pops_info,

    n_bins_ihs=n_bins_ihs,
    n_bins_nsl=n_bins_nsl,
    n_bins_delihh=n_bins_delihh,

    norm_bins_ihs=compute_normalization_stats_wf.norm_bins_ihs,
    norm_bins_nsl=compute_normalization_stats_wf.norm_bins_nsl,
    norm_bins_ihh12=compute_normalization_stats_wf.norm_bins_ihh12,
    norm_bins_delihh=compute_normalization_stats_wf.norm_bins_delihh,
    norm_bins_xpehh=compute_normalization_stats_wf.norm_bins_xpehh,

    one_pop_bin_stats_sel_pop_used=compute_normalization_stats_wf.one_pop_bin_stats_sel_pop_used,
    two_pop_bin_stats_sel_pop_used=compute_normalization_stats_wf.two_pop_bin_stats_sel_pop_used,
    two_pop_bin_stats_alt_pop_used=compute_normalization_stats_wf.two_pop_bin_stats_alt_pop_used,

    compute_resources_for_compute_one_pop_cms2_components=compute_resources_for_compute_one_pop_cms2_components,
    compute_resources_for_compute_two_pop_cms2_components=compute_resources_for_compute_two_pop_cms2_components,
    docker=docker,
    preemptible=preemptible
  }

# ** Workflow outputs
  output {
# *** Bookkeeping outputs
    File saved_input_files = save_input_files.out_tar_gz
    PopsInfo pops_info = sims_wf.pops_info
# *** Simulation outputs
    #Array[File] neutral_sims_tar_gzs = sims_wf.neutral_sims_tar_gzs
    #Array[File] selection_sims_tar_gzs = sims_wf.selection_sims_tar_gzs
    #Array[ReplicaInfo] neutral_sims_replica_infos = flatten(run_neutral_sims.replicaInfos)
    #Array[ReplicaInfo] selection_sims_replica_infos = flatten(run_selection_sims.replicaInfos)
    #Int n_neutral_sims_succeeded = length(select_all(compute_cms2_components_for_neutral.ihs[0]))
# *** Component scores
    #Array[File?] sel_normed_and_collated = component_stats_for_sel_sims_wf.sel_normed_and_collated
    #Array[File?] sel_sim_region_haps_tar_gzs = component_stats_for_sel_sims_wf.sel_sim_region_haps_tar_gzs
    #Array[CMS2_Components_Result?] sel_components_results = sel_components_result
    Array[File] all_hapsets_component_stats_h5_blocks = 
    component_stats_for_sel_sims_wf.all_hapsets_component_stats_h5_blocks
  }
}
