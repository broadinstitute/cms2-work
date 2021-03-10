version 1.0


import "https://raw.githubusercontent.com/notestaff/dockstore-tool-cms2/88fec6145a02f8e41c2219dfef3415b5f607db9f-staging/run_sims_and_compute_cms2_components.wdl"

# * workflow run_sims_and_compute_cms2_components
workflow cms2_main {
  meta {
    description: "Run simulations and compute CMS2 component scores"
    email: "ilya_shl@alum.mit.edu"
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
  }

  call run_sims_and_compute_cms2_components.run_sims_and_compute_cms2_components_wf as main_call {
    input:
    paramFile_demographic_model=paramFile_demographic_model,
    paramFile_neutral=paramFile_neutral,
    paramFiles_selection=paramFiles_selection,
    recombFile=recombFile,
    nreps_neutral=nreps_neutral,
    nreps=nreps,
    numRepsPerBlock=numRepsPerBlock,
    numCpusPerBlock=numCpusPerBlock,
    memoryPerBlock=memoryPerBlock
  }

# ** Workflow outputs
  output {
# *** Bookkeeping outputs
#    File saved_input_files = main_call.saved_input_files
    PopsInfo pops_info = main_call.pops_info
# *** Simulation outputs
    #Array[File] neutral_sims_tar_gzs = main_call.neutral_sims_tar_gzs
    #Array[File] selection_sims_tar_gzs = main_call.selection_sims_tar_gzs
    #Array[ReplicaInfo] neutral_sims_replica_infos = flatten(run_neutral_sims.replicaInfos)
    #Array[ReplicaInfo] selection_sims_replica_infos = flatten(run_selection_sims.replicaInfos)
    #Int n_neutral_sims_succeeded = length(select_all(compute_cms2_components_for_neutral.ihs[0]))
# *** Component scores
    Array[File?] sel_normed_and_collated = main_call.sel_normed_and_collated
    #Array[CMS2_Components_Result?] sel_components_results = sel_components_result
  }
}

