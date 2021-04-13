version 1.0


import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210407-1304-add-sim-info-to-collate.org--5389b35ec5d3b6fec1fe5aea78d1ef702306e47a/run_sims_and_compute_cms2_components.wdl"

# * workflow run_sims_and_compute_cms2_components
workflow cms2_main {
  meta {
    description: "Run simulations and compute CMS2 component scores"
    email: "ilya_shl@alum.mit.edu"
  }

  parameter_meta {
# ** inputs
# *** Simulation params
# **** Specifying the demographic model and sweep scenarios
    paramFile_demographic_model: "(File) cosi2 parameter file specifying the demographic model; this file describes the parts common to neutral and selection simulations."
    paramFile_neutral: "(File) cosi2 parameter file portion used for neutral simulations only.  *paramFile_demographic_model* is prepended to it to create the full parameter file for neutral simulations."
    paramFiles_selection: "(Array[File]) cosi2 parameter file portions for selection simulations.   For each file in this array, a set of selection simulations will be done.  *paramFile_demographic_model* is prepended to this file to construct the full cosi2 parameter file."
# **** Identification information about the computational experiment
    experimentId: "(String) An arbitrary string identifying this (computational) experiment.  Change it to ensure that random seeds used for simulations are not reused."
    experiment_description: "(String) Experiment description, for recording purposes"
    modelId: "(String) A string identifying the demographic model; if not given, defaults to the base name of *paramFile_demographic_model*."
# **** Specifying how many simulations to create
    nreps_neutral: "(Int) Run this many neutral simulations, for establishing normalization stats."
    nreps: "(Int) Run this many selection simulations for each selection scenario in *paramFiles_selection*."
# **** Computational limits and resources for doing simulations
    maxAttempts: "(Int) Max number of times to attempt to generate an allele frequency trajectory for a given random seed"
    numRepsPerBlock: "(Int) Run this many simulations per block.  A block of simulations is run in a single task."
    numCpusPerBlock: "(Int) Allocate this many CPUs to each task running the simulations."
    memoryPerBlock: "(String) Memory spec for each task running a block of simulation replicas."
    repTimeoutSeconds: "(Int) Time out (and fail) a simulation replica for a given random seed after this many seconds."

# *** Component stats computation params
    n_bins: "Number of frequency bins for normalizing iHS, nSL and delIHH component statistics."
  }

# ** inputs
  input {
    #
    # Simulation params
    #
    File paramFile_demographic_model
    File paramFile_neutral
    Array[File] paramFiles_selection
    File recombFile
    String experimentId = "default"
    String experiment_description = "an experiment"
    String modelId = "model_"+basename(paramFile_demographic_model, ".par")

    Int nreps_neutral
    Int nreps

    Int maxAttempts = 10000000
    Int numRepsPerBlock = 1
    Int numCpusPerBlock = numRepsPerBlock
    Int repTimeoutSeconds = 600
    String       memoryPerBlock = "3 GB"

    Int n_bins = 20
  }

  call run_sims_and_compute_cms2_components.run_sims_and_compute_cms2_components_wf as main_call {
    input:
    paramFile_demographic_model=paramFile_demographic_model,
    paramFile_neutral=paramFile_neutral,
    paramFiles_selection=paramFiles_selection,
    recombFile=recombFile,
    nreps_neutral=nreps_neutral,
    nreps=nreps,

    n_bins_ihs=n_bins,
    n_bins_nsl=n_bins,
    n_bins_delihh=n_bins,

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

