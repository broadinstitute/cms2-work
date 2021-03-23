version 1.0

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
    docker: "quay.io/ilya_broad/cms@sha256:1834a9e5eb9db5253b4cf051c39d23e51ca6c3f812b6d17f5d2c87d9506f5e8a"  # selscan=1.3.0a06
    #docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    memory: "500 MB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

workflow run_sims_wf {
  meta {
    description: "Run simulations"
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
    File taskScript_simulation = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-210323-1646-add-missing-one-pop-stats-wip-01/e3c686ab1bcfe497b36a9f7213b68782091ee570/runcosi.py"


    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    File get_pops_info_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-210323-1646-add-missing-one-pop-stats-wip-01/e3c686ab1bcfe497b36a9f7213b68782091ee570/get_pops_info.py"
    #String docker = "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    String docker = "quay.io/ilya_broad/cms@sha256:1834a9e5eb9db5253b4cf051c39d23e51ca6c3f812b6d17f5d2c87d9506f5e8a"  # selscan=1.3.0a06
    #String docker = "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
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

# ** Workflow outputs
  output {
# *** Bookkeeping outputs
    PopsInfo pops_info = get_pops_info.pops_info
# *** Simulation outputs
    Array[File] neut_sim_region_haps_tar_gzs = select_all(neut_sim_region_haps_tar_gz_maybe)
    Array[Pair[ReplicaInfo,File]] selection_sims = 
        zip(flatten(run_selection_sims.replicaInfos),
            flatten(run_selection_sims.region_haps_tar_gzs))

    Array[File] neutral_sims_tar_gzs = flatten(run_neutral_sims.region_haps_tar_gzs)
    Array[File] selection_sims_tar_gzs = flatten(run_selection_sims.region_haps_tar_gzs)
    #Array[ReplicaInfo] neutral_sims_replica_infos = flatten(run_neutral_sims.replicaInfos)
    #Array[ReplicaInfo] selection_sims_replica_infos = flatten(run_selection_sims.replicaInfos)
    #Int n_neutral_sims_succeeded = length(select_all(compute_cms2_components_for_neutral.ihs[0]))
  }
}
