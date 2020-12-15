version 1.0

#
# tofix:

#   - add proper automated CI with deployment to terra and tagging of workflow versions by code version and saving of version as output
#   - add ability to run things with caching locally

#
#   - add collation step
#   - add automatic generation of summary plots
#        - including for the distribution of present-day freqs, and other params chosen from distributions
#

#   - fix odd ihs norm scores
#   - add taking max of relevant xpop scores

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
# jvitti's score norming incl xpop
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

  File        tpeds_tar_gz

  Boolean succeeded
  Float durationSeconds
}

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
    Array[File] tpeds_tar_gz = prefix(tpedPrefix + "__tar_gz__rep_", range(numRepsPerBlock))

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


# task run_sims_cosi2 {
#     meta {
#       description: "Run a set of cosi2 simulations for one or more demographic models."
#       author: "Ilya Shlyakhter"
#       email: "ilya_shl@alum.mit.edu"
#     }

#     parameter_meta {
#       paramFiles: "cosi2 parameter files specifying the demographic model (paramFileCommon is prepended to each)"
#       recombFile: "Recombination map from which map of each simulated region is sampled"
#       nreps: "Number of replicates for _each_ file in paramFiles"
#     }

#     input {
#       String experimentId = "default"
#       File paramFileCommon
#       String modelId = "model_"+basename(paramFileCommon, ".par")
#       Array[File] paramFiles
#       File recombFile
#       Int nreps = 1
#       Int maxAttempts = 10000000
#       Int numRepsPerBlock = 1
#       Int numCpusPerBlock = numRepsPerBlock
#       Int repTimeoutSeconds = 600
#       String       memoryPerBlock = "3 GB"
#       String       cosi2_docker = "quay.io/ilya_broad/dockstore-tool-cosi2@sha256:11df3a646c563c39b6cbf71490ec5cd90c1025006102e301e62b9d0794061e6a"
#       Int preemptible = 3
#       File         taskScript
#     }
#     Int numBlocks = nreps / numRepsPerBlock
#     #Array[String] paramFileCommonLines = read_lines(paramFileCommonLines)

#     scatter(paramFile_blockNum in cross(paramFiles, range(numBlocks))) {
#       call cosi2_run_one_sim_block {
#         input:
#         paramFileCommon = paramFileCommon,
#         paramFile = paramFile_blockNum.left,
# 	recombFile=recombFile,
#         modelId=modelId+"_"+basename(paramFile_blockNum.left, ".par"),
# 	blockNum=paramFile_blockNum.right,
# 	simBlockId=modelId+"_"+basename(paramFile_blockNum.left, ".par")+"__block_"+paramFile_blockNum.right+"__of_"+numBlocks,
# 	numBlocks=numBlocks,
# 	maxAttempts=maxAttempts,
# 	repTimeoutSeconds=repTimeoutSeconds,
# 	numRepsPerBlock=numRepsPerBlock,
# 	numCpusPerBlock=numCpusPerBlock,
# 	memoryPerBlock=memoryPerBlock,
# 	cosi2_docker=cosi2_docker,
# 	preemptible=preemptible,
# 	taskScript=taskScript
#       }
#     }

#     output {
#       Array[ReplicaInfo] replicaInfos = flatten(cosi2_run_one_sim_block.replicaInfos)
#       Array[File] tpeds_tar_gz = flatten(cosi2_run_one_sim_block.tpeds_tar_gz)
#     }
# }

task compute_normalization_values {
  meta {
    description: "Compute CMS2 normalization values"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    String out_fnames_base
    Array[File]+ ihs_out
    Array[File]+ nsl_out
    Array[File]+ ihh12_out

    Int n_bins_ihs
    Int n_bins_nsl

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
    norm --ihh12 --files @~{write_lines(ihh12_out)} --save-bins ~{out_fnames_base}.norm_bins_ihh12.dat --only-save-bins --log ~{out_fnames_base}.norm_bins_ihh12.log
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


task compute_cms2_components_for_one_replica {
  meta {
    description: "Compute CMS2 component scores"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
#    ReplicaInfo replicaInfo
    File replica_output
    Int sel_pop
    File script
    File? ihs_bins
    File? nsl_bins
    File? ihh12_bins
    Int? n_bins_ihs
    Int n_bins_nsl

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
  String replica_id_string = basename(replica_output)
  String script_used_name = "script-used." + basename(script)
  String ihs_out_fname = replica_id_string + ".ihs.out"
  String ihs_normed_out_fname = replica_id_string + ".ihs.out." + n_bins_ihs + "bins.norm"
  String ihs_normed_out_log_fname = ihs_normed_out_fname + ".log"
  String nsl_normed_out_fname = replica_id_string + ".nsl.out." + n_bins_nsl + "bins.norm"
  String nsl_normed_out_log_fname = nsl_normed_out_fname + ".log"
  String ihh12_normed_out_fname = replica_id_string + ".ihh12.out.norm"
  String ihh12_normed_out_log_fname = ihh12_normed_out_fname + ".log"

  command <<<
    tar xvfz ~{replica_output}

    cp ~{script} ~{script_used_name}
    python3 ~{script} --replica-info *.replicaInfo.json --replica-id-string ~{replica_id_string} --sel-pop ~{sel_pop} --threads ~{threads} ~{"--ihs-bins " + ihs_bins} ~{"--nsl-bins " + nsl_bins} ~{"--ihh12-bins " + ihh12_bins} ~{"--n-bins-ihs " + n_bins_ihs} ~{"--n-bins-nsl " + n_bins_nsl}
  >>>

  output {
    Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
    File ihh12 = replica_id_string + ".ihh12.out"
    File ihs = ihs_out_fname
    File ihs_normed = ihs_normed_out_fname
    File ihs_normed_log = ihs_normed_out_log_fname
    File nsl = replica_id_string + ".nsl.out"
    File nsl_normed = nsl_normed_out_fname
    File nsl_normed_log = nsl_normed_out_log_fname
    File ihh12_normed = ihh12_normed_out_fname
    File ihh12_normed_log = ihh12_normed_out_log_fname
    Array[File] xpehh = glob("*.xpehh.out")
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

task create_tar_gz {
  meta {
    description: "Combine files into a tar file"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    Array[File] files
    String out_basename = "out"
    Int preemptible
  }
  String out_fname_tar_gz = out_basename + ".tar.gz"
  command <<<
    tar cvfz ~{out_fname_tar_gz} ~{sep=" " files}
  >>>
  output {
    File out_tar_gz = out_fname_tar_gz
  }
  runtime {
    docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    # docker: "ubuntu@sha256:c95a8e48bf88e9849f3e0f723d9f49fa12c5a00cfc6e60d2bc99d87555295e4c"
    preemptible: preemptible
    memory: "500 MB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

task get_pop_ids {
  meta {
    description: "Extract population ids from param file"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    File paramFile_demographic_model
    Array[File] paramFiles_selection
    File pop_ids_script
    Int preemptible
  }
  command <<<
    python3 ~{pop_ids_script} --dem-model ~{paramFile_demographic_model} --sweep-defs ~{sep=" " paramFiles_selection}
  >>>
  output {
    Array[Int] pop_ids = read_lines("pop_ids.txt")
    Array[String] pop_names = read_lines("pop_names.txt")
    Array[Int] sel_pop_ids = read_lines("sel_pop_ids.txt")
    Map[Int,Int] pop_id_to_idx = read_json("pop_id_to_idx.json")
  }
  runtime {
    docker: "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
    #docker: "ubuntu@sha256:c95a8e48bf88e9849f3e0f723d9f49fa12c5a00cfc6e60d2bc99d87555295e4c"
    preemptible: preemptible
    #docker: "python@sha256:665fe0313c2c76ee88308e6d186df0cda152000e7c141ba38a6da6c14b78c1fd"
    memory: "500 MB"
    cpu: 1
    disks: "local-disk 1 LOCAL"
  }
}

struct CMS2_Components_Result {
   ReplicaInfo replicaInfo
   File selection_sim_tar_gz
   File ihsout
   File ihsnormedout
   File nslout
   File nslnormedout
   File ihh12out
   File ihh12normedout
   Array[File] xpehhout
}

workflow run_sims_and_compute_cms2_components {
  meta {
    description: "Run simulations and compute CMS2 component scores"
    email: "ilya_shl@alum.mit.edu"
  }
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
    Int nreps = 1
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

    #Array[File] replica_outputs
    #Array[File] neutral_replica_outputs

    File script

    Int n_bins_ihs = 100
    Int n_bins_nsl = 100
    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    File pop_ids_script
    String docker = "quay.io/broadinstitute/cms2@sha256:0684c85ee72e6614cb3643292e79081c0b1eb6001a8264c446c3696a3a1dda97"
  }

  Int numBlocksNeutral = nreps_neutral / numRepsPerBlock
  Int numBlocks = nreps / numRepsPerBlock
  #Array[String] paramFileCommonLines = read_lines(paramFileCommonLines)
  
  call create_tar_gz as save_input_files {
    input:
       files = flatten([[paramFile_demographic_model, paramFile_neutral, recombFile, taskScript_simulation, script], paramFiles_selection]),
       out_basename = modelId,
       preemptible=preemptible
  }

  call get_pop_ids {
    input:
       paramFile_demographic_model = paramFile_demographic_model,
       paramFiles_selection = paramFiles_selection,
       pop_ids_script = pop_ids_script,
       preemptible=preemptible
  }

  ####################################################
  # Run neutral sims
  ####################################################

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

  ####################################################
  # Run selection sims
  ####################################################

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


  Array[Pair[ReplicaInfo,File]] neutral_sims = zip(flatten(run_neutral_sims.replicaInfos), flatten(run_neutral_sims.tpeds_tar_gz))
  scatter(sel_pop in get_pop_ids.pop_ids) {
    scatter(neut_sim in neutral_sims) {
      Boolean neut_sim_succeeded = neut_sim.left.succeeded
      if (neut_sim_succeeded) {
	call compute_cms2_components_for_one_replica as compute_cms2_components_for_neutral {
	  input:
	  sel_pop=sel_pop,
	  replica_output=neut_sim.right,
	  n_bins_ihs=n_bins_ihs,
	  n_bins_nsl=n_bins_nsl,
	  script=script,
	  threads=threads,
	  mem_base_gb=mem_base_gb,
	  mem_per_thread_gb=mem_per_thread_gb,
	  local_disk_gb=local_disk_gb,
	  docker=docker,
	  preemptible=preemptible
	}
      }
    }
  }

  scatter(pop_idx in range(length(get_pop_ids.pop_ids))) {
    call compute_normalization_values {
      input:
      out_fnames_base = modelId,
      ihs_out=select_all(compute_cms2_components_for_neutral.ihs[pop_idx]),
      nsl_out=select_all(compute_cms2_components_for_neutral.nsl[pop_idx]),
      ihh12_out=select_all(compute_cms2_components_for_neutral.ihh12[pop_idx]),
      n_bins_ihs=n_bins_ihs,
      n_bins_nsl=n_bins_nsl,
      threads=1,
      mem_base_gb=64,
      mem_per_thread_gb=0,
      local_disk_gb=local_disk_gb,
      docker=docker,
      preemptible=preemptible
    }
  }

  Array[Pair[ReplicaInfo,File]] selection_sims = zip(flatten(run_selection_sims.replicaInfos), flatten(run_selection_sims.tpeds_tar_gz))
  scatter(sel_sim in selection_sims) {
    ReplicaInfo replicaInfo = sel_sim.left
    if (replicaInfo.succeeded) {
      Int sel_pop = replicaInfo.modelInfo.sweepInfo.selPop
      Int sel_pop_idx = get_pop_ids.pop_id_to_idx[sel_pop]
      call compute_cms2_components_for_one_replica as compute_cms2_components_for_selection {
	input:
	sel_pop=sel_pop,
	replica_output=sel_sim.right,
	n_bins_ihs=n_bins_ihs,
	n_bins_nsl=n_bins_nsl,
	ihs_bins=compute_normalization_values.norm_bins_ihs[sel_pop_idx],
	nsl_bins=compute_normalization_values.norm_bins_nsl[sel_pop_idx],
	ihh12_bins=compute_normalization_values.norm_bins_ihh12[sel_pop_idx],
	script=script,
	threads=threads,
	mem_base_gb=mem_base_gb,
	mem_per_thread_gb=mem_per_thread_gb,
	local_disk_gb=local_disk_gb,
	docker=docker,
	preemptible=preemptible
      }
      CMS2_Components_Result sel_components_result = object {
	replicaInfo: replicaInfo,
	selection_sim_tar_gz: sel_sim.right,
	ihsout: compute_cms2_components_for_selection.ihs,
	ihsnormedout: compute_cms2_components_for_selection.ihs_normed,
	nslout: compute_cms2_components_for_selection.nsl,
	nslnormedout: compute_cms2_components_for_selection.nsl_normed,
	ihh12out: compute_cms2_components_for_selection.ihh12,
	ihh12normedout: compute_cms2_components_for_selection.ihh12_normed,
	xpehhout: compute_cms2_components_for_selection.xpehh
      }
    }
  }

  output {
    Array[File] neutral_sims_tar_gzs = flatten(run_neutral_sims.tpeds_tar_gz)
    Array[File] selection_sims_tar_gzs = flatten(run_selection_sims.tpeds_tar_gz)
    Array[ReplicaInfo] neutral_sims_replica_infos = flatten(run_neutral_sims.replicaInfos)
    Array[ReplicaInfo] selection_sims_replica_infos = flatten(run_selection_sims.replicaInfos)
    File saved_input_files = save_input_files.out_tar_gz
    Array[Int] pop_ids = get_pop_ids.pop_ids
    Array[String] pop_names = get_pop_ids.pop_names
    Array[Int] sel_pop_ids = get_pop_ids.sel_pop_ids
    Map[Int,Int] pop_id_to_idx = get_pop_ids.pop_id_to_idx
    
    Array[CMS2_Components_Result?] sel_components_results = sel_components_result
  }
}
