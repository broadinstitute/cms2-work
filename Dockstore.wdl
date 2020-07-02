version 1.0

#
# WDL workflows for running population genetics simulations using cosi2
#

#
# TODO:
#
#   include metadata including selection start/stop/pop in workflow output as table
#   and muation age
#
#   figure out how to enable result caching without 
#

struct ReplicaInfo {
  String modelId
  Int blockNum
  Int replicaNum
  Int randomSeed

  Int  selPop
  Float selGen
  Int selBegPop
  Float selBegGen
  Float selCoeff
  Float selFreq

  Array[Int] pops
  Array[File] tpeds

  File        tpeds_tar_gz

  Int succeeded
  Float duration
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
    Array[Int]   pops
    Array[String] repPopSuffixes
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

  String tpedPrefix = "tpeds_${simBlockId}_tar_gz_"

  command <<<
    python3 ~{taskScript} --paramFileCommon ~{paramFileCommon} --paramFile ~{paramFile} --recombFile ~{recombFile} \
      --simBlockId ~{simBlockId} --modelId ~{modelId} --blockNum ~{blockNum} --numRepsPerBlock ~{numRepsPerBlock} --maxAttempts ~{maxAttempts} --repTimeoutSeconds ~{repTimeoutSeconds} --pops ~{sep=' ' pops } --outJson replicaInfos.json --tpedPrefix ~{tpedPrefix}
  >>>

  output {
    Array[ReplicaInfo] replicaInfos = read_json("replicaInfos.json").replicaInfos
    Array[File] tpeds_tar_gz = prefix(tpedPrefix, range(numRepsPerBlock))
    Array[File] tpeds = prefix("${simBlockId}.rep", repPopSuffixes)

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

task get_pops_from_cosi2_param_file {
  meta {
    description: "Get list of population IDs from cosi2 parameter file"
  }

  input {
    File paramFileCommon
    Int numRepsPerBlock
    String cosi2_docker
  }

  command <<<
    grep ^pop_define ~{paramFileCommon} | awk '{print $2;}' | tee pops.txt
    python3 <<CODE
    with open('pops.txt') as pops_in:
        pops = pops_in.read().strip().split()
    with open('suffixes.txt', 'w') as out:
        for rep, pop in zip(range(~{numRepsPerBlock}), pops):
            out.write(f"{rep}_0_{pop}.tped\n")
    CODE
  >>>
  output {
    Array[Int] popIds = read_lines(stdout())
    Array[String] repPopSuffixes = read_lines('suffixes.txt')
  }
  runtime {
    docker: cosi2_docker
  }
}


workflow run_sims_cosi2 {
    meta {
      description: "Run a set of cosi2 simulations for one or more demographic models."
      author: "Ilya Shlyakhter"
      email: "ilya_shl@alum.mit.edu"
    }

    parameter_meta {
      paramFiles: "cosi2 parameter files specifying the demographic model (paramFileCommon is prepended to each)"
      recombFile: "Recombination map from which map of each simulated region is sampled"
      nreps: "Number of replicates for _each_ file in paramFiles"
    }

    input {
      String experimentId = 'default'
      File paramFileCommon
      String modelId = basename(paramFileCommon, ".par")
      Array[File] paramFiles
      File recombFile
      Int nreps = 1
      Int maxAttempts = 10000000
      Int numRepsPerBlock = 1
      Int numCpusPerBlock = numRepsPerBlock
      Int repTimeoutSeconds = 600
      String       memoryPerBlock = "3 GB"
      String       cosi2_docker = "quay.io/ilya_broad/dockstore-tool-cosi2@sha256:11df3a646c563c39b6cbf71490ec5cd90c1025006102e301e62b9d0794061e6a"
      Int preemptible = 3
      File         taskScript
    }

    Array[Int] pops = get_pops_from_cosi2_param_file.popIds
    Array[String] repPopSuffixes = get_pops_from_cosi2_param_file.repPopSuffixes

    call get_pops_from_cosi2_param_file { 
      input: 
        paramFileCommon=paramFileCommon,
        numRepsPerBlock=numRepsPerBlock,
        cosi2_docker=cosi2_docker
    }

    Int numBlocks = nreps / numRepsPerBlock
    #Array[String] paramFileCommonLines = read_lines(paramFileCommonLines)

    scatter(paramFile_blockNum in cross(paramFiles, range(numBlocks))) {
      call cosi2_run_one_sim_block {
        input:
        paramFileCommon = paramFileCommon,
        paramFile = paramFile_blockNum.left,
	recombFile=recombFile,
	pops=pops,
	repPopSuffixes=repPopSuffixes,
        modelId=modelId,
	blockNum=paramFile_blockNum.right,
	simBlockId=modelId+"_"+paramFile_blockNum.right,
	numBlocks=numBlocks,
	maxAttempts=maxAttempts,
	repTimeoutSeconds=repTimeoutSeconds,
	numRepsPerBlock=numRepsPerBlock,
	numCpusPerBlock=numCpusPerBlock,
	memoryPerBlock=memoryPerBlock,
	cosi2_docker=cosi2_docker,
	preemptible=preemptible,
	taskScript=taskScript
      }
    }

    output {
      Array[Pair[ReplicaInfo,File]] replicaInfos = zip(flatten(cosi2_run_one_sim_block.replicaInfos), flatten(cosi2_run_one_sim_block.tpeds_tar_gz))
      Array[Array[File]] tpeds = cosi2_run_one_sim_block.tpeds
      Array[Int] popsUsed = pops
    }
}


