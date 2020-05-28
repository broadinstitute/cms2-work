version 1.0

#
# WDL workflows for running population genetics simulations using cosi2
#

#import "tasks_simulation.wdl" as sims

#
# TODO:
#   include metadata including selection start/stop/pop in workflow output as table
#   and muation age
#

task cosi2_run_one_sim {
  input {
    File         paramFile
    File         recombFile
    String       simId
    Int          maxAttempts = 10000000
    String       cosi2_docker = "quay.io/ilya_broad/docker-tool-cosi2:latest"
  }

  command {
    grep -v "recomb_file" "${paramFile}" > ${simId}.fixed.par
    echo "recomb_file ${recombFile}" >> ${simId}.fixed.par
    env COSI_NEWSIM=1 COSI_MAXATTEMPTS=${maxAttempts} coalescent -p ${simId}.fixed.par --genmapRandomRegions --drop-singletons .25 --output-gen-map --tped "${simId}"
    tar cvfz "${simId}.tpeds.tar.gz" *.tped
  }

  output {
    File        tpeds = "${simId}.tpeds.tar.gz"
#    String      cosi2_docker_used = ""
  }
  runtime {
#    docker: "quay.io/ilya_broad/cms-dev:2.0.1-15-gd48e1db-is-cms2-new"
    docker: cosi2_docker
    memory: "3 GB"
    cpu: 2
    dx_instance_type: "mem1_ssd1_v2_x4"
  }

  meta {
    email: "ilya_shl@alum.mit.edu"
    description: "Run one cosi2 simulation for one demographic model."
  }
}

task cosi2_run_one_sim_block {
  meta {
    description: "Run one block of cosi2 simulations for one demographic model."
    email: "ilya_shl@alum.mit.edu"
  }

  parameter_meta {
    # Inputs
    paramFile: "cosi2 parameter file"
    recombFile: "recombination map"
    simBlockId: "an ID of this simulation block (e.g. block number in a list of blocks); used to name output files"
    nSimsInBlock: "number of simulations in this block"

    # Outputs
    tpeds: ".tar.gz file containing simulated samples for each population"
  }

  input {
    File         paramFile
    File         recombFile
    String       simBlockId
    Int          nSimsInBlock = 1
    Int          maxAttempts = 10000000
    Int          randomSeed = 0
    String       cosi2_docker = "quay.io/ilya_broad/docker-tool-cosi2:latest"
  }

  command {
    grep -v "recomb_file" "${paramFile}" > ${simBlockId}.fixed.par
    echo "recomb_file ${recombFile}" >> ${simBlockId}.fixed.par
    env COSI_NEWSIM=1 COSI_MAXATTEMPTS=${maxAttempts} coalescent -p ${simBlockId}.fixed.par -r ${randomSeed} -n ${nSimsInBlock} --genmapRandomRegions --drop-singletons .25 --output-gen-map --tped "${simBlockId}"
    tar cvfz "${simBlockId}.tpeds.tar.gz" *.tped
  }

  output {
    File        tpeds = "${simBlockId}.tpeds.tar.gz"
#    String      cosi2_docker_used = ""
  }
  runtime {
#    docker: "quay.io/ilya_broad/cms-dev:2.0.1-15-gd48e1db-is-cms2-new"
    docker: cosi2_docker
    memory: "3 GB"
    cpu: 2
    dx_instance_type: "mem1_ssd1_v2_x4"
    volatile: randomSeed == 0
  }

}


workflow run_sims_cosi2 {
    meta {
      description: "Run a set of cosi2 simulations for one or more demographic models."
      author: "Ilya Shlyakhter"
      email: "ilya_shl@alum.mit.edu"
    }

    parameter_meta {
      paramFiles: "cosi2 parameter files specifying the demographic model"
      recombFile: "Recombination map from which map of each simulated region is sampled"
      nreps: "Number of replicates for _each_ demographic model."
    }

    input {
      Array[File]+ paramFiles
      File recombFile
      Int nreps = 1
      Int nSimsPerBlock = 1
      String       cosi2_docker = "quay.io/ilya_broad/docker-tool-cosi2:latest"
    }
    Int nBlocks = nreps / nSimsPerBlock

    scatter(paramFile in paramFiles) {
        scatter(blockNum in range(nBlocks)) {
            call cosi2_run_one_sim_block {
                input:
                   paramFile = paramFile, recombFile=recombFile,
	           simBlockId=basename(paramFile, ".par")+"_"+blockNum,
	           nSimsInBlock=nSimsPerBlock,
	           cosi2_docker=cosi2_docker
            }
        }
    }

    output {
      Array[File] tpeds = flatten(cosi2_run_one_sim_block.tpeds)
    }

}
