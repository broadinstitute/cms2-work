version 1.0

#import "tasks_simulation.wdl" as sims

task cosi2_run_one_sim {
  input {
    File         paramFile
    File         recombFile
    String       simId
    Int          maxAttempts = 10000000
    String       cosi2_docker = "quay.io/ilya_broad/docker-tool-cosi2:latest"
  }

  command {
    
    grep -v "recomb_file" "${paramFile}" > ${paramFile}.fixed.par
    echo "recomb_file ${recombFile}" >> ${paramFile}.fixed.par
    env COSI_NEWSIM=1 COSI_MAXATTEMPTS=${maxAttempts} coalescent -p ${paramFile}.fixed.par --genmapRandomRegions --drop-singletons .25 --output-gen-map --tped "${simId}"
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

workflow run_sims_cosi2 {
    input {
      Array[File] paramFiles
      File recombFile
      Int nreps
      String       cosi2_docker = "quay.io/ilya_broad/docker-tool-cosi2:latest"
    }

    scatter(paramFile in paramFiles) {
        scatter(rep in range(nreps)) {
            call cosi2_run_one_sim {
                input:
                   paramFile = paramFile, recombFile=recombFile, simId=basename(paramFile, ".par")+"_"+rep, cosi2_docker=cosi2_docker
            }
        }
    }

    output {
      Array[File] tpeds = flatten(cosi2_run_one_sim.tpeds)
    }

    parameter_meta {
      paramFiles: "cosi2 parameter files specifying the demographic model"
      recombFile: "Recombination map from which map of each simulated region is sampled"
      nreps: "Number of replicates for _each_ demographic model."
    }

    meta {
      email: "ilya_shl@alum.mit.edu"
      description: "Run a set of cosi2 simulations for one or more demographic models."
    }
}
