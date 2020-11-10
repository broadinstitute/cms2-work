version 1.0

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


task compute_cms2_components_for_one_replica {
  meta {
    description: "Compute CMS2 component scores"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
#    ReplicaInfo replicaInfo
    File replica_output
    File neutral_replica_output
    Int sel_pop
    File script

    Int threads
    Int mem_base_gb
    Int mem_per_thread_gb
    Int local_disk_gb
    String docker
  }
#  String modelId = replicaInfo.modelInfo.modelId
#  Int replicaNumGlobal = replicaInfo.replicaId.replicaNumGlobal
#  String replica_id_string = "model_" + modelId + "__rep_" + replicaNumGlobal + "__selpop_" + sel_pop
  String replica_id_string = basename(replica_output)
  String script_used_name = "script-used." + basename(script)
  String ihs_out_fname = replica_id_string + ".ihs.out"
  String ihs_normed_out_fname = replica_id_string + ".ihs.out.100bins.norm"

  command <<<
    tar xvfz ~{replica_output}
    mkdir -p $PWD/neut
    tar xvfz -C $PWD/neut/ ~{neutral_replica_output}
    cd neut
    
    python3 ~{script} --replica-info *.replicaInfo.json --replica-id-string neutrep --sel-pop ~{sel_pop} --threads ~{threads}
    norm --ihs --files neutrep.ihs.out --save-bins ihsbins.dat
    cd ..

    cp ~{script} ~{script_used_name}
    python3 ~{script} --replica-info *.replicaInfo.json --replica-id-string ~{replica_id_string} --sel-pop ~{sel_pop} --threads ~{threads}
    norm --ihs --files ~{ihs_out_fname} --load-bins neut/ihsbins.dat
  >>>

  output {
    Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
    File ihh12 = replica_id_string + ".ihh12.out"
    File ihs = ihs_out_fname
    File ihs_normed = ihs_normed_out_fname
    File nsl = replica_id_string + ".nsl.out"
    Array[File] xpehh = glob("*.xpehh.out")
    Int threads_used = threads
    File script_used = script_used_name
  }

  runtime {
    docker: docker
    memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
    cpu: threads
    disks: "local-disk " + local_disk_gb + " LOCAL"
  }
}


# task compute_score_normalization_stats {
#   meta {
#     description: "Compute stats needed for normalization of CMS component scores"
#     email: "ilya_shl@alum.mit.edu"
#   }
#   input {
# #    ReplicaInfo replicaInfo
#     File neutral_replica_output
#     File script

#     Int threads
#     Int mem_base_gb
#     Int mem_per_thread_gb
#     Int local_disk_gb
#     String docker
#   }
# #  String modelId = replicaInfo.modelInfo.modelId
# #  Int replicaNumGlobal = replicaInfo.replicaId.replicaNumGlobal
# #  String replica_id_string = "model_" + modelId + "__rep_" + replicaNumGlobal + "__selpop_" + sel_pop
#   String replica_id_string = basename(replica_output)
#   String script_used_name = "script-used." + basename(script)

#   command <<<
#     tar xvfz ~{replica_output}
#     cp ~{script} ~{script_used_name}
#     python3 ~{script} --replica-info *.replicaInfo.json --replica-id-string ~{replica_id_string} --sel-pop ~{sel_pop} --threads ~{threads}
#   >>>

#   output {
#     Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
#     File ihh12 = replica_id_string + ".ihh12.out"
#     File ihs = replica_id_string + ".ihs.out"
#     File nsl = replica_id_string + ".nsl.out"
#     Array[File] xpehh = glob("*.xpehh.out")
#     Int threads_used = threads
#     File script_used = script_used_name
#   }

#   runtime {
#     docker: docker
#     memory: (mem_base_gb  +  threads * mem_per_thread_gb) + " GB"
#     cpu: threads
#     disks: "local-disk " + local_disk_gb + " LOCAL"
#   }
# }

workflow compute_cms2_components {
  input {
#    Array[ReplicaInfo] replicaInfos
    Array[File] replica_outputs
    File neutral_replica_output
    Int sel_pop
    File script

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    String docker = "quay.io/broadinstitute/cms2@sha256:2ae725a834f62a40d83b1cb8f3102c1fa95c9c98f05853e5fa3bbf79fdf2e981"
  }
  scatter(replica_output in replica_outputs) {
    call compute_cms2_components_for_one_replica {
      input:
      replica_output=replica_output,
      neutral_replica_output=neutral_replica_output,
      sel_pop=sel_pop,
      script=script,
      threads=threads,
      mem_base_gb=mem_base_gb,
      mem_per_thread_gb=mem_per_thread_gb,
      local_disk_gb=local_disk_gb,
      docker=docker
    }
  }
  output {
    Array[Object] replicaInfo_out = compute_cms2_components_for_one_replica.replicaInfo
    Array[File] ihh12out = compute_cms2_components_for_one_replica.ihh12
    Array[File] ihsout = compute_cms2_components_for_one_replica.ihs
    Array[File] ihsnormedout = compute_cms2_components_for_one_replica.ihs_normed
    Array[File] nslout = compute_cms2_components_for_one_replica.nsl
    Array[Array[File]] xpehhout = compute_cms2_components_for_one_replica.xpehh
    Int threads_used=threads
    Array[File] script_used = compute_cms2_components_for_one_replica.script_used
  }
}
