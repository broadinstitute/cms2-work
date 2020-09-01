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
    Int sel_pop
    File script
  }
#  String modelId = replicaInfo.modelInfo.modelId
#  Int replicaNumGlobal = replicaInfo.replicaId.replicaNumGlobal
#  String replica_id_string = "model_" + modelId + "__rep_" + replicaNumGlobal + "__selpop_" + sel_pop
  String replica_id_string = basename(replica_output)
  command <<<
    tar xvfz ~{replica_output}
    python3 ~{script} --replica-info *.replicaInfo.json --replica-id-string ~{replica_id_string} --sel-pop ~{sel_pop}
  >>>
  output {
    Object replicaInfo = read_json(replica_id_string + ".replica_info.json")
    File ihh12 = replica_id_string + ".ihh12.out"
    File ihs = replica_id_string + ".ihs.out"
    File nsl = replica_id_string + ".nsl.out"
    Array[File] xpehh = glob("*.xpehh.out")
  }
  runtime {
    docker: "quay.io/broadinstitute/cms2@sha256:aa2311202d138770abaf15cfa50e26cef29e95dcf8fbc81b75bfc751f9d8b74d"
  }
}

workflow compute_cms2_components {
  input {
#    Array[ReplicaInfo] replicaInfos
    Array[File] replica_outputs
    Int sel_pop
    File script
  }
  scatter(replica_output in replica_outputs) {
    call compute_cms2_components_for_one_replica {
      input:
      replica_output=replica_output,
      sel_pop=sel_pop,
      script=script
    }
  }
  output {
    Array[Object] replicaInfo_out = compute_cms2_components_for_one_replica.replicaInfo
    Array[File] ihh12out = compute_cms2_components_for_one_replica.ihh12
    Array[File] ihsout = compute_cms2_components_for_one_replica.ihs
    Array[File] nslout = compute_cms2_components_for_one_replica.nsl
    Array[Array[File]] xpehhout = compute_cms2_components_for_one_replica.xpehh
  }
}
