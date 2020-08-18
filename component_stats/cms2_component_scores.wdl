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


task compute_ihh12_for_one_replica {
  meta {
    description: "Compute iHH12 scores"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    ReplicaInfo replicaInfo
    Int sel_pop
    File script
  }
  String replicaIdString = "model_" + replicaInfo.modelInfo.modelId + "__rep_" + replicaInfo.replicaId.replicaNumGlobal
  command <<<
    tar xvfz ~{replicaInfo.tpeds_tar_gz}
    python3 ~{script} --replica-info *.replicaInfo.json --replica-id-string ~{replicaIdString}
  >>>
  output {
    File ihh12 = replicaIdString + ".ihh12.out"
  }
  runtime {
    docker: "quay.io/broadinstitute/cms2@sha256:aa2311202d138770abaf15cfa50e26cef29e95dcf8fbc81b75bfc751f9d8b74d"
  }
}

workflow compute_ihh12 {
  input {
    Array[ReplicaInfo] replicaInfos
    Int sel_pop
    File script
  }
  scatter(replica_info in replicaInfos) {
    call compute_ihh12_for_one_replica {
      input:
      replicaInfo=replica_info,
      sel_pop=sel_pop,
      script=script
    }
  }
  output {
    Array[File] ihh12out = compute_ihh12_for_one_replica.ihh12
  }
}


