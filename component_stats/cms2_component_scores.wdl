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
  }
  String replicaIdString = "model_" + replicaInfo.modelInfo.modelId + "__rep_" + replicaInfo.replicaId.replicaNumGlobal
  command <<<
    python3 remodel_components.py --tpeds-tar-gz ~{tpeds_tar_gz} --sel-pop ~{sel_pop} --ihh12-unnormedfileprefix 
  >>>
  output {
    File ihh12
  }
}

