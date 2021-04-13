version 1.0

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

struct NormalizeAndCollateInput {
    ReplicaInfo replica_info
    Array[Int] pop_ids
    Array[Pair[Int,Int]] pop_pairs
    String replica_id_str
    Int sel_pop
    File ihs_out
    File nsl_out
    File ihh12_out
    File delihh_out
    File derFreq_out
    Array[File] xpehh_out
    Array[File] fst_and_delDAF_out

    File norm_bins_ihs
    File norm_bins_nsl
    File norm_bins_ihh12
    File norm_bins_delihh
    Array[File] norm_bins_xpehh

    Int n_bins_ihs
    Int n_bins_nsl
    Int n_bins_ihh12
    Int n_bins_delihh
    Int n_bins_xpehh
}
