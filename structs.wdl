version 1.0

# * struct Pop
#
# Identifier for one population.
struct Pop {
  String pop_id
}

struct PopPair {
  Pop sel_pop
  Pop alt_pop
}

#
# ** struct PopsInfo
#
# Information about population ids and names.
#
# Each pop has: a pop id (a small integer); a pop name (a string); a pop index
# (0-based index of the pop in the list of pop ids).
#
struct PopsInfo {
    Array[String] pop_ids  # population IDs, used throughout to identify populations
    Array[String] pop_names
    Array[Pop] pops
    Map[String,String] pop_id_to_idx  # map from pop id to its index in pop_ids
    Map[String,Array[String]] pop_alts  # map from pop id to list of all other pop ids
    Array[Pair[String,String]] pop_pairs # all two-pop sets, for cross-pop comparisons

    Array[String] sel_pop_ids  # for each sweep definition in paramFiles_selection input to
    # workflow run_sims_and_compute_cms2_components, the pop id of the pop in which selection is defined.
}

struct SweepInfo {
  String  selPop
  Float selGen
  String selBegPop
  Float selBegGen
  Float selCoeff
  Float selFreq
}

struct ModelInfo {
  String modelId
  Array[String] modelIdParts
  Array[String] popIds
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
    #ReplicaInfo replica_info
    File replica_info_file
    Array[String] pop_ids
    Array[Pair[String,String]] pop_pairs
    #String replica_id_str
    Pop sel_pop
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

struct NormalizeAndCollateBlockInput {
    #ReplicaInfo replica_info
    Array[File] replica_info
    #Array[Int] pop_ids
    #Array[Pair[Int,Int]] pop_pairs
    #String replica_id_str
    Pop sel_pop
    Array[File] ihs_out
    Array[File] nsl_out
    Array[File] ihh12_out
    Array[File] delihh_out
    Array[File] derFreq_out
    Array[Array[File]] xpehh_out
    Array[Array[File]] fst_and_delDAF_out

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



struct ComputeResources {
   Int? mem_gb
   Int? cpus
   Int? local_storage_gb
}

# struct AllComputeResources {
#   ComputeResources compute_one_pop_cms2_components
#   ComputeResources compute_two_pop_cms2_components
# }
