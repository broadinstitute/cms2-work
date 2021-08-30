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
    Array[String]+ pop_ids  # population IDs, used throughout to identify populations
    Array[String]+ pop_names
    Array[Pop]+ pops
    Map[String,String] pop_id_to_idx  # map from pop id to its index in pop_ids
    Map[String,Array[String]+] pop_alts  # map from pop id to list of all other pop ids
    Array[Array[Boolean]+]+ pop_alts_used # pop_alts_used[pop1][pop2] is true iff pop2 in pop_alts[pop1]
    #                                     pop_alts_used indicates which pop pair comparisons we need to do
    #Array[Pair[String,String]] pop_pairs # all two-pop sets, for cross-pop comparisons

    Array[Pop] sel_pops  # for each sweep definition in paramFiles_selection input to
    # workflow run_sims_and_compute_cms2_components, the pop id of the pop in which selection is defined.
  }

struct SimulatedHapsetsDef {
    File paramFile_demographic_model
    File paramFile_neutral
    Array[File] paramFiles_selection
    File recombFile
    String experimentId #= "default"
    String experiment_description #= "an experiment"
    String modelId # = "model_"+basename(paramFile_demographic_model, ".par")

    Int nreps_neutral
    Int nreps

    Int maxAttempts #= 10000000
    Int numRepsPerBlock #= 1
    Int numCpusPerBlock #= numRepsPerBlock

    String       memoryPerBlock #= "3 GB"
}

struct EmpiricalHapsetsDef {
  String empirical_hapsets_bundle_id
  File empirical_neutral_regions_bed
  File empirical_selection_regions_bed
}

struct HapsetsBundle {
    String hapsets_bundle_id
    PopsInfo pops_info
    Array[File]+ neutral_hapsets
    Array[Array[Array[File]+]+] selection_hapsets
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
  Int n_attempts
}

struct NormalizeAndCollateBlockInput {
    Array[File]+ replica_info
    Pop sel_pop
    Array[File]+ ihs_out
    Array[File]+ nsl_out
    Array[File]+ ihh12_out
    Array[File]+ delihh_out
    Array[File]+ derFreq_out
    Array[Array[File]]+ xpehh_out
    Array[Array[File]]+ fst_and_delDAF_out

    File norm_bins_ihs
    File norm_bins_nsl
    File norm_bins_ihh12
    File norm_bins_delihh
    Array[File]+ norm_bins_xpehh

    Int n_bins_ihs
    Int n_bins_nsl
    Int n_bins_ihh12
    Int n_bins_delihh
    Int n_bins_xpehh

    Pop one_pop_components_sel_pop_used
    Array[Pop]+ two_pop_components_sel_pop_used
    Array[Pop]+ two_pop_components_alt_pop_used

    Pop norm_one_pop_components_sel_pop_used
    Array[Pop]+ norm_two_pop_components_sel_pop_used
    Array[Pop]+ norm_two_pop_components_alt_pop_used
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
