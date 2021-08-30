version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--dc800c8682e447ef076a0076b2f7fe17f578194c/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--dc800c8682e447ef076a0076b2f7fe17f578194c/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--dc800c8682e447ef076a0076b2f7fe17f578194c/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--dc800c8682e447ef076a0076b2f7fe17f578194c/compute_cms2_components.wdl"

workflow cms2_empirical {
  input {
    EmpiricalHapsetsDef empirical_hapsets_def
  }

  call fetch_empirical_hapsets.fetch_empirical_hapsets_wf {
    input:
    empirical_hapsets_def=empirical_hapsets_def
  }
  call compute_cms2_components.compute_cms2_components_wf {
    input:
    hapsets_bundle=fetch_empirical_hapsets_wf.hapsets_bundle
  }
  output {
    PopsInfo pops_info = fetch_empirical_hapsets_wf.hapsets_bundle.pops_info
    Array[File] all_hapsets_component_stats_h5_blocks = compute_cms2_components_wf.all_hapsets_component_stats_h5_blocks
  }
}


