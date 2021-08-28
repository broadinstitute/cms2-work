version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--ffb56361e4715b6cef386a58dfb9f715a5f75069/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--ffb56361e4715b6cef386a58dfb9f715a5f75069/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--ffb56361e4715b6cef386a58dfb9f715a5f75069/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210827-1136-finalize-empirical--ffb56361e4715b6cef386a58dfb9f715a5f75069/compute_cms2_components.wdl"

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


