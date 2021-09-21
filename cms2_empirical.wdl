version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--a47fa8d8cd6d009de68da65f2879422e6c7ff813/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--a47fa8d8cd6d009de68da65f2879422e6c7ff813/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--a47fa8d8cd6d009de68da65f2879422e6c7ff813/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--a47fa8d8cd6d009de68da65f2879422e6c7ff813/compute_cms2_components.wdl"

workflow cms2_empirical {
  input {
    NeutralRegionExplorerParams nre_params
    EmpiricalHapsetsDef empirical_hapsets_def
  }

  call tasks.call_neutral_region_explorer {
    input:
    nre_params=nre_params
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


