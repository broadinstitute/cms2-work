version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--19145c915c5e982817467c3a47e0c59734916a4a/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--19145c915c5e982817467c3a47e0c59734916a4a/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--19145c915c5e982817467c3a47e0c59734916a4a/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--19145c915c5e982817467c3a47e0c59734916a4a/compute_cms2_components.wdl"

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


