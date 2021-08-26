version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--19549bbc002f42d46edeabf42a334e76148126b4/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--19549bbc002f42d46edeabf42a334e76148126b4/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--19549bbc002f42d46edeabf42a334e76148126b4/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--19549bbc002f42d46edeabf42a334e76148126b4/compute_cms2_components.wdl"

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


