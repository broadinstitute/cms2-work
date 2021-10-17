version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--86cb84a5a9605e47478b7c032c15d9b9f1c11bff/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--86cb84a5a9605e47478b7c032c15d9b9f1c11bff/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--86cb84a5a9605e47478b7c032c15d9b9f1c11bff/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--86cb84a5a9605e47478b7c032c15d9b9f1c11bff/compute_cms2_components.wdl"

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


