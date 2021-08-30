version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210830-1022-validate-empirical-and-clean-up--682ce25e8c3c529d36a34ec508bdeebff7209134/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210830-1022-validate-empirical-and-clean-up--682ce25e8c3c529d36a34ec508bdeebff7209134/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210830-1022-validate-empirical-and-clean-up--682ce25e8c3c529d36a34ec508bdeebff7209134/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210830-1022-validate-empirical-and-clean-up--682ce25e8c3c529d36a34ec508bdeebff7209134/compute_cms2_components.wdl"

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


