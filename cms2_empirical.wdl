version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--6ca1f2fdf185a4cf3010575b1a6a8f1987f395c0/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--6ca1f2fdf185a4cf3010575b1a6a8f1987f395c0/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--6ca1f2fdf185a4cf3010575b1a6a8f1987f395c0/fetch_empirical_hapsets.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--6ca1f2fdf185a4cf3010575b1a6a8f1987f395c0/compute_cms2_components.wdl"

workflow cms2_empirical {
  input {
    NeutralRegionExplorerParams? nre_params
    EmpiricalHapsetsDef empirical_hapsets_def
  }

  if (defined(nre_params)) {
    call tasks.call_neutral_region_explorer {
      input:
      nre_params=select_first([nre_params])
      }
  }

  call fetch_empirical_hapsets.fetch_empirical_hapsets_wf {
    input:
    empirical_hapsets_def = object {  # struct EmpiricalHapsetsDef
      empirical_hapsets_bundle_id: empirical_hapsets_def.empirical_hapsets_bundle_id,
      empirical_selection_regions_bed: empirical_hapsets_def.empirical_selection_regions_bed,
      empirical_neutral_regions_bed:
         select_first([call_neutral_region_explorer.neutral_regions_bed, empirical_hapsets_def.empirical_neutral_regions_bed])
    }
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


