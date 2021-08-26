version 1.0

import "./structs.wdl"
import "./tasks.wdl"

workflow fetch_empirical_hapsets_wf {
  input {
    EmpiricalHapsetsDef empirical_hapsets_def
  }
  call tasks.construct_pops_info_for_1KG {
    input:
    empirical_regions_bed=empirical_hapsets_def.empirical_selection_regions_bed
  }
  PopsInfo pops_info_1KG = construct_pops_info_for_1KG.pops_info

  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_neutral_regions {
    input:
    pops_info=pops_info_1KG,
    empirical_regions_bed=empirical_hapsets_def.empirical_neutral_regions_bed,
    out_fnames_prefix=empirical_hapsets_def.empirical_hapsets_bundle_id
  }

  scatter(sel_pop in pops_info_1KG.sel_pops) {
    call tasks.fetch_empirical_hapsets_from_1KG  as fetch_selection_regions {
      input:
      pops_info=pops_info_1KG,
      sel_pop_id=sel_pop.pop_id,
      empirical_regions_bed=empirical_hapsets_def.empirical_selection_regions_bed,
      out_fnames_prefix=empirical_hapsets_def.empirical_hapsets_bundle_id
    }
    Array[Array[File]+]+ selection_hapsets_for_sel_pop = [fetch_selection_regions.empirical_hapsets]
  }

  output {
    HapsetsBundle hapsets_bundle = object {
      hapsets_bundle_id: empirical_hapsets_def.empirical_hapsets_bundle_id,
      pops_info: pops_info_1KG,
      neutral_hapsets: fetch_neutral_regions.empirical_hapsets,
      selection_hapsets: selection_hapsets_for_sel_pop
    }
  }
}
