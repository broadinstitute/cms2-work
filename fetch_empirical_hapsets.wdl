version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--f1ea0298b2c065749b15dbbe465abddcdeec0330/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--f1ea0298b2c065749b15dbbe465abddcdeec0330/tasks.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--f1ea0298b2c065749b15dbbe465abddcdeec0330/wdl_assert.wdl"

workflow fetch_empirical_hapsets_wf {
  meta {
    description: "Constructs hapsets from specified regions of empirical data (currently 1000 Genomes)"
    email: "ilya_shl@alum.mit.edu"
  }

  input {
    EmpiricalHapsetsDef empirical_hapsets_def
  }

  if (defined(empirical_hapsets_def.nre_params)) {
    call tasks.call_neutral_region_explorer {
      input:
      nre_params=select_first([empirical_hapsets_def.nre_params])
    }
  }
  call wdl_assert.wdl_assert_wf as check_neutral_regions_spec {
    input:
    cond=(defined(empirical_hapsets_def.nre_params) != defined(empirical_hapsets_def.empirical_neutral_regions_bed)),
    msg="exactly one of nre_params or empirical_neutral_regions_bed must be specified"
  }
  call tasks.construct_pops_info_for_1KG {
    input:
    empirical_regions_bed=empirical_hapsets_def.empirical_selection_regions_bed
  }
  PopsInfo pops_info_1KG = construct_pops_info_for_1KG.pops_info

  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_neutral_regions {
    input:
    pops_info=pops_info_1KG,
    empirical_regions_bed=select_first([call_neutral_region_explorer.neutral_regions_bed,
                                         empirical_hapsets_def.empirical_neutral_regions_bed]),
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
    Array[Boolean]+ assert_results = [check_neutral_regions_spec.assert_result]
  }
}
