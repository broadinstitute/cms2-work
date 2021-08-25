version 1.0

import "./structs.wdl"
import "./tasks.wdl"

workflow cms2_empirical {
  input {
    File empirical_neut_regions_bed
    File empirical_sel_regions_bed
  }
  call tasks.construct_pops_info_for_1KG {
    input:
    empirical_regions_bed=empirical_sel_regions_bed
  }
  PopsInfo pops_info_1KG = construct_pops_info_for_1KG.pops_info

  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_neut_regions {
    input:
    pops_info=pops_info_1KG,
    empirical_regions_bed=empirical_neut_regions_bed
  }

  scatter(sel_pop in pops_info.sel_pops) {
    call tasks.fetch_empirical_hapsets_from_1KG  as fetch_sel_regions {
      input:
      pops_info=pops_info_1KG,
      sel_pop=sel_pop,
      empirical_regions_bed=empirical_sel_regions_bed
    }
    Array[Array[File]+]+ sel_hapsets_tar_gzs = [fetch_sel_regions.empirical_hapsets_tar_gzs]
  }

  output {
    PopsInfo pops_info = pops_info_1KG
    Array[File]+ neut_region_haps_tar_gzs = fetch_neut_regions.empirical_hapsets_tar_gzs
    Array[Array[Array[File]+]+] empirical_sel_hapsets_tar_gzs = sel_hapsets_tar_gzs
  }
}


