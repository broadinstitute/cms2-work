version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--113fe0989423eb4b4f275cd72be046ab2b03999d/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210822-2127-add-pop-pair-match-checks--113fe0989423eb4b4f275cd72be046ab2b03999d/tasks.wdl"

workflow cms2_empirical {
  input {
    File empirical_sel_regions_bed
  }
  call tasks.construct_pops_info_for_1KG {
    input:
    empirical_regions_bed=empirical_sel_regions_bed
  }
  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_sel_regions {
    input:
    pops_info=construct_pops_info_for_1KG.pops_info,
    empirical_regions_bed=empirical_sel_regions_bed
  }
  output {
    Array[File] empirical_sel_hapsets_tar_gzs = fetch_sel_regions.empirical_hapsets_tar_gzs
  }
}

