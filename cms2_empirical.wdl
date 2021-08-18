version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210816-1725-refactor-terra--ee04f0c5b73ca03694d4652373e298d9b8d4d638/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210816-1725-refactor-terra--ee04f0c5b73ca03694d4652373e298d9b8d4d638/tasks.wdl"

workflow cms2_empirical {
  input {
    File empirical_sel_regions_bed
  }
  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_sel_regions {
    input:
    empirical_regions_bed=empirical_sel_regions_bed
  }
  output {
    Array[File] empirical_sel_hapsets_tar_gzs = fetch_sel_regions.empirical_hapsets_tar_gzs
  }
}

