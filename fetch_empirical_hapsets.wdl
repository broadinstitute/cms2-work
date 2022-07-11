version 1.0

import "./structs.wdl"
import "./tasks.wdl"
import "./wdl_assert.wdl"

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

  call tasks.fetch_file_from_google_drive as fetch_genetic_maps {
    input:
    file_metadata={"description": "hg19 genetic maps from https://www.science.org/doi/10.1126/sciadv.aaw9206" },
    out_fname="hg19_maps.tar.gz",
    gdrive_file_id="17KWNaJQJuldfbL9zljFpqj5oPfUiJ0Nv",
    sha256="6e161fac40f0689ea33fe9c6afb1fccda670bfba29cc7be0df0348c232da2b3f"
  }

  call tasks.fetch_file_from_url as fetch_chrom_sizes {
     input: url="https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.chrom.sizes"
  }
  
  call tasks.merge_likely_neutral_regions {
    input:
    neutral_regions_bed=select_first([call_neutral_region_explorer.neutral_regions_bed,
                                      empirical_hapsets_def.empirical_neutral_regions_bed]),
    chrom_sizes=fetch_chrom_sizes.file
  }

  call tasks.compute_intervals_stats as compute_neutral_intervals_merged_stats {
    input:
    intervals_files=[merge_likely_neutral_regions.neutral_regions_merged_bed]
  }

  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_neutral_regions {
    input:
    pops_info=pops_info_1KG,
    empirical_regions_bed=merge_likely_neutral_regions.neutral_regions_merged_bed,
    genetic_maps_tar_gz=fetch_genetic_maps.file,
    out_fnames_prefix=empirical_hapsets_def.empirical_hapsets_bundle_id
  }

  scatter(sel_pop in pops_info_1KG.sel_pops) {
    call tasks.fetch_empirical_hapsets_from_1KG  as fetch_selection_regions {
      input:
      pops_info=pops_info_1KG,
      genetic_maps_tar_gz=fetch_genetic_maps.file,
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
    File neutral_regions_merged_stats_report_html = compute_neutral_intervals_merged_stats.intervals_report_html
    Array[Boolean]+ assert_results = [check_neutral_regions_spec.assert_result]
  }
}
