version 1.0

import "./structs.wdl"
import "./tasks.wdl"
import "./wdl_assert.wdl"
import "./fetch_g1k_vcfs.wdl"

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

  call tasks.fetch_file_from_url as fetch_pedigree_data {
    input: url="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/technical/working/20130606_sample_info/20130606_g1k.ped"
  }

  call tasks.fetch_file_from_url as fetch_related_individuals {
    input: url="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/20140625_related_individuals.txt"
  }

  call tasks.fetch_file_from_url as fetch_pops_data {
    input: url="https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/phase3/20131219.populations.tsv"
  }
  
  call tasks.merge_likely_neutral_regions {
    input:
    neutral_regions_bed=select_first([call_neutral_region_explorer.neutral_regions_bed,
                                      empirical_hapsets_def.empirical_neutral_regions_bed]),
    merge_margin_bp=select_first([empirical_hapsets_def.empirical_neutral_regions_merge_margin, 0])
  }

  call tasks.compute_intervals_stats as compute_neutral_intervals_merged_stats {
    input:
    intervals_files=[merge_likely_neutral_regions.neutral_regions_merged_bed]
  }

  Int slop_margin_bp = select_first([empirical_hapsets_def.empirical_neutral_regions_slop_margin, 0])

  call tasks.slop_likely_neutral_regions {
    input:
    neutral_regions_bed=merge_likely_neutral_regions.neutral_regions_merged_bed,
    chrom_sizes=fetch_chrom_sizes.file,
    slop_margin_bp=slop_margin_bp
  }

  call tasks.strip_chr_prefix as strip_chr_prefix_neutral {
    input:
    bed_file=slop_likely_neutral_regions.neutral_regions_slopped_bed
  }

  File neutral_bed_final = strip_chr_prefix_neutral.bed_file_nochr

  call tasks.compute_intervals_stats as compute_neutral_intervals_slopped_stats {
    input:
    intervals_files=[neutral_bed_final]
  }

  call fetch_g1k_vcfs.fetch_g1k_vcfs_wf as fetch_neutral_vcfs {
    input:
    intervals_files=[neutral_bed_final]
  }

  call tasks.fetch_empirical_hapsets_from_1KG  as fetch_neutral_regions {
    input:
    pops_info=pops_info_1KG,
    empirical_regions_bed=neutral_bed_final,
    genetic_maps_tar_gz=fetch_genetic_maps.file,
    pedigree_data_ped=fetch_pedigree_data.file,
    related_individuals_txt=fetch_related_individuals.file,
    pops_data_tsv=fetch_pops_data.file,
    out_fnames_prefix=empirical_hapsets_def.empirical_hapsets_bundle_id,
    chrom_vcfs=fetch_neutral_vcfs.chrom_vcfs
  }

  call tasks.strip_chr_prefix as strip_chr_prefix_selection {
    input:
    bed_file=empirical_hapsets_def.empirical_selection_regions_bed
  }

  File selection_bed_final = strip_chr_prefix_selection.bed_file_nochr


  call fetch_g1k_vcfs.fetch_g1k_vcfs_wf as fetch_selection_vcfs {
    input:
    intervals_files=[selection_bed_final]
  }

  scatter(sel_pop in pops_info_1KG.sel_pops) {
    call tasks.fetch_empirical_hapsets_from_1KG  as fetch_selection_regions {
      input:
      pops_info=pops_info_1KG,
      genetic_maps_tar_gz=fetch_genetic_maps.file,
      pedigree_data_ped=fetch_pedigree_data.file,
      related_individuals_txt=fetch_related_individuals.file,
      pops_data_tsv=fetch_pops_data.file,
      sel_pop_id=sel_pop.pop_id,
      empirical_regions_bed=selection_bed_final,
      out_fnames_prefix=empirical_hapsets_def.empirical_hapsets_bundle_id,
      chrom_vcfs=fetch_selection_vcfs.chrom_vcfs
    }
    Array[Array[File]+]+ selection_hapsets_for_sel_pop = [fetch_selection_regions.empirical_hapsets]
  }

  output {
    HapsetsBundle hapsets_bundle = object {
      hapsets_bundle_id: empirical_hapsets_def.empirical_hapsets_bundle_id,
      pops_info: pops_info_1KG,
      neutral_hapsets: fetch_neutral_regions.empirical_hapsets,
      neutral_hapsets_trim_margin_bp: slop_margin_bp,
      selection_hapsets: selection_hapsets_for_sel_pop
    }
    File neutral_regions_merged_stats_report_html = compute_neutral_intervals_merged_stats.intervals_report_html
    File neutral_regions_slopped_stats_report_html = compute_neutral_intervals_slopped_stats.intervals_report_html

    Array[Boolean]+ assert_results = [check_neutral_regions_spec.assert_result]
  }
}
