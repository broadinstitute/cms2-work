version 1.0

import "./structs.wdl"
import "./tasks.wdl"

workflow construct_empirical_neutral_regions {
  input {
    EmpiricalNeutralRegionsParams empirical_neutral_regions_params = object {
      genes_pad_bp: 1000,
      telomeres_pad_bp: 1000000
    }
  }

  call tasks.fetch_file_from_url as fetch_chrom_sizes {
     input: url="https://hgdownload.soe.ucsc.edu/goldenPath/hg19/bigZips/hg19.chrom.sizes"
  }

  call tasks.fetch_file_from_url as fetch_gencode_annots { 
    input: url="https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_39/GRCh37_mapping/gencode.v39lift37.annotation.gff3.gz"
  }

  call tasks.fetch_file_from_url as fetch_ucsc_gap_track { 
    input: url="https://hgdownload.soe.ucsc.edu/goldenPath/hg19/database/gap.txt.gz"
  }

  call tasks.fetch_file_from_url as fetch_pophumanscan_coords { 
    input: url="https://pophumanscan.uab.cat/data/files/pophumanscanCoordinates.tab"
  }

  call tasks.construct_neutral_regions_list {
    input:
    empirical_neutral_regions_params=empirical_neutral_regions_params,
    genomic_features_for_finding_empirical_neutral_regions=object {  # struct GenomicFeaturesForFindingEmpiricalNeutralRegions
      chrom_sizes: fetch_chrom_sizes.file,
      gencode_annots: fetch_gencode_annots.file,
      ucsc_gap_track: fetch_ucsc_gap_track.file,
      pophumanscan_coords: fetch_pophumanscan_coords.file
    }
  }
  
  call tasks.compute_intervals_stats {
    input:
    intervals_files=flatten([[construct_neutral_regions_list.neutral_regions_bed], construct_neutral_regions_list.aux_beds])
  }

  output {
    File empirical_neutral_regions_bed=construct_neutral_regions_list.neutral_regions_bed
  }
}
