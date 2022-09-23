version 1.0

import "./structs.wdl"
import "./tasks.wdl"
import "./wdl_assert.wdl"

workflow fetch_g1k_vcfs_wf {
  meta {
    description: "Fetch vcfs and vcf indexes for a set of chromosomes(currently 1000 Genomes)"
    email: "ilya_shl@alum.mit.edu"
  }
  input {
    Array[File] intervals_files
  }
  call tasks.get_intervals_chroms as get_intervals_chroms {
    input:
    intervals_files=intervals_files
  }

  Array[String] chroms = get_intervals_chroms.intervals_chroms
  scatter(chrom in chroms) {
    String chrom_url = "https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/" +
      "ALL.chr${chrom}.phase3_shapeit2_mvncall_integrated_v5b.20130502.genotypes.vcf.gz"
    call tasks.fetch_file_from_url as fetch_chrom_vcf {
      input:
      file_metadata={"description": "1KG chr${chrom} vcf" },
      url=chrom_url
    }
    call tasks.fetch_file_from_url as fetch_chrom_vcf_tbi {
      input:
      file_metadata={"description": "1KG chr${chrom} vcf index" },
      url=chrom_url+".tbi"
    }
  }
  output {
    ChromVcfs chrom_vcfs = object {
      chrom_ids: chroms,
      chrom_vcfs: fetch_chrom_vcf.file,
      chrom_vcf_tbis: fetch_chrom_vcf_tbi.file
    }
    Array[File] intervals_files_used = intervals_files
  }
}


