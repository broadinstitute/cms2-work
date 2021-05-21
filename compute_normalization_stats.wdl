version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210430-1631-pick-stats-to-compute--d0dcdf7f5eadec699473fd8e3c9550250692b11d/run_sims.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210430-1631-pick-stats-to-compute--d0dcdf7f5eadec699473fd8e3c9550250692b11d/tasks.wdl"

# * workflow compute_normalization_stats_wf
workflow compute_normalization_stats_wf {
  meta {
    description: "Computes stats (means and stds) needed to normalize each component score.  For each component score, for each selected pop or combo of (selected pop, alternate pop), computes that component score for SNPs in neutral hapsets, then computes the mean and std of that score for SNPs within each frequency bin."
  }

  parameter_meta {
# ** inputs
    modelId: "(String) Id of the demographic model; used in naming output files."
    
# ** outputs
    
  }


  input {
    String modelId
    PopsInfo pops_info
    Array[File] neut_sim_region_haps_tar_gzs

    File compute_components_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-210430-1631-pick-stats-to-compute/d0dcdf7f5eadec699473fd8e3c9550250692b11d/remodel_components.py"

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20
    Int n_bins_delihh = 20

    Int hapsetBlockSize = 2

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50

    String docker = "quay.io/ilya_broad/cms@1sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    Int preemptible

    ComputeResources compute_resources_for_compute_one_pop_cms2_components = object {
      mem_gb: 4,
      cpus: 1,
      local_storage_gb: 50
    }
    ComputeResources compute_resources_for_compute_two_pop_cms2_components = object {
      mem_gb: 4,
      cpus: 1,
      local_storage_gb: 50
    }
  }  # end: input

  Int n_bins_ihh12 = 1
  Int n_bins_xpehh = 1

  Array[Pop] pops = pops_info.pops
  Int n_pops = length(pops)

  scatter(sel_pop in pops) {
    scatter(neut_sim_region_haps_tar_gz in neut_sim_region_haps_tar_gzs) {
      call tasks.compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_neutral {
	input:
	sel_pop=sel_pop,
	region_haps_tar_gz=neut_sim_region_haps_tar_gz,

	script=compute_components_script,
	compute_resources=compute_resources_for_compute_one_pop_cms2_components,
	docker=docker,
	preemptible=preemptible
      }
    }

# **** Compute normalization stats for one-pop components for neutral sims
    call tasks.compute_one_pop_bin_stats_for_normalization {
      input:
      out_fnames_base = modelId + "__selpop_" + sel_pop.pop_id,
      sel_pop=sel_pop,

      ihs_out=compute_one_pop_cms2_components_for_neutral.ihs,
      nsl_out=compute_one_pop_cms2_components_for_neutral.nsl,
      ihh12_out=compute_one_pop_cms2_components_for_neutral.ihh12,
      delihh_out=compute_one_pop_cms2_components_for_neutral.delihh,

      n_bins_ihs=n_bins_ihs,
      n_bins_nsl=n_bins_nsl,
      n_bins_ihh12=n_bins_ihh12,
      n_bins_delihh=n_bins_delihh,

      threads=1,
      mem_base_gb=64,
      mem_per_thread_gb=0,
      local_disk_gb=local_disk_gb,
      docker=docker,
      preemptible=preemptible
    }  # end: call tasks.compute_one_pop_bin_stats_for_normalization
  }  # end: scatter(sel_pop in pops)

# **** Compute two-pop CMS2 components for neutral sims
   scatter(sel_pop_idx in range(n_pops)) {
     scatter(alt_pop_idx in range(n_pops)) {
       if (alt_pop_idx > sel_pop_idx) {
	 scatter(neut_sim_region_haps_tar_gz in neut_sim_region_haps_tar_gzs) {
	   call tasks.compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_neutral {
	     input:
	     sel_pop=pops[sel_pop_idx],
	     alt_pop=pops[alt_pop_idx],
	     region_haps_tar_gz=neut_sim_region_haps_tar_gz,
	     
	     script=compute_components_script,
	     compute_resources=compute_resources_for_compute_two_pop_cms2_components,
	     docker=docker,
	     preemptible=preemptible
	   }
	 }

	 call tasks.compute_two_pop_bin_stats_for_normalization {
	   input:
	   out_fnames_base = modelId,
	   sel_pop=pops[sel_pop_idx],
	   alt_pop=pops[alt_pop_idx],

	   xpehh_out=compute_two_pop_cms2_components_for_neutral.xpehh,

	   n_bins_xpehh=n_bins_xpehh,

	   threads=1,
	   mem_base_gb=64,
	   mem_per_thread_gb=0,
	   local_disk_gb=local_disk_gb,
	   docker=docker,
	   preemptible=preemptible
	 }
       }
     }
  }

  scatter(sel_pop_idx in range(n_pops)) {
    scatter(alt_pop_idx in range(n_pops)) {
      if (alt_pop_idx != sel_pop_idx) {
  	File norm_bins_xpehh_maybe = 
        select_first([
        compute_two_pop_bin_stats_for_normalization.norm_bins_xpehh[sel_pop_idx][alt_pop_idx],
        compute_two_pop_bin_stats_for_normalization.norm_bins_flip_pops_xpehh[alt_pop_idx][sel_pop_idx]
        ])
      }
    }
    Array[File] norm_bins_xpehh_vals = select_all(norm_bins_xpehh_maybe)
  }  # end: scatter(sel_pop_idx in range(length(pops)))

  output {

    Array[File] norm_bins_ihs=compute_one_pop_bin_stats_for_normalization.norm_bins_ihs
    Array[File] norm_bins_nsl=compute_one_pop_bin_stats_for_normalization.norm_bins_nsl
    Array[File] norm_bins_ihh12=compute_one_pop_bin_stats_for_normalization.norm_bins_ihh12
    Array[File] norm_bins_delihh=compute_one_pop_bin_stats_for_normalization.norm_bins_delihh
    Array[Array[File]] norm_bins_xpehh = norm_bins_xpehh_vals
  }
}
