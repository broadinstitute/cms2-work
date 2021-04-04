version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-add-missing-one-pop-stats--191f6b727ae9659a58afc6692222e454faf774ae/run_sims.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-add-missing-one-pop-stats--191f6b727ae9659a58afc6692222e454faf774ae/tasks.wdl"

workflow compute_normalization_stats_wf {
  input {
    String modelId
    PopsInfo pops_info
    Array[File] neut_sim_region_haps_tar_gzs

    File compute_components_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-add-missing-one-pop-stats/191f6b727ae9659a58afc6692222e454faf774ae/remodel_components.py"

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20

    Int n_bins_ihh12 = 1
    Int n_bins_xpehh = 1

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    File get_pops_info_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-add-missing-one-pop-stats/191f6b727ae9659a58afc6692222e454faf774ae/get_pops_info.py"
    #String docker = "quay.io/ilya_broad/cms@sha256:61329639d8a8479b059d430fcd816b51b825d4a22716660cc3d1688d97c99cc7"
    String docker = "quay.io/ilya_broad/cms@sha256:1834a9e5eb9db5253b4cf051c39d23e51ca6c3f812b6d17f5d2c87d9506f5e8a"  # selscan=1.3.0a06
    Int preemptible
  }  # end: input

  scatter(sel_pop in pops_info.pop_ids) {
    scatter(neut_sim_region_haps_tar_gz in neut_sim_region_haps_tar_gzs) {
      call tasks.compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_neutral {
	input:
	sel_pop=sel_pop,
	region_haps_tar_gz=neut_sim_region_haps_tar_gz,

	script=compute_components_script,
	threads=threads,
	mem_base_gb=mem_base_gb,
	mem_per_thread_gb=mem_per_thread_gb,
	local_disk_gb=local_disk_gb,
	docker=docker,
	preemptible=preemptible
      }
    }

# *** Compute normalization stats for one-pop components for neutral sims
    call tasks.compute_one_pop_bin_stats_for_normalization {
      input:
      out_fnames_base = modelId + "__selpop_" + sel_pop,
      sel_pop=sel_pop,

      ihs_out=compute_one_pop_cms2_components_for_neutral.ihs,
      nsl_out=compute_one_pop_cms2_components_for_neutral.nsl,
      ihh12_out=compute_one_pop_cms2_components_for_neutral.ihh12,

      n_bins_ihs=n_bins_ihs,
      n_bins_nsl=n_bins_nsl,
      n_bins_ihh12=n_bins_ihh12,

      threads=1,
      mem_base_gb=64,
      mem_per_thread_gb=0,
      local_disk_gb=local_disk_gb,
      docker=docker,
      preemptible=preemptible
    }  # end: call tasks.compute_one_pop_bin_stats_for_normalization
  }  # end: scatter(sel_pop in pops_info.pop_ids)

# *** Compute two-pop CMS2 components for neutral sims
   scatter(sel_pop_idx in range(length(pops_info.pop_ids))) {
     scatter(alt_pop_idx in range(length(pops_info.pop_ids))) {
       if (alt_pop_idx > sel_pop_idx) {
	 scatter(neut_sim_region_haps_tar_gz in neut_sim_region_haps_tar_gzs) {
	   call tasks.compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_neutral {
	     input:
	     sel_pop=pops_info.pop_ids[sel_pop_idx],
	     alt_pop=pops_info.pop_ids[alt_pop_idx],
	     region_haps_tar_gz=neut_sim_region_haps_tar_gz,
	     
	     script=compute_components_script,
	     threads=threads,
	     mem_base_gb=mem_base_gb,
	     mem_per_thread_gb=mem_per_thread_gb,
	     local_disk_gb=local_disk_gb,
	     docker=docker,
	     preemptible=preemptible
	   }
	 }

	 call tasks.compute_two_pop_bin_stats_for_normalization {
	   input:
	   out_fnames_base = modelId,
	   sel_pop=pops_info.pop_ids[sel_pop_idx],
	   alt_pop=pops_info.pop_ids[alt_pop_idx],

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

  scatter(sel_pop_idx in range(length(pops_info.pop_ids))) {
    scatter(alt_pop_idx in range(length(pops_info.pop_ids))) {
      if (alt_pop_idx != sel_pop_idx) {
  	File norm_bins_xpehh_maybe = 
        select_first([
        compute_two_pop_bin_stats_for_normalization.norm_bins_xpehh[sel_pop_idx][alt_pop_idx],
        compute_two_pop_bin_stats_for_normalization.norm_bins_flip_pops_xpehh[alt_pop_idx][sel_pop_idx]
        ])
      }
    }
    Array[File] norm_bins_xpehh_vals = select_all(norm_bins_xpehh_maybe)
  }  # end: scatter(sel_pop_idx in range(length(pops_info.pop_ids)))

  output {

    Array[File] norm_bins_ihs=compute_one_pop_bin_stats_for_normalization.norm_bins_ihs
    Array[File] norm_bins_nsl=compute_one_pop_bin_stats_for_normalization.norm_bins_nsl
    Array[File] norm_bins_ihh12=compute_one_pop_bin_stats_for_normalization.norm_bins_ihh12
    Array[Array[File]] norm_bins_xpehh = norm_bins_xpehh_vals
  }
}
