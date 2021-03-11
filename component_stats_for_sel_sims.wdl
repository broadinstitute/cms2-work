version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-add-missing-one-pop-stats--de5581c4a7b52e038d23062519a1df4d661fabc4/run_sims.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-add-missing-one-pop-stats--de5581c4a7b52e038d23062519a1df4d661fabc4/tasks.wdl"

workflow component_stats_for_sel_sims_wf {
  input {
    String modelId
    Array[Pair[ReplicaInfo, File]] selection_sims
    File compute_components_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-add-missing-one-pop-stats/de5581c4a7b52e038d23062519a1df4d661fabc4/remodel_components.py"
    File normalize_and_collate_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tool-cms2/is-add-missing-one-pop-stats/de5581c4a7b52e038d23062519a1df4d661fabc4/norm_and_collate.py"
    PopsInfo pops_info

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20

    Int n_bins_ihh12 = 1
    Int n_bins_xpehh = 1

    Array[File] norm_bins_ihs
    Array[File] norm_bins_nsl
    Array[File] norm_bins_ihh12
    Array[Array[File]] norm_bins_xpehh

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    String docker = "quay.io/ilya_broad/cms@sha256:1834a9e5eb9db5253b4cf051c39d23e51ca6c3f812b6d17f5d2c87d9506f5e8a"  # selscan=1.3.0a06
    Int preemptible
  }

  scatter(sel_sim in selection_sims) {
    ReplicaInfo sel_sim_replicaInfo = sel_sim.left
    if (sel_sim_replicaInfo.succeeded) {
      Int sel_pop = sel_sim_replicaInfo.modelInfo.sweepInfo.selPop
      Int sel_pop_idx = pops_info.pop_id_to_idx[sel_pop]
      File sel_sim_region_haps_tar_gz = sel_sim.right
      String sel_sim_replica_id_str = modelId + "__selpop_" + sel_pop + "__rep_" + sel_sim_replicaInfo.replicaId.replicaNumGlobal
      call tasks.compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_selection {
	input:
	sel_pop=sel_pop,
	region_haps_tar_gz=sel_sim_region_haps_tar_gz,

	script=compute_components_script,
	threads=threads,
	mem_base_gb=mem_base_gb,
	mem_per_thread_gb=mem_per_thread_gb,
	local_disk_gb=local_disk_gb,
	docker=docker,
	preemptible=preemptible
      }
      scatter(alt_pop_idx in range(length(pops_info.pop_ids))) {
	if (alt_pop_idx != sel_pop_idx) {
	  call tasks.compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_selection {
	    input:
	    sel_pop=sel_pop,
	    alt_pop=pops_info.pop_ids[alt_pop_idx],
	    region_haps_tar_gz=sel_sim.right,

	    script=compute_components_script,
	    threads=threads,
	    mem_base_gb=mem_base_gb,
	    mem_per_thread_gb=mem_per_thread_gb,
	    local_disk_gb=local_disk_gb,
	    docker=docker,
	    preemptible=preemptible
	  }
	}
      }

      # should normalize_and_collate be done by blocks?
      call tasks.normalize_and_collate {
	input:
	  inp = object {
	    replica_id_str: sel_sim_replica_id_str,
	    pop_ids: pops_info.pop_ids,
	    pop_pairs: pops_info.pop_pairs,
	    sel_pop: sel_pop,

	    ihs_out: compute_one_pop_cms2_components_for_selection.ihs,
	    nsl_out: compute_one_pop_cms2_components_for_selection.nsl,
	    ihh12_out: compute_one_pop_cms2_components_for_selection.ihh12,

	    xpehh_out: select_all(compute_two_pop_cms2_components_for_selection.xpehh),

	    n_bins_ihs: n_bins_ihs,
	    n_bins_nsl: n_bins_nsl,
	    n_bins_ihh12: n_bins_ihh12,
	    n_bins_xpehh: n_bins_xpehh,

	    norm_bins_ihs: norm_bins_ihs[sel_pop_idx],
	    norm_bins_nsl: norm_bins_nsl[sel_pop_idx],
	    norm_bins_ihh12: norm_bins_ihh12[sel_pop_idx],

	    norm_bins_xpehh: norm_bins_xpehh[sel_pop_idx]
	  },
	  normalize_and_collate_script=normalize_and_collate_script
      } # call tasks.normalize_and_collate
    }  # if (sel_sim_replicaInfo.succeeded) 
  }  # end: scatter(sel_sim in selection_sims)

  output {	     
     Array[File?] sel_normed_and_collated = normalize_and_collate.normed_collated_stats
     Array[File?] sel_sim_region_haps_tar_gzs = sel_sim_region_haps_tar_gz
  }
}

