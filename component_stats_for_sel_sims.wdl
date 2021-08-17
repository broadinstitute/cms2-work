version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210816-1725-refactor-terra--9b80d925b3804fe1b8cef8a564338385295cd4c0/tasks.wdl"

workflow component_stats_for_sel_sims_wf {
  input {
    String modelId
    String experimentId = "default"
    Array[Array[Array[File]]] selection_sims
    File compute_components_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210816-1725-refactor-terra/9b80d925b3804fe1b8cef8a564338385295cd4c0/remodel_components.py"
    File normalize_and_collate_script = "gs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/cms2-work/is-210816-1725-refactor-terra/9b80d925b3804fe1b8cef8a564338385295cd4c0/norm_and_collate.py"
    PopsInfo pops_info

    Int n_bins_ihs = 20
    Int n_bins_nsl = 20
    Int n_bins_delihh = 20

    Array[File] norm_bins_ihs
    Array[File] norm_bins_nsl
    Array[File] norm_bins_ihh12
    Array[File] norm_bins_delihh
    Array[Array[File]] norm_bins_xpehh

    Int threads = 1
    Int mem_base_gb = 0
    Int mem_per_thread_gb = 1
    Int local_disk_gb = 50
    String docker = "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
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
  }

  Int n_bins_ihh12 = 1
  Int n_bins_xpehh = 1

  scatter(sel_scen_idx in range(length(selection_sims))) {
    Pop sel_pop = object { pop_id: pops_info.sel_pop_ids[sel_scen_idx] }
    Int sel_pop_idx = pops_info.pop_id_to_idx[sel_pop.pop_id]
    scatter(sel_blk_idx in range(length(selection_sims[sel_scen_idx]))) {
    # if (sel_sim.left.succeeded  &&  (sel_sim.left.modelInfo.sweepInfo.selPop == sel_pop)) {
    #   ReplicaInfo sel_sim_replicaInfo = sel_sim.left
    #   Pop sel_pop = object { pop_id: sel_sim_replicaInfo.modelInfo.sweepInfo.selPop }
    #   File sel_sim_region_haps_tar_gz = sel_sim.right
    #   String sel_sim_replica_id_str = modelId + "__selpop_" + sel_pop.pop_id + "__rep_" + sel_sim_replicaInfo.replicaId.replicaNumGlobal
      call tasks.compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_selection {
	input:
	sel_pop=sel_pop,
	region_haps_tar_gzs=selection_sims[sel_scen_idx][sel_blk_idx],

	script=compute_components_script,
	compute_resources=compute_resources_for_compute_one_pop_cms2_components,
	docker=docker,
	preemptible=preemptible
      }
      scatter(alt_pop_idx in range(length(pops_info.pop_ids))) {
	if (alt_pop_idx != sel_pop_idx) {
	  call tasks.compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_selection {
	    input:
	    sel_pop=sel_pop,
	    alt_pop=pops_info.pops[alt_pop_idx],
	    region_haps_tar_gzs=selection_sims[sel_scen_idx][sel_blk_idx],

	    script=compute_components_script,
	    compute_resources=compute_resources_for_compute_two_pop_cms2_components,
	    docker=docker,
	    preemptible=preemptible
	  }
	}
      }  # for each comparison pop

      # should normalize_and_collate be done by blocks?
      call tasks.normalize_and_collate_block {
	input:
	  inp = object {
	    #replica_info: sel_sim_replicaInfo,
	    #replica_id_str: sel_sim_replica_id_str,
	    pop_ids: pops_info.pop_ids,
	    pop_pairs: pops_info.pop_pairs,
	    sel_pop: sel_pop,

	    replica_info: compute_one_pop_cms2_components_for_selection.replicaInfos,
	    ihs_out: compute_one_pop_cms2_components_for_selection.ihs,
	    delihh_out: compute_one_pop_cms2_components_for_selection.delihh,
	    nsl_out: compute_one_pop_cms2_components_for_selection.nsl,
	    ihh12_out: compute_one_pop_cms2_components_for_selection.ihh12,
	    delihh_out: compute_one_pop_cms2_components_for_selection.delihh,
	    derFreq_out: compute_one_pop_cms2_components_for_selection.derFreq,

	    xpehh_out: select_all(compute_two_pop_cms2_components_for_selection.xpehh),
	    fst_and_delDAF_out: select_all(compute_two_pop_cms2_components_for_selection.fst_and_delDAF),

	    n_bins_ihs: n_bins_ihs,
	    n_bins_nsl: n_bins_nsl,
	    n_bins_ihh12: n_bins_ihh12,
	    n_bins_delihh: n_bins_delihh,
	    n_bins_xpehh: n_bins_xpehh,

	    norm_bins_ihs: norm_bins_ihs[sel_pop_idx],
	    norm_bins_nsl: norm_bins_nsl[sel_pop_idx],
	    norm_bins_ihh12: norm_bins_ihh12[sel_pop_idx],
	    norm_bins_delihh: norm_bins_delihh[sel_pop_idx],

	    norm_bins_xpehh: norm_bins_xpehh[sel_pop_idx]
	  }
       } # call tasks.normalize_and_collate_block

       call tasks.collate_stats_and_metadata_for_sel_sims_block {
	    input:
	    inp = object {
	      experimentId: experimentId + "__selscen_" + sel_scen_idx + "__selblk_" + sel_blk_idx,
	      sel_normed_and_collated: normalize_and_collate_block.normed_collated_stats,
	      replica_infos: normalize_and_collate_block.replica_info
	    }
	}  
    }   # for each block of sel sims
    #}  # if (sel_sim.left.succeeded) 
  }  # end: scatter(sel_scen_idx in range(length(selection_sims)))


  output {
    Array[File] all_hapsets_component_stats_h5_blocks =
    flatten(collate_stats_and_metadata_for_sel_sims_block.hapsets_component_stats_h5)
  }
}
