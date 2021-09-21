version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--05300818036fd1b9866c0acb7ff3e89b66d85b0c/run_sims.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-210914-1648-add-nre-wdl--05300818036fd1b9866c0acb7ff3e89b66d85b0c/tasks.wdl"

# * workflow compute_normalization_stats_wf
workflow compute_normalization_stats_wf {
  meta {
    description: "Computes stats (means and stds) needed to normalize each component score.  For each component score, for each selected pop or combo of (selected pop, alternate pop), computes that component score for SNPs in neutral hapsets, then computes the mean and std of that score for SNPs within each frequency bin."
  }

  parameter_meta {
# ** inputs
    out_fnames_prefix: "(String) Prefix for naming output files"
    
    hapsets_per_block: "(Int) Number of hapsets to process together when computing component scores"
    
# ** outputs
    
  }

  input {
    String out_fnames_prefix
    PopsInfo pops_info
    Array[File]+ neutral_hapsets

    ComponentComputationParams component_computation_params

    Int hapset_block_size = 2
  }  # end: input

  Array[Pop]+ pops = pops_info.pops
  Int n_pops = length(pops)

  Int n_hapset_blocks = length(neutral_hapsets) / hapset_block_size

  scatter(hapset_block_num in range(n_hapset_blocks)) {
      scatter(hapset_block_offset in range(hapset_block_size)) {
	Int idx = hapset_block_num * hapset_block_size + hapset_block_offset
	File neutral_hapsets_in_block = neutral_hapsets[idx]
      }
  }
  scatter(sel_pop in pops) {
    scatter(hapsets_block in neutral_hapsets_in_block) {
      call tasks.compute_one_pop_cms2_components as compute_one_pop_cms2_components_for_neutral {
	input:
	sel_pop=sel_pop,
	hapsets=hapsets_block
      }
    }

# **** Compute normalization stats for one-pop components for neutral sims
    call tasks.compute_one_pop_bin_stats_for_normalization {
      input:
      out_fnames_prefix=out_fnames_prefix + "__selpop_" + sel_pop.pop_id,
      sel_pop=sel_pop,

      ihs_out=flatten(compute_one_pop_cms2_components_for_neutral.ihs),
      nsl_out=flatten(compute_one_pop_cms2_components_for_neutral.nsl),
      ihh12_out=flatten(compute_one_pop_cms2_components_for_neutral.ihh12),
      delihh_out=flatten(compute_one_pop_cms2_components_for_neutral.delihh),

      n_bins_ihs=component_computation_params.n_bins_ihs,
      n_bins_nsl=component_computation_params.n_bins_nsl,
      n_bins_delihh=component_computation_params.n_bins_delihh
    }  # end: call tasks.compute_one_pop_bin_stats_for_normalization
  }  # end: scatter(sel_pop in pops)

# **** Compute two-pop CMS2 components for neutral sims
   scatter(sel_pop_idx in range(n_pops)) {
     scatter(alt_pop_idx in range(n_pops)) {
       if ((alt_pop_idx > sel_pop_idx) &&
           (pops_info.pop_alts_used[sel_pop_idx][alt_pop_idx] || pops_info.pop_alts_used[alt_pop_idx][sel_pop_idx])) {
	 scatter(hapsets_block in neutral_hapsets_in_block) {
	   call tasks.compute_two_pop_cms2_components as compute_two_pop_cms2_components_for_neutral {
	     input:
	     sel_pop=pops[sel_pop_idx],
	     alt_pop=pops[alt_pop_idx],
	     hapsets=hapsets_block
	   }
         }

	 call tasks.compute_two_pop_bin_stats_for_normalization {
	   input:
	   out_fnames_prefix=out_fnames_prefix,
	   sel_pop=pops[sel_pop_idx],
	   alt_pop=pops[alt_pop_idx],

	   xpehh_out=flatten(compute_two_pop_cms2_components_for_neutral.xpehh),
	 }
       }
     }
  }

  scatter(sel_pop_idx in range(n_pops)) {
    scatter(alt_pop_idx in range(n_pops)) {
      if ((alt_pop_idx != sel_pop_idx) && 
          (pops_info.pop_alts_used[sel_pop_idx][alt_pop_idx] || pops_info.pop_alts_used[alt_pop_idx][sel_pop_idx])) {
  	File norm_bins_xpehh_maybe = 
        select_first([
        compute_two_pop_bin_stats_for_normalization.norm_bins_xpehh[sel_pop_idx][alt_pop_idx],
        compute_two_pop_bin_stats_for_normalization.norm_bins_flip_pops_xpehh[alt_pop_idx][sel_pop_idx]
        ])
  	Pop norm_bins_xpehh_sel_pop_used_maybe = 
        select_first([
        compute_two_pop_bin_stats_for_normalization.sel_pop_used[sel_pop_idx][alt_pop_idx],
        compute_two_pop_bin_stats_for_normalization.flip_pops_sel_pop_used[alt_pop_idx][sel_pop_idx]
        ])
  	Pop norm_bins_xpehh_alt_pop_used_maybe = 
        select_first([
        compute_two_pop_bin_stats_for_normalization.alt_pop_used[sel_pop_idx][alt_pop_idx],
        compute_two_pop_bin_stats_for_normalization.flip_pops_alt_pop_used[alt_pop_idx][sel_pop_idx]
        ])
      }
    }
    Array[File]+ norm_bins_xpehh_vals = select_all(norm_bins_xpehh_maybe)
    Array[Pop]+ norm_bins_xpehh_sel_pop_used_vals = select_all(norm_bins_xpehh_sel_pop_used_maybe)
    Array[Pop]+ norm_bins_xpehh_alt_pop_used_vals = select_all(norm_bins_xpehh_alt_pop_used_maybe)
  }  # end: scatter(sel_pop_idx in range(length(pops)))

  output {
    Array[File]+ norm_bins_ihs=compute_one_pop_bin_stats_for_normalization.norm_bins_ihs
    Array[File]+ norm_bins_nsl=compute_one_pop_bin_stats_for_normalization.norm_bins_nsl
    Array[File]+ norm_bins_ihh12=compute_one_pop_bin_stats_for_normalization.norm_bins_ihh12
    Array[File]+ norm_bins_delihh=compute_one_pop_bin_stats_for_normalization.norm_bins_delihh
    Array[Array[File]+]+ norm_bins_xpehh = norm_bins_xpehh_vals

    Array[Pop]+ one_pop_bin_stats_sel_pop_used = compute_one_pop_bin_stats_for_normalization.sel_pop_used
    Array[Array[Pop]+]+ two_pop_bin_stats_sel_pop_used = norm_bins_xpehh_sel_pop_used_vals
    Array[Array[Pop]+]+ two_pop_bin_stats_alt_pop_used = norm_bins_xpehh_alt_pop_used_vals
  }
}
