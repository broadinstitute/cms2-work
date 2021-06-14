task old_compute_one_pop_cms2_components {
  meta {
    description: "Compute one-pop CMS2 component scores assuming selection in a given pop"
  }
  input {
    File region_haps_tar_gz
    Pop sel_pop

    File script = "./old_remodel_components.py"
    String docker
    Int preemptible
    ComputeResources compute_resources
  }

  String out_basename = basename(region_haps_tar_gz, ".tar.gz") + "__selpop_" + sel_pop.pop_id
  String script_used_name = "script-used." + basename(script)

  String ihs_out_fname = out_basename + ".ihs.out"
  String nsl_out_fname = out_basename + ".nsl.out"
  String ihh12_out_fname = out_basename + ".ihh12.out"
  String delihh_out_fname = out_basename + ".delihh.out"
  String derFreq_out_fname = out_basename + ".derFreq.tsv"

  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --replica-id-string "~{out_basename}" \
      --out-basename "~{out_basename}" --sel-pop ~{sel_pop.pop_id} --threads ~{compute_resources.cpus} --components ihs nsl ihh12 delihh derFreq
  >>>

  output {
    File ihs = ihs_out_fname
    File nsl = nsl_out_fname
    File ihh12 = ihh12_out_fname
    File delihh = delihh_out_fname
    File derFreq = derFreq_out_fname

    File script_used = script_used_name
  }

  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: select_first([compute_resources.mem_gb, 4]) + " GB"
    cpu: select_first([compute_resources.cpus, 1])
    disks: "local-disk " + select_first([compute_resources.local_storage_gb, 50]) + " HDD"
  }
}

task old_compute_two_pop_cms2_components {
  meta {
    description: "Compute cross-pop comparison CMS2 component scores"
  }
# ** inputs
  input {
#    ReplicaInfo replicaInfo
    File region_haps_tar_gz
    Pop sel_pop
    Pop alt_pop

    #File? xpehh_bins

    File script = "./old_remodel_components.py"
    ComputeResources compute_resources
    String docker
    Int preemptible
  }
  String out_basename = basename(region_haps_tar_gz) + "__selpop_" + sel_pop.pop_id + "__altpop_" + alt_pop.pop_id
  String script_used_name = out_basename + ".script-used." + basename(script)

  String xpehh_out_fname = out_basename + ".xpehh.out"
  String xpehh_log_fname = out_basename + ".xpehh.log"

  String fst_and_delDAF_out_fname = out_basename + ".fst_and_delDAF.tsv"

# ** command
  command <<<
    tar xvfz "~{region_haps_tar_gz}"

    cp "~{script}" "~{script_used_name}"
    python3 "~{script}" --replica-info *.replicaInfo.json --out-basename "~{out_basename}" \
        --replica-id-string "~{out_basename}" --sel-pop ~{sel_pop.pop_id} --alt-pop ~{alt_pop.pop_id} \
        --threads ~{compute_resources.cpus} --components xpehh fst delDAF
  >>>

# ** outputs
  output {
    File xpehh = xpehh_out_fname
    File xpehh_log = xpehh_log_fname
    File fst_and_delDAF = fst_and_delDAF_out_fname
    Pop sel_pop_used = sel_pop
    Pop alt_pop_used = alt_pop
    File script_used = script_used_name
  }

# ** runtime
  runtime {
    docker: "quay.io/ilya_broad/cms@sha256:fc4825edda550ef203c917adb0b149cbcc82f0eeae34b516a02afaaab0eceac6"  # selscan=1.3.0a09
    preemptible: preemptible
    memory: select_first([compute_resources.mem_gb, 4]) + " GB"
    cpu: select_first([compute_resources.cpus, 1])
    disks: "local-disk " + select_first([compute_resources.local_storage_gb, 50]) + " HDD"
  }
}

