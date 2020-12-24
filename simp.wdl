version 1.0

task example {
  input {
    Array[String] pfx_val
  }
  String prefix_as_str = '~{prefix("mydata_", pfx_val)}'
  String out_fname = sub(prefix_as_str, "\\W+", "_")
  command <<<
    echo "fname is ~{out_fname}"
    echo "hi there" > "~{out_fname}"
  >>>
  output {
    Array[String] analyzed_fnames = [prefix_as_str, out_fname]
    File analyzed = out_fname
  }
}

workflow my_example {
  input {
     Array[String] comps = ["ihs", "ihh12", "xpehh"]
     Array[Int] pops = [1,4,5]
  }
  String comps_val = "~{comps}"
  String comps_map = "~{zip(comps,pops)}"
  Array[String] for_pfx = [comps_val, comps_map]
  call example { input: pfx_val = for_pfx }
  scatter(comp in comps) {
    scatter(pop in pops) {
       String comp_with_pop = comp + "_" + pop
    }
  }
  output {
    Array[String] out_fnames = flatten(comp_with_pop)
    String out_comps_map = comps_map
    String out_comps_val = comps_val
    Array[String] out_analyzed_fnames = example.analyzed_fnames
    File out_analyzed = example.analyzed
  }
  # call example {
  #   input:
  #      pfx_val = prefix("data_", result)
  # }
  # output {
  #    Array[String] result_fnames = example.analyzed_fnames
  #    File result = example.analyzed
  # }
}

