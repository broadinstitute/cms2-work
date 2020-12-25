version 1.0

struct Pop {
  Int val
}

task example {
  input {
    Array[String] pfx_val
    Pop pops1 = object {val: 1}
    Pop pops2 = object {val: 4}
  }
  String prefix_as_str = '~{prefix("mydata_", pfx_val)}'
  String out_fname = sub(prefix_as_str, "\\W+", "_")
  #Boolean test_bool = ([pfx_val] == [pfx_val, pfx_val])
  Boolean test_bool2 = (pops1.val == pops2.val)
  command <<<
    echo "fname is ~{out_fname}"
    echo "hi there" > "~{out_fname}"
    echo '{"val": {"Left": 1, "Right": 2}}' > npair.json
  >>>
  output {
    Array[String] analyzed_fnames = [prefix_as_str, out_fname]
    File analyzed = out_fname
    Pair[Int,Int] ppair = read_json("npair.json").val
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
    Pair[Int,Int] out_ppair = (example.ppair.left + 1, example.ppair.right-1)
    String out_version = "GITCOMMIT"
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

