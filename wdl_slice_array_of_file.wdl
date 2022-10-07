version 1.0

workflow wdl_slice_array_of_file_wf {
  input {
    Array[File] inp
    Int beg_idx = 0
    Int end_idx = length(inp)
  }
  scatter(idx in range(length(inp))) {
    if ((beg_idx <= idx) && (idx < end_idx)) {
       File out_maybe = inp[idx]
    }
  }
  output {
    Array[File] out = select_all(out_maybe)
  }
}

