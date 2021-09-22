version 1.0

workflow wdl_assert_wf {
  input {
    Boolean cond
    String msg
  }
  if (!cond) {
    Array[Boolean] error = select_first([])
  }
  output {
    String checked_cond_msg = msg
  }
}

