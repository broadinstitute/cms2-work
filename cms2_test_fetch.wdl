version 1.0

import "./structs.wdl"
import "./tasks.wdl"

workflow cms2_test_fetch {
  input {
    String url
  }

  call tasks.fetch_file_from_url {
    input:
    url=url
  }
  output {
    File out_file = fetch_file_from_url.file
  }
}
