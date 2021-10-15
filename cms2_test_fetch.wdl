version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--cc8f59c339f3c93eb279f1cf40f3c5f660060d66/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--cc8f59c339f3c93eb279f1cf40f3c5f660060d66/tasks.wdl"

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
