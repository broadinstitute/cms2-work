version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--86cb84a5a9605e47478b7c032c15d9b9f1c11bff/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--86cb84a5a9605e47478b7c032c15d9b9f1c11bff/tasks.wdl"

workflow cms2_test_fetch {
  input {
    String url
    String sha256
  }

  call tasks.fetch_file_from_url {
    input:
    url=url, sha256=sha256
  }
  output {
    File out_file = fetch_file_from_url.file
  }
}
