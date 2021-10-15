version 1.0

import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--318e9138b388cbb8bf690190993b77eb0ae07198/structs.wdl"
import "https://raw.githubusercontent.com/notestaff/cms2-staging/staging-is-211013-1315-add-isafe--318e9138b388cbb8bf690190993b77eb0ae07198/tasks.wdl"

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
