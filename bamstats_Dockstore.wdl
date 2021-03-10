version 1.0
task bamstats {
    input {
        File bam_input
        Int mem_gb
    }


	command {
		bashgs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/bbb1c3caa96531715a9ca6aa37ad89c205e7341b/usgs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/bbb1c3caa96531715a9ca6aa37ad89c205e7341b/locags://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/bbb1c3caa96531715a9ca6aa37ad89c205e7341b/bigs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/bbb1c3caa96531715a9ca6aa37ad89c205e7341b/bamstats ${mem_gb} ${bam_input}
	}

	output {
		File bamstats_report = "bamstats_report.zip"
	}

	runtime {
		docker: "quay.igs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/bbb1c3caa96531715a9ca6aa37ad89c205e7341b/collaboratorgs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/bbb1c3caa96531715a9ca6aa37ad89c205e7341b/dockstore-tool-bamstats:1.25-7"
		memory: mem_gb + "GB"
	}

	meta {
		author: "Andrew Duncan"
	}
}

workflow bamstatsWorkflow {
    input {
        File bam_input
        Int mem_gb
    }
	call bamstats { input: bam_input=bam_input, mem_gb=mem_gb }
}
