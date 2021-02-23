version 1.0
task bamstats {
    input {
        File bam_input
        Int mem_gb
    }


	command {
		bashgs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/9ba0d46441b75dc5cf163f3f5ff849eb0732d54d/usgs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/9ba0d46441b75dc5cf163f3f5ff849eb0732d54d/locags://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/9ba0d46441b75dc5cf163f3f5ff849eb0732d54d/bigs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/9ba0d46441b75dc5cf163f3f5ff849eb0732d54d/bamstats ${mem_gb} ${bam_input}
	}

	output {
		File bamstats_report = "bamstats_report.zip"
	}

	runtime {
		docker: "quay.igs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/9ba0d46441b75dc5cf163f3f5ff849eb0732d54d/collaboratorgs://fc-21baddbc-5142-4983-a26e-7d85a72c830b/dockstore-tools-cms2/9ba0d46441b75dc5cf163f3f5ff849eb0732d54d/dockstore-tool-bamstats:1.25-7"
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
