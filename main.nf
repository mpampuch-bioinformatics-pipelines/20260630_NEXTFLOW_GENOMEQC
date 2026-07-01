#!/usr/bin/env nextflow
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    nf-core/pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Github : https://github.com/nf-core/pipeline
----------------------------------------------------------------------------------------
*/

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT FUNCTIONS / MODULES / SUBWORKFLOWS / WORKFLOWS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

include { PIPELINE                } from './workflows/pipeline'
include { PIPELINE_INITIALISATION } from './subworkflows/local/utils_nfcore_pipeline_pipeline'
include { PIPELINE_COMPLETION     } from './subworkflows/local/utils_nfcore_pipeline_pipeline'
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    NAMED WORKFLOWS FOR PIPELINE
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

//
// WORKFLOW: Run main analysis pipeline depending on type of input
//
workflow NFCORE_PIPELINE {
    take:
    samplesheet // channel: samplesheet read in from --input

    main:

    //
    // WORKFLOW: Run pipeline
    //
    PIPELINE(
        samplesheet
    )

    emit:
    multiqc_report = PIPELINE.out.multiqc_report // channel: /path/to/multiqc_report.html
    versions       = PIPELINE.out.versions // channel: [ path(versions.yml) ]
    asmstats       = PIPELINE.out.asmstats
    gfastats       = PIPELINE.out.gfastats
    busco          = PIPELINE.out.busco
    merqury        = PIPELINE.out.merqury // optional: only emitted when reads are provided
}
/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow {

    main:
    //
    // SUBWORKFLOW: Run initialisation tasks
    //
    PIPELINE_INITIALISATION(
        params.version,
        params.validate_params,
        params.monochrome_logs,
        args,
        params.outdir,
        params.input,
        params.help,
        params.help_full,
        params.show_hidden,
    )

    //
    // WORKFLOW: Run main workflow
    //
    NFCORE_PIPELINE(
        PIPELINE_INITIALISATION.out.samplesheet
    )
    //
    // SUBWORKFLOW: Run completion tasks
    //
    PIPELINE_COMPLETION(
        params.email,
        params.email_on_fail,
        params.plaintext_email,
        params.outdir,
        params.monochrome_logs,
        NFCORE_PIPELINE.out.multiqc_report,
    )

    publish:
    asmstats = NFCORE_PIPELINE.out.asmstats
    gfastats = NFCORE_PIPELINE.out.gfastats
    busco    = NFCORE_PIPELINE.out.busco
    merqury  = NFCORE_PIPELINE.out.merqury // optional: only emitted when reads are provided
    versions = NFCORE_PIPELINE.out.versions // channel: [ path(versions.yml) ]
}

output {
    asmstats {
        path { meta, _file ->
            "${meta.id}/asmstats"
        }
        mode params.publish_dir_mode
    }
    gfastats {
        path { meta, _file ->
            "${meta.id}/gfastats"
        }
        mode params.publish_dir_mode
    }
    busco {
        path { meta, _file ->
            "${meta.id}/busco"
        }
        mode params.publish_dir_mode
    }
    merqury {
        path { meta, _file ->
            "${meta.id}/merquryfk"
        }
        mode params.publish_dir_mode
    }
    versions {
    }
}
