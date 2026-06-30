/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    IMPORT MODULES / SUBWORKFLOWS / FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/
include { GUNZIP                 } from '../modules/nf-core/gunzip/main'
include { GENOME_STATISTICS      } from '../subworkflows/sanger-tol/genome_statistics/main'
include { BUILD_KMER_DATABASES   } from '../subworkflows/local/build_kmer_databases/main'
include { MULTIQC                } from '../modules/nf-core/multiqc/main'
include { paramsSummaryMap       } from 'plugin/nf-schema'
include { paramsSummaryMultiqc   } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { softwareVersionsToYAML } from '../subworkflows/nf-core/utils_nfcore_pipeline'
include { methodsDescriptionText } from '../subworkflows/local/utils_nfcore_pipeline_pipeline'

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    RUN MAIN WORKFLOW
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

workflow PIPELINE {
    take:
    ch_samplesheet // channel: [ val(meta), assembly_fasta, busco_lineage, reads? ]

    main:

    ch_versions = channel.empty()
    ch_multiqc_files = channel.empty()

    //
    // Prepare assembly inputs and decompress gzipped FASTA files when required
    //
    ch_assembly_input = ch_samplesheet.map { meta, assembly, lineage, reads = null, _platform = null ->
        [meta, file(assembly, checkIfExists: true), lineage, reads]
    }

    ch_assembly_branched = ch_assembly_input.branch { meta, assembly, lineage, reads ->
        gunzip: assembly.name.endsWith('.gz') || assembly.extension == 'gz'
        plain: true
        return [meta, assembly, lineage, reads]
    }

    GUNZIP(
        ch_assembly_branched.gunzip.map { meta, assembly, _lineage, _reads ->
            [meta, assembly]
        }
    )

    ch_assemblies_ready = GUNZIP.out.gunzip
        .combine(
            ch_assembly_branched.gunzip.map { meta, _assembly, lineage, reads ->
                [meta, lineage, reads]
            },
            by: 0
        )
        .map { meta, assembly, lineage, reads ->
            [meta, assembly, lineage, reads]
        }
        .mix(ch_assembly_branched.plain)

    //
    // Build FastK databases from optional read data for MerquryFK (skipped when reads absent)
    //
    ch_kmer_data = ch_assemblies_ready
        .filter { _meta, _assembly, _lineage, reads ->
            hasReadsInput(reads)
        }
        .map { meta, _assembly, _lineage, reads ->
            def platform = meta.platform ?: params.read_platform
            [meta + [platform: platform], reads, []]
        }

    BUILD_KMER_DATABASES(
        channel.empty(),
        ch_kmer_data,
        params.kmer_size,
    )

    ch_fastk = BUILD_KMER_DATABASES.out.data
        .filter { data -> data.fk_hist && !(data.fk_hist instanceof List && data.fk_hist.isEmpty()) }
        .map { data ->
            [
                sampleMeta(data),
                data.fk_hist,
                data.fk_ktab ?: [],
                [],
                [],
            ]
        }

    //
    // Build channels expected by GENOME_STATISTICS
    //
    ch_assemblies = ch_assemblies_ready.map { meta, assembly, _lineage, _reads ->
        [sampleMeta(meta), assembly, []]
    }

    ch_busco_lineage = ch_assemblies_ready.map { meta, _assembly, lineage, _reads ->
        [sampleMeta(meta), lineage]
    }

    def busco_lineage_dir = params.busco_lineage_directory
        ? file(params.busco_lineage_directory, checkIfExists: true)
        : []

    GENOME_STATISTICS(
        ch_assemblies,
        ch_fastk,
        ch_busco_lineage,
        busco_lineage_dir,
    )

    //
    // Collate and save software versions
    //
    def topic_versions = Channel
        .topic("versions")
        .distinct()
        .branch { entry ->
            versions_file: entry instanceof Path
            versions_tuple: true
        }

    def topic_versions_string = topic_versions.versions_tuple
        .map { process, tool, version ->
            [process[process.lastIndexOf(':') + 1..-1], "  ${tool}: ${version}"]
        }
        .groupTuple(by: 0)
        .map { process, tool_versions ->
            tool_versions.unique().sort()
            "${process}:\n${tool_versions.join('\n')}"
        }

    softwareVersionsToYAML(ch_versions.mix(topic_versions.versions_file))
        .mix(topic_versions_string)
        .collectFile(
            storeDir: "${params.outdir}/pipeline_info",
            name: 'pipeline_software_' + 'mqc_' + 'versions.yml',
            sort: true,
            newLine: true,
        )
        .set { ch_collated_versions }


    //
    // MODULE: MultiQC
    //
    ch_multiqc_config = channel.fromPath(
        "${projectDir}/assets/multiqc_config.yml",
        checkIfExists: true
    )
    ch_multiqc_custom_config = params.multiqc_config
        ? channel.fromPath(params.multiqc_config, checkIfExists: true)
        : channel.empty()
    ch_multiqc_logo = params.multiqc_logo
        ? channel.fromPath(params.multiqc_logo, checkIfExists: true)
        : channel.empty()

    summary_params = paramsSummaryMap(
        workflow,
        parameters_schema: "nextflow_schema.json"
    )
    ch_workflow_summary = channel.value(paramsSummaryMultiqc(summary_params))
    ch_multiqc_files = ch_multiqc_files.mix(
        ch_workflow_summary.collectFile(name: 'workflow_summary_mqc.yaml')
    )
    ch_multiqc_custom_methods_description = params.multiqc_methods_description
        ? file(params.multiqc_methods_description, checkIfExists: true)
        : file("${projectDir}/assets/methods_description_template.yml", checkIfExists: true)
    ch_methods_description = channel.value(
        methodsDescriptionText(ch_multiqc_custom_methods_description)
    )

    ch_multiqc_files = ch_multiqc_files.mix(ch_collated_versions)
    ch_multiqc_files = ch_multiqc_files.mix(
        ch_methods_description.collectFile(
            name: 'methods_description_mqc.yaml',
            sort: true,
        )
    )

    MULTIQC(
        ch_multiqc_files.collect(),
        ch_multiqc_config.toList(),
        ch_multiqc_custom_config.toList(),
        ch_multiqc_logo.toList(),
        [],
        [],
    )

    emit:
    multiqc_report = MULTIQC.out.report.toList() // channel: /path/to/multiqc_report.html
    versions       = ch_versions // channel: [ path(versions.yml) ]
    stats          = GENOME_STATISTICS.out.stats
    busco          = GENOME_STATISTICS.out.busco
    merqury        = GENOME_STATISTICS.out.merqury // optional: only emitted when reads are provided
}

/*
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    FUNCTIONS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
*/

def hasReadsInput(reads) {
    if (!reads) {
        return false
    }
    if (reads instanceof List) {
        return reads.any { it?.toString()?.trim() }
    }
    return reads.toString().trim() as boolean
}

def sampleMeta(meta) {
    meta instanceof Map ? meta.subMap(['id']) : [id: meta.id]
}
