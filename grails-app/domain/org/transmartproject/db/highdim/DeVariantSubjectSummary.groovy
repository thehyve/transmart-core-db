package org.transmartproject.db.highdim

class DeVariantSubjectSummary {

    static final Integer REF_ALLELE = 0
    String chromosome
    Long position
    String subjectId
    String rsId
    String variant
    String variantFormat
    String variantType
    Boolean reference
    Integer allele1
    Integer allele2

    static belongsTo = [dataset: DeVariantDataset, assay: DeSubjectSampleMapping]

    static constraints = {
        variant(nullable: true)
        variantFormat(nullable: true)
        variantType(nullable: true)
    }

    static mapping = {
        table schema: 'deapp'
        version false
        id column:'variant_subject_summary_id', generator: 'sequence', params: [sequence: 'de_variant_subject_summary_seq']
        chromosome column: 'chr'
        position column: 'pos'
        dataset column: 'dataset_id'
        assay   column: 'assay_id'
    }
}
