package org.transmartproject.db.highdim

class DeVariantDataset {

    String id
    String datasourceId
    String etlId
    Date etlDate
    String genome
    String metadataComment
    String variantDatasetType

    static hasMany = [summaries: DeVariantSubjectSummary, details: DeVariantSubjectDetail]

    static constraints = {
        datasourceId(nullable: true)
        etlId(nullable: true)
        etlDate(nullable: true)
        metadataComment(nullable: true)
        variantDatasetType(nullable: true)
    }

    static mapping = {
        table schema: 'deapp'
        version false
        id column:'dataset_id', generator: 'assigned'
    }
}
