package org.transmartproject.db.dataquery.highdim.vcf

import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode()
class DeVariantPopulationData implements Serializable {
    String name
    Long idx
    
    Long intValue
    Double floatValue
    String textValue
    
    // Actually this data object should also belong to a DeVariantPopulationInfo
    // object. However, this mapping cannot be properly created using GORM, so 
    // a getter is manually added.
    // See http://grails.1312388.n4.nabble.com/Repeated-Column-MappingException-tp3662719p3663236.html
    @Lazy
    DeVariantPopulationInfo infoField = {
        DeVariantPopulationInfo.findByDatasetAndName( dataset, name )
    }()
    
    static transients = [ 'infoField' ]
    
    static belongsTo = [
            subjectDetail: DeVariantSubjectDetailCoreDb,
            dataset: DeVariantDatasetCoreDb
    ]

    static constraints = {
        idx         nullable: true
        intValue    nullable: true
        floatValue  nullable: true
        textValue   nullable: true
    }

    static mapping = {
        table     schema:  'deapp', name: 'de_variant_population_data'
        version   false

        id  column: 'variant_population_data_id', 
            generator: 'sequence',
            params: [sequence: 'de_variant_population_data_seq', schema: 'deapp']

        dataset     column: 'dataset_id'
        idx         column: 'info_index'
        intValue    column: 'integer_value'
        floatValue  column: 'float_value'
        textValue   column: 'text_value'
        name        column: 'info_name'
        
        columns {
            subjectDetail {
                column name: 'dataset'
                column name: 'chr'
                column name: 'pos'
            }
        }
    }

}
