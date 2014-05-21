package org.transmartproject.db.dataquery.highdim.vcf

import groovy.transform.EqualsAndHashCode

@EqualsAndHashCode()
class DeVariantPopulationInfo implements Serializable {
    String name  
    String description
    String type
    String number      // Can be a number, or A or G
    
    static belongsTo = [dataset: DeVariantDatasetCoreDb]

    static constraints = {
        name        nullable: true
        description nullable: true
        type        nullable: true
        number      nullable: true
    }

    static mapping = {
        table     schema:  'deapp', name: 'de_variant_population_info'
        version   false

        id  composite: ['dataset', 'name']

        dataset     column: 'dataset_id'
        name        column: 'info_name'
        description column: 'description'
        type        column: 'type'
        number      column: 'number'
    }

}
