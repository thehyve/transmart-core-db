package org.transmartproject.db.dataquery.vcf

import org.transmartproject.core.dataquery.vcf.GenomicVariantType
import org.transmartproject.core.dataquery.vcf.VcfValues

class VcfValuesImpl implements VcfValues {
    String chromosome
    Long position
    String rsId
    String mafAllele
    Double maf
    Double qualityOfDepth
    String referenceAllele
    List<String> alternativeAlleles
    Map<String, String> additionalInfo
    List<GenomicVariantType> genomicVariantTypes
}
