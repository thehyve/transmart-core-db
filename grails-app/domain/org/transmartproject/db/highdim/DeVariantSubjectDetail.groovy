package org.transmartproject.db.highdim

import org.transmartproject.core.dataquery.vcf.GenomicVariantType
import static org.transmartproject.core.dataquery.vcf.GenomicVariantType.*
import org.transmartproject.core.dataquery.vcf.VcfValues

class DeVariantSubjectDetail implements VcfValues {

    String chromosome
    Long position
    String rsId
    String ref
    String alt
    String quality
    String filter
    String info
    String format
    String variant

    Map<String, String> additionalInfo
    List<String> alternativeAlleles

    static belongsTo = [dataset: DeVariantDataset]
    static transients = ['additionalInfo', 'alternativeAlleles']

    static constraints = {
        alt(nullable: true)
        quality(nullable: true)
        filter(nullable: true)
        info(nullable: true)
        format(nullable: true)
        variant(nullable: true)
    }

    static mapping = {
        table schema: 'deapp'
        version false
        id column: 'variant_subject_detail_id', generator: 'sequence', params: [sequence: 'de_variant_subject_detail_seq']
        dataset column: 'dataset_id'
        chromosome column: 'chr'
        position column: 'pos'
        rsId column: 'rs_id'
        ref column: 'ref'
        alt column: 'alt'
        quality column: 'qual'
        filter column: 'filter'
        info column: 'info'
        format column: 'format'
        variant column: 'variant_value', sqlType: 'clob'
    }

    @Override
    String getMafAllele() {
        if (!getAdditionalInfo()['AF']) {
            return null
        }
        def afs = parseNumbersList(additionalInfo['AF'])
        if (!afs) {
            return null
        }
        def maf = afs.max()
        def mafIndx = afs.indexOf(maf)
        def alts = alt.split(/\s*,\s*/)
        assert mafIndx < alts.length, "Could not find $mafIndx position in $alt"
        alts[mafIndx]
    }

    @Override
    Double getMaf() {
        if (!getAdditionalInfo()['AF']) {
            return null
        }
        def afs = parseNumbersList(additionalInfo['AF'])
        if (!afs) {
            return null
        }
        afs.max()
    }

    @Override
    Double getQualityOfDepth() {
        if (!getAdditionalInfo()['QD'] || !additionalInfo['QD'].isNumber()) {
            return null
        }
        Double.valueOf(additionalInfo['QD'])
    }

    @Override
    String getReferenceAllele() { ref }

    @Override
    List<String> getAlternativeAlleles() {
        if(alternativeAlleles == null) {
            alternativeAlleles = parseCsvString(alt)
        }
        alternativeAlleles
    }

    @Override
    Map<String, String> getAdditionalInfo() {
        if (additionalInfo == null) {
            additionalInfo = parseVcfInfo(info)
        }
        additionalInfo
    }

    @Override
    List<GenomicVariantType> getGenomicVariantTypes() {
        getGenomicVariantTypes(ref, getAlternativeAlleles())
    }

    List<GenomicVariantType> getGenomicVariantTypes(Collection<String> altCollection) {
        getGenomicVariantTypes(ref, altCollection)
    }

    static List<GenomicVariantType> getGenomicVariantTypes(String ref, Collection<String> altCollection) {
        altCollection.collect{ getGenomicVariantType(ref, it) }
    }

    static GenomicVariantType getGenomicVariantType(String ref, String alt) {
        def refCleaned = ref.replaceAll(/[^ACGT]/, '')
        def altCleaned = alt.replaceAll(/[^ACGT]/, '')

        if(refCleaned.length() == 1 && altCleaned.length() == 1)
            return SNP

        if(altCleaned.length() > refCleaned.length()
                && altCleaned.contains(refCleaned))
            return INS

        if(altCleaned.length() < refCleaned.length()
                && refCleaned.contains(altCleaned))
            return DEL

        DIV
    }

    List<String> getAltAllelesByPositions(Collection<Integer> pos) {
        pos.collect { getAlternativeAlleles()[it - 1] }
    }

    private setAdditionalInfo(Map<String, String> info) { this.info = info }

    private Map parseVcfInfo(String info) {
        if (!info) {
            return [:]
        }

        info.split(';').collectEntries {
            def keyValues = it.split('=')
            [(keyValues[0]): keyValues.length > 1 ? keyValues[1] : 'Yes']
        }
    }

    private List<Double> parseNumbersList(String numbersString) {
        parseCsvString(numbersString) {
            it.isNumber() ? Double.valueOf(it) : null
        }
    }

    private List parseCsvString(String string, Closure typeConverterClosure = { it }) {
        if (!string) {
            return []
        }

        string.split(/\s*,\s*/).collect {
            typeConverterClosure(it)
        }
    }
}
