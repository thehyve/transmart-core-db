package org.transmartproject.db.dataquery.highdim.vcf

import org.transmartproject.db.dataquery.highdim.DeGplInfo
import org.transmartproject.db.dataquery.highdim.DeSubjectSampleMapping
import org.transmartproject.db.dataquery.highdim.HighDimTestData
import org.transmartproject.db.i2b2data.PatientDimension

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.is
import static org.hamcrest.Matchers.notNullValue
import static org.transmartproject.db.dataquery.highdim.HighDimTestData.save

/**
 * Created by j.hudecek on 13-3-14.
 */
class VcfTestData  {

    public static final String TRIAL_NAME = 'VCF_SAMP_TRIAL'

    DeGplInfo platform
    DeGplInfo otherPlatform
    DeVariantDatasetCoreDb dataset
    List<PatientDimension> patients
    List<DeSubjectSampleMapping> assays
    List<DeVariantSubjectSummaryCoreDb> summariesData
    List<DeVariantSubjectDetailCoreDb> detailsData
    List<DeVariantSubjectIdxCoreDb> indexData

    List<DeVariantPopulationInfo> infoFields
    List<DeVariantPopulationData> infoFieldContents
        
    public VcfTestData() {
        // Create VCF platform and assays
        platform = new DeGplInfo(
                    title: 'Test VCF',
                    organism: 'Homo Sapiens',
                    markerType: 'VCF')
        platform.id = 'BOGUSGPLVCF'
        dataset = new DeVariantDatasetCoreDb(genome:'human')
        dataset.id = 'BOGUSDTST'
        patients = HighDimTestData.createTestPatients(3, -800, TRIAL_NAME)
        assays = HighDimTestData.createTestAssays(patients, -1400, platform, TRIAL_NAME)
        
        // Setup 3 info fields
        infoFields = []
        infoFields << new DeVariantPopulationInfo( dataset: dataset, name: "NS", type: "Flag", number: "0", description: "Number of samples with data" )
        infoFields << new DeVariantPopulationInfo( dataset: dataset, name: "AF", type: "Integer", number: "A", description: "Allele frequency" )
        infoFields << new DeVariantPopulationInfo( dataset: dataset, name: "ID", type: "String", number: "1", description: "Identifier of this line; for testing purposes only" )
        
        // Create VCF data
        detailsData = []
        detailsData += createDetail(1, 'C', 'A', 'DP=88;AF1=1;QD=2;DP4=0,0,80,0;MQ=60;FQ=-268')
        detailsData += createDetail(2, 'GCCCCC', 'GCCCC', 'DP=88;AF1=1;QD=2;DP4=0,0,80,0;MQ=60;FQ=-268')
        detailsData += createDetail(3, 'A', 'C,T', 'DP=88;AF1=1;QD=2;DP4=0,0,80,0;MQ=60;FQ=-268')

        summariesData = []
        detailsData.each { detail ->
            // Create VCF summary entries with the following variants:
            // 1/0, 0/1 and 1/1
            int mut = 0
            assays.each { assay ->
                mut++
                summariesData += createSummary detail, mut&1, (mut&2)>>1,  assay
            }
            if (detail.alt.contains(','))
                summariesData.last().allele1=2
        }
        
        indexData = []
        assays.eachWithIndex { assay, idx ->
            indexData << new DeVariantSubjectIdxCoreDb(
                dataset: dataset,
                subjectId: assay.sampleCode,
                position: idx + 1
            )
        }
        
        infoFieldContents = []
        detailsData.each { detail ->
            infoFieldContents += createPopulationData( detail )
        }
        
        // Add also another platform and assays for those patients
        // to test whether the VCF module only returns VCF assays
        otherPlatform = new DeGplInfo(
            title: 'Other platform',
            organism: 'Homo Sapiens',
            markerType: 'mrna')
        otherPlatform.id = 'BOGUSGPLMRNA'
        
        assays += HighDimTestData.createTestAssays(patients, -1800, otherPlatform, "OTHER_TRIAL")
    }

    def createDetail = {
        int position,
        String reference,
        String alternative,
        String info
            ->
            new DeVariantSubjectDetailCoreDb(
                    chr: 1,
                    pos: position,
                    rsId: '.',
                    ref: reference,
                    alt: alternative,
                    quality: position, //nonsensical value
                    filter: '.',
                    info:  info,
                    format: 'GT',
                    dataset: dataset,
                    variant: "" + position + "/" + position + "\t" + ( position + 1 ) + "/" + ( position + 1 ) + "\t" + ( position * 2 ) + "/" + ( position * 2 ) 
            )
    }

    def createSummary = {
        DeVariantSubjectDetailCoreDb detail,
        int allele1,
        int allele2,
        DeSubjectSampleMapping assay
            ->

            new DeVariantSubjectSummaryCoreDb(
                    chr: 1,
                    pos: detail.pos,
                    rsId: '.',
                    variant: ( (allele1 == 0)? detail.ref : detail.alt) + '/' + ( (allele2 == 0)? detail.ref : detail.alt),
                    variantFormat: ( (allele1 == 0) ? 'R':'V') + '/' + ( (allele2 == 0) ? 'R':'V'),
                    variantType: detail.ref.length()>1?'DIV':'SNV',
                    reference: true,
                    allele1: allele1,
                    allele2: allele2,
                    subjectId: assay.sampleCode,
                    dataset: dataset,
                    assay: assay,
                    jDetail: detail
            )
    }
    
    def createPopulationData( def detail ) {
        def data = []
        infoFields.each { infoField ->
            def numValues = 1;
            if( infoField.number == 'A' )
                numValues = 1 + detail.alt.split( "," ).size()
           
            def intValue = null
            def floatValue = null
            def textValue = null
            
            (1..numValues).each { idx ->
                switch( infoField.type ) {
                    case 'String':
                        textValue = infoField.name + "-" + detail.pos + "-" + idx
                        break;
                    case 'Integer':
                        intValue = detail.pos * idx
                        break
                }

                data << new DeVariantPopulationData(
                    dataset: dataset,
                    infoField: infoField,
                    subjectDetail: detail,

                    name: infoField.name,
                    chr: detail.chr,
                    pos: detail.pos,
                    index: idx - 1,
                    
                    intValue: intValue,
                    floatValue: floatValue,
                    textValue: textValue,
                )
            }
        }
        
        data
    }


    void saveAll() {
        assertThat platform.save(), is(notNullValue(DeGplInfo))
        assertThat otherPlatform.save(), is(notNullValue(DeGplInfo))
        save([dataset])
        save patients
        save assays
        save detailsData
        save summariesData
        save indexData
        save infoFields
        save infoFieldContents
    }
}
