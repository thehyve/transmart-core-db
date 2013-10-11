package org.transmartproject.db.dataquery

import org.hibernate.Query
import org.hibernate.Session
import org.hibernate.StatelessSession
import org.hibernate.impl.AbstractSessionImpl
import org.transmartproject.core.dataquery.DataQueryResource
import org.transmartproject.core.dataquery.acgh.ChromosomalSegment
import org.transmartproject.core.dataquery.acgh.RegionResult
import org.transmartproject.core.dataquery.assay.Assay
import org.transmartproject.core.dataquery.constraints.ACGHRegionQuery
import org.transmartproject.core.dataquery.constraints.HighDimensionalQuery
import org.transmartproject.core.dataquery.vcf.VcfValues
import org.transmartproject.core.dataquery.vcf.VcfValuesImpl
import org.transmartproject.db.highdim.DeVariantSubjectDetail
import org.transmartproject.db.highdim.DeVariantSubjectSummary
import org.transmartproject.db.querytool.QtQueryResultInstance

import static org.hibernate.ScrollMode.FORWARD_ONLY

class DataQueryResourceService implements DataQueryResource {

    def sessionFactory
    def patientSetQueryBuilderService

    @Override
    RegionResult runACGHRegionQuery(ACGHRegionQuery spec, session) {
        if (!(session == null || session instanceof Session || session
        instanceof StatelessSession)) {
            throw new IllegalArgumentException('Expected session to be null ' +
                    'or an instance of type org.hibernate.Session or ' +
                    'org.hibernate.StatelessSession')
        }

        validateQuery(spec)

        session = session ?: sessionFactory.currentSession
        List<Assay> assays = queryAssays(spec, session)
        if (log.isDebugEnabled()) {
            log.debug("Found ${assays.size()} assays: " +
                    assays.collect {
                        "{id: $it.id, subjectId: $it.subjectId}"
                    }.join(", "))
        }

        getRegionResultForAssays(spec, assays, session)
    }

    @Override
    List<ChromosomalSegment> getChromosomalSegments(ACGHRegionQuery spec) {
        def session = sessionFactory.currentSession
        List<Assay> assays = queryAssays(spec, session)
        def platformIds = assays.collect { it.platform.id } as Set
        def rows = createQuery(session, '''
            select region.chromosome, min(region.start), max(region.end) from DeChromosomalRegion region
            where region.platform.id in (:platformIds)
            group by region.chromosome
        ''', ['platformIds': platformIds]).list()
        rows.collect{
            new ChromosomalSegment(chromosome: it[0], start: it[1], end: it[2])
        }
    }

    protected List<Assay> queryAssays(HighDimensionalQuery spec, AbstractSessionImpl session) {
        def whereClauses, params

        /* first obtain list of assays */
        whereClauses = []
        params = [:]
        populateWhereClauses(spec, whereClauses, params, session)

        def assayHQL = 'from DeSubjectSampleMapping assay\n'
        assayHQL <<= 'where ' + whereClauses.join("\nand ") + "\n"
        assayHQL <<= 'order by assay\n'

        createQuery(session, assayHQL, params).list()
    }

    protected RegionResult getRegionResultForAssays(final ACGHRegionQuery spec, final List<Assay> assays, final AbstractSessionImpl session) {
        /* Then obtain the meat.
         *
         * Doing 'select acgh from ... inner join fetch acgh.region' should
         * be enough (see HHH-3528) so that acgh.region is loaded by the
         * query and such data is used when retrieving DeSubjectAcghData
         * .region. Unfortunately, I could not get it to work on Stateless
         * sessions, even though the generated SQL query does retrieve the
         * region data. I get a "proxies cannot be fetched by a stateless
         * session" (when using stateless sessions) when grails tries to
         * unwrap the hibernate proxy after trying to retrieve .region.
         * Therefore, RegionResultImpl does not use the .region property and
         * instead uses the region object returned alongside the
         * DeSubjectAcghData as result rows.
         *
         * By the way, it could be more efficient (some testing would be
         * necessary) to retrieve only the region id here and issue another
         * query before just to get the regions. This would minimize the
         * amount of data that the postgres server has to send us.
         */

        def params = ['assayIds': assays.collect {Assay assay -> assay.id}]
        def regionsWhereClauses = []

        if (spec.segments) {
            spec.segments.eachWithIndex {ChromosomalSegment segment, int indx ->
                def subClauses = []
                if(segment.chromosome) {
                    params["chromosome$indx"] = segment.chromosome
                    subClauses = ["region.chromosome like :chromosome$indx"]
                }
                if(segment.start && segment.end) {
                    params["start$indx"] = segment.start
                    params["end$indx"] = segment.end
                    subClauses << "(region.start between :start$indx and :end$indx" +
                            " or region.end between :start$indx and :end$indx" +
                            " or (region.start < :start$indx and region.end > :end$indx))"
                }
                regionsWhereClauses << "(${subClauses.join(' and ')})"
            }
        }

        def mainHQL = """
            select acgh, acgh.region
            from DeSubjectAcghData as acgh
            inner join acgh.assay assay
            inner join acgh.region region
            where assay.id in (:assayIds) ${regionsWhereClauses ? 'and (' + regionsWhereClauses.join('\nor ') + ')' : ''}
            order by acgh.region.id, assay
         """

        def mainQuery = createQuery(session, mainHQL, params).scroll(FORWARD_ONLY)

        new RegionResultImpl(assays, mainQuery)
    }

    private void validateQuery(ACGHRegionQuery q) {
        def checkEmptiness = {it, name ->
            if (!it) {
                throw new IllegalArgumentException("$name not specified/empty")
            }
        }
        checkEmptiness(q.common, "query.common")
        checkEmptiness(q.common.patientQueryResult, "query.common.patientQueryResult")
    }

    private Query createQuery(session, hql, params) {
        log.debug("Creating HQL query: $hql")
        log.debug("Parameters to bind: $params")

        Query assayQuery = session.createQuery(hql.toString())

        assayQuery.readOnly = true
        assayQuery.cacheable = false

        params.each {key, val ->
            if (val instanceof Collection) {
                assayQuery.setParameterList(key, val)
            } else {
                assayQuery.setParameter(key, val)
            }
        }

        assayQuery
    }

    private void populateWhereClauses(HighDimensionalQuery q,
                                      List whereClauses,
                                      Map params,
                                      AbstractSessionImpl session) {

        /* assays correspond to patients in the result set */
        /* do not use assay.patient in ( select pset.patient ... ); hibernate
         * will unnecessarily do a join with patient_dimension */
        whereClauses <<
                '''assay.patient.id in (
                    select pset.patient.id
                    from QtPatientSetCollection pset
                    where pset.resultInstance = :queryResult
                )'''
        params['queryResult'] = q.common.patientQueryResult

        if(q.term) {
            def termSql = patientSetQueryBuilderService.getQuerySql(q.term)
            List concepts = session.createSQLQuery(termSql).list()
            assert concepts, "No concept found for term (${q.term})"
            assert concepts.size() == 1, "There are several concepts ($concepts) for term (${q.term})."
            whereClauses << 'assay.conceptCode = :conceptCode'
            params['conceptCode'] = concepts[0]
        }

        /* We treat empty lists the same as nulls here!
         * This means specifying an empty list for platforms will not return an
         * empty result set -- there simply won't be any filtering by platform
         * Idem for the rest
         */

        if (q.common.studies) {
            /* assay in the desired studies */
            whereClauses << 'assay.trialName in (:studies)'
            params['studies'] = q.common.studies
        }

        if (q.common.platforms) {
            whereClauses << 'assay.platform in (:platforms)'
            params['platforms'] = q.common.platforms
        }

        if (q.common.sampleCodes) {
            whereClauses << 'assay.sampleTypeCd in (:sampleCodes)'
            params['sampleCodes'] = q.common.sampleCodes
        }
        if (q.common.tissueCodes) {
            whereClauses << 'assay.tissueTypeCd in (:tissueCodes)'
            params['tissueCodes'] = q.common.tissueCodes
        }

        if (q.common.timepointCodes) {
            whereClauses << 'assay.timepointCd in (:timepointCodes)'
            params['timepointCodes'] = q.common.timepointCodes
        }
    }

    @Override
    List<VcfValues> getCohortMaf(HighDimensionalQuery spec) {
        List<Assay> assays = queryAssays(spec, sessionFactory.currentSession)
        def summaries = DeVariantSubjectSummary.createCriteria().list {
            or {
                spec.segments.each {
                    eq('chromosome', it.chromosome)
                    between('position', it.start, it.end)
                }
            }
            inList('assay', assays)
            and {
                order('chromosome')
                order('position')
            }
        }
        if(!summaries) return []

        def collector = [:]
        def chrPos = null
        List<VcfValues> results = []
        summaries.each {
            if (chrPos && (chrPos.chromosome != it.chromosome || chrPos.position != it.position)) {
                def vcfValue = calculateVcfValues(chrPos, collector[chrPos])
                if (vcfValue) results << vcfValue
            }
            chrPos = [chromosome: it.chromosome, position: it.position, rsId: it.rsId]
            if(!collector[chrPos]) collector[chrPos] = [:]
            collector[chrPos][it.allele1] = (collector[chrPos][it.allele1] ?: 0) + 1
            collector[chrPos][it.allele2] = (collector[chrPos][it.allele2] ?: 0) + 1
        }
        def vcfValue = calculateVcfValues(chrPos, collector[chrPos])
        if (vcfValue) results << vcfValue

        results
    }

    //private final int REF_ALLELE_INDX = 0
    private VcfValuesImpl calculateVcfValues(infromationMap, alleleDistributionMap) {
        double total = alleleDistributionMap.values().sum()
        def maxAltAlleleEntry = alleleDistributionMap.max { it.key == 0 ? 0 : it.value }
        if(maxAltAlleleEntry.key == 0) return null
        new VcfValuesImpl(
                chromosome: infromationMap.chromosome,
                position: infromationMap.position,
                rsId: infromationMap.rsId,
                mafAllele: "${maxAltAlleleEntry.key}",
                maf: maxAltAlleleEntry.value / total,
                //TODO Where to get these fields
                qualityOfDepth: 0D,
                ref: 'TODO',
                alt: 'TOOD',
                additionalInfo: [:]
        )
    }

    @Override
    List<VcfValues> getSummaryMaf(HighDimensionalQuery spec) {
        long resultInstanceId = spec.common.patientQueryResult.id
        List sysCdList = QtQueryResultInstance.executeQuery(
                '''select p.sourcesystemCd
                   from QtQueryResultInstance ri
                   inner join ri.patientSet ps
                   inner join ps.patient p
                   where ri.id = :id
                ''',
                [id: resultInstanceId],
                [max: 1])

       assert sysCdList, "No patients found for resultInstanceId = $resultInstanceId"
       def studyMatcher = sysCdList[0] =~ /^([^:]+):[^:]+$/
       assert studyMatcher, "Can't retrieve study id from ${sysCdList[0]}"

       def studyId = studyMatcher[0][1]

        log.debug("studyId = $studyId for resultInstanceId = $resultInstanceId")

        def results = DeVariantSubjectDetail.createCriteria().list {
            or {
                spec.segments.each {
                    eq('chromosome', it.chromosome)
                    between('position', it.start, it.end)
                }
            }
            //TODO Study id should be stored in datasourceId
            eq('dataset.id', studyId)
            /*
            dataset {
                eq('datasourceId', studyId)
            }
            */
        }
        results
    }
}


