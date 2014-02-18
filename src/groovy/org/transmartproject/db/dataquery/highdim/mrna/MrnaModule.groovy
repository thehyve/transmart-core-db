package org.transmartproject.db.dataquery.highdim.mrna

import com.google.common.collect.ImmutableSet
import grails.orm.HibernateCriteriaBuilder
import org.hibernate.ScrollableResults
import org.hibernate.engine.SessionImplementor
import org.hibernate.transform.Transformers
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.AssayColumn
import org.transmartproject.core.dataquery.highdim.projections.Projection
import org.transmartproject.db.dataquery.highdim.AbstractHighDimensionDataTypeModule
import org.transmartproject.db.dataquery.highdim.DefaultHighDimensionTabularResult
import org.transmartproject.db.dataquery.highdim.RepeatedEntriesCollectingTabularResult
import org.transmartproject.db.dataquery.highdim.correlations.CorrelationTypesRegistry
import org.transmartproject.db.dataquery.highdim.correlations.SearchKeywordDataConstraintFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.DataRetrievalParameterFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.AllDataProjectionFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.SimpleRealProjectionsFactory

import static org.hibernate.sql.JoinFragment.INNER_JOIN

class MrnaModule extends AbstractHighDimensionDataTypeModule {

    final String name = 'mrna'

    final String description = "Messenger RNA data (Microarray)"

    final List<String> platformMarkerTypes = ['Gene Expression']

    final Set<String> dataProperties = ImmutableSet.of('trialName', 'rawIntensity', 'logIntensity', 'zscore')

    final Set<String> rowProperties = ImmutableSet.of('probe', 'geneId', 'geneSymbol')

    @Autowired
    DataRetrievalParameterFactory standardAssayConstraintFactory

    @Autowired
    DataRetrievalParameterFactory standardDataConstraintFactory

    @Autowired
    CorrelationTypesRegistry correlationTypesRegistry

    @Override
    HibernateCriteriaBuilder prepareDataQuery(Projection projection,
                                              SessionImplementor session) {
        HibernateCriteriaBuilder criteriaBuilder =
            createCriteriaBuilder(DeSubjectMicroarrayDataCoreDb, 'mrnadata', session)

        criteriaBuilder.with {
            createAlias 'jProbe', 'p', INNER_JOIN

            projections {
                property 'assay',        'assay'

                property 'p.id',         'probeId'
                property 'p.probeId',    'probeName'
                property 'p.geneSymbol', 'geneSymbol'
                property 'p.geneId',     'geneId'
                property 'p.organism',   'organism'
            }

            order 'p.id',         'asc'
            order 'p.geneSymbol', 'asc' // see below
            order 'assay.id',     'asc' // important! See assumption below

            // because we're using this transformer, every column has to have an alias
            instance.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP)
        }

        criteriaBuilder
    }

    @Override
    TabularResult transformResults(ScrollableResults results,
                                     List<AssayColumn> assays,
                                     Projection projection) {
        /* assumption here is the assays in the passed in list are in the same
         * order as the assays in the result set */
        Map assayIndexMap = createAssayIndexMap assays

        def preliminaryResult = new DefaultHighDimensionTabularResult(
                rowsDimensionLabel:    'Probes',
                columnsDimensionLabel: 'Sample codes',
                indicesList:           assays,
                results:               results,
                allowMissingAssays:    true,
                assayIdFromRow:        { it[0].assay.id },
                inSameGroup:           { a, b -> a.probeId == b.probeId && a.geneSymbol == b.geneSymbol },
                finalizeGroup:         { List list -> /* list of arrays with one element: a map */
                    /* we may have nulls if allowMissingAssays is true,
                     * but we're guaranteed to have at least one non-null */
                    def firstNonNullCell = list.find()
                    new ProbeRow(
                            probe:         firstNonNullCell[0].probeName,
                            geneSymbol:    firstNonNullCell[0].geneSymbol,
                            geneId:        firstNonNullCell[0].geneId,
                            assayIndexMap: assayIndexMap,
                            data:          list.collect { projection.doWithResult it?.getAt(0) }
                    )
                }
        )

        /* In some implementations, probeset_id is actually not a primary key on
         * the annotations table and several rows will be returned for the same
         * probeset_id, just with different genes.
         * Hence the order by clause and the definition of inSameGroup above */
        new RepeatedEntriesCollectingTabularResult(
                tabularResult: preliminaryResult,
                collectBy: { it.probe },
                resultItem: {collectedList ->
                    if (collectedList) {
                        new ProbeRow(
                                probe:         collectedList[0].probe,
                                geneSymbol:    collectedList*.geneSymbol.join('/'),
                                geneId:        collectedList*.geneId.join('/'),
                                assayIndexMap: collectedList[0].assayIndexMap,
                                data:          collectedList[0].data
                        )
            }
            }
        )
    }

    @Override
    protected List<DataRetrievalParameterFactory> createAssayConstraintFactories() {
        [ standardAssayConstraintFactory ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createDataConstraintFactories() {
        [ standardDataConstraintFactory,
                new SearchKeywordDataConstraintFactory(correlationTypesRegistry,
                        'GENE', 'jProbe', 'geneId') ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createProjectionFactories() {
        [ new SimpleRealProjectionsFactory(
                (Projection.LOG_INTENSITY_PROJECTION): 'logIntensity',
                (Projection.DEFAULT_REAL_PROJECTION): 'rawIntensity',
                (Projection.ZSCORE_PROJECTION):       'zscore'),
        new AllDataProjectionFactory(dataProperties, rowProperties)]
    }
}
