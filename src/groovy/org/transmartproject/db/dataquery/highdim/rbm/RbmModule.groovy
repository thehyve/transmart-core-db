package org.transmartproject.db.dataquery.highdim.rbm

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
import org.transmartproject.db.dataquery.highdim.parameterproducers.AllDataProjectionFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.DataRetrievalParameterFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.SimpleRealProjectionsFactory

import static org.hibernate.sql.JoinFragment.INNER_JOIN

class RbmModule extends AbstractHighDimensionDataTypeModule {

    final String name = 'rbm'

    final String description = "RBM data"

    final List<String> platformMarkerTypes = ['RBM']

    private final Set<String> dataProperties = ImmutableSet.of('value', 'zscore')

    private final Set<String> rowProperties = ImmutableSet.of('antigenName', 'uniprotId')

    @Autowired
    DataRetrievalParameterFactory standardAssayConstraintFactory

    @Autowired
    DataRetrievalParameterFactory standardDataConstraintFactory

    @Autowired
    CorrelationTypesRegistry correlationTypesRegistry

    @Override
    protected List<DataRetrievalParameterFactory> createAssayConstraintFactories() {
        [ standardAssayConstraintFactory ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createDataConstraintFactories() {
        [
                standardDataConstraintFactory,
                new SearchKeywordDataConstraintFactory(correlationTypesRegistry, 'PROTEIN', 'annotations', 'uniprotId'),
        ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createProjectionFactories() {
        [ new SimpleRealProjectionsFactory(
                (Projection.DEFAULT_REAL_PROJECTION): 'value',
                (Projection.ZSCORE_PROJECTION):       'zscore'),
        new AllDataProjectionFactory(dataProperties, rowProperties)]
    }

    @Override
    HibernateCriteriaBuilder prepareDataQuery(Projection projection, SessionImplementor session) {
        HibernateCriteriaBuilder criteriaBuilder =
            createCriteriaBuilder(DeSubjectRbmData, 'rbmdata', session)

        criteriaBuilder.with {
            createAlias 'annotations', 'p', INNER_JOIN

            projections {
                property 'assay', 'assay'
                property 'p.id', 'annotationId'
                property 'p.antigenName', 'antigenName'
                property 'p.uniprotId', 'uniprotId'
            }

            order 'p.id', 'asc'
            order 'p.uniprotId', 'asc'
            order 'assay.id', 'asc'
            instance.setResultTransformer(Transformers.ALIAS_TO_ENTITY_MAP)
        }
        criteriaBuilder
    }

    @Override
    TabularResult transformResults(ScrollableResults results, List<AssayColumn> assays, Projection projection) {
        Map assayIndexes = createAssayIndexMap assays

        def preliminaryResult = new DefaultHighDimensionTabularResult(
                rowsDimensionLabel: 'Antigenes',
                columnsDimensionLabel: 'Sample codes',
                indicesList: assays,
                results: results,
                //TODO Remove this. On real data missing assays are signaling about problems
                allowMissingAssays: true,
                assayIdFromRow: { it[0].assay.id },
                inSameGroup: {a, b -> a.annotationId == b.annotationId && a.uniprotId == b.uniprotId },
                finalizeGroup: {List list ->
                    def firstNonNullCell = list.find()
                    new RbmRow(
                            annotationId: firstNonNullCell[0].annotationId,
                            antigenName: firstNonNullCell[0].antigenName,
                            uniprotId: firstNonNullCell[0].uniprotId,
                            assayIndexMap: assayIndexes,
                            data: list.collect { projection.doWithResult it?.getAt(0) }
                    )
                }
        )

        new RepeatedEntriesCollectingTabularResult(
                tabularResult: preliminaryResult,
                collectBy: { it.antigenName },
                resultItem: {collectedList ->
                    if (collectedList) {
                        new RbmRow(
                                annotationId: collectedList[0].annotationId,
                                antigenName: collectedList[0].antigenName,
                                uniprotId: collectedList*.uniprotId.join('/'),
                                assayIndexMap: collectedList[0].assayIndexMap,
                                data: collectedList[0].data
                        )
    }
}
        )
    }

}
