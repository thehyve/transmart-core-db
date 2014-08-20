/*
 * Copyright © 2013-2014 The Hyve B.V.
 *
 * This file is part of transmart-core-db.
 *
 * Transmart-core-db is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * transmart-core-db.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.transmartproject.db.dataquery.highdim.rnaseq

import grails.orm.HibernateCriteriaBuilder
import org.hibernate.ScrollableResults
import org.hibernate.engine.SessionImplementor
import org.hibernate.transform.Transformers
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.AssayColumn
import org.transmartproject.core.dataquery.highdim.HighDimensionDataTypeResource
import org.transmartproject.core.dataquery.highdim.projections.Projection
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.core.exceptions.UnexpectedResultException
import org.transmartproject.db.dataquery.highdim.AbstractHighDimensionDataTypeModule
import org.transmartproject.db.dataquery.highdim.DefaultHighDimensionTabularResult
import org.transmartproject.db.dataquery.highdim.acgh.AcghDataTypeResource
import org.transmartproject.db.dataquery.highdim.chromoregion.ChromosomeSegmentConstraintFactory
import org.transmartproject.db.dataquery.highdim.correlations.CorrelationTypesRegistry
import org.transmartproject.db.dataquery.highdim.correlations.SearchKeywordDataConstraintFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.AllDataProjectionFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.DataRetrievalParameterFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.MapBasedParameterFactory
import org.transmartproject.db.dataquery.highdim.parameterproducers.SimpleRealProjectionsFactory

import static org.hibernate.sql.JoinFragment.INNER_JOIN

/**
 * Module for RNA-seq, as implemented for Postgres by TraIT.
 */
class RnaSeqModule extends AbstractHighDimensionDataTypeModule {

    static final String RNASEQ_VALUES_PROJECTION = 'rnaseq_values'

    final List<String> platformMarkerTypes = ['Chromosomal']

    final String name = 'rnaseq'

    final String description = "Messenger RNA data (Sequencing)"

    final Map<String, Class> dataProperties = typesMap(DeSubjectRnaseqData,
            ['readCount', 'normalizedReadCount', 'logNormalizedReadCount', 'zscore'])

    final Map<String, Class> rowProperties = typesMap(RnaSeqDataRow,
        ['regionId', 'name', 'cytoband', 'chromosome', 'start', 'end', 'numberOfProbes', 'geneSymbol', 'geneId'])

    @Autowired
    DataRetrievalParameterFactory standardAssayConstraintFactory

    @Autowired
    DataRetrievalParameterFactory standardDataConstraintFactory

    @Autowired
    ChromosomeSegmentConstraintFactory chromosomeSegmentConstraintFactory

    @Autowired
    CorrelationTypesRegistry correlationTypesRegistry

    @Override
    HighDimensionDataTypeResource createHighDimensionResource(Map params) {
        /* return instead subclass of HighDimensionDataTypeResourceImpl,
         * because we add a method, retrieveChromosomalSegments() */
        new AcghDataTypeResource(this)
    }

    @Override
    protected List<DataRetrievalParameterFactory> createAssayConstraintFactories() {
        [ standardAssayConstraintFactory ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createDataConstraintFactories() {
        [
                standardDataConstraintFactory,
                chromosomeSegmentConstraintFactory,
                new SearchKeywordDataConstraintFactory(correlationTypesRegistry,
                        'GENE', 'jRegion', 'geneId')
        ]
    }

    @Override
    protected List<DataRetrievalParameterFactory> createProjectionFactories() {
        [
                new MapBasedParameterFactory(
                        (RNASEQ_VALUES_PROJECTION): { Map<String, Object> params ->
                            if (!params.isEmpty()) {
                                throw new InvalidArgumentsException('Expected no parameters here')
                            }
                            new RnaSeqValuesProjection()
                        }
                ),
                new SimpleRealProjectionsFactory(
                        (Projection.LOG_INTENSITY_PROJECTION): 'logNormalizedReadCount',
                        (Projection.DEFAULT_REAL_PROJECTION):  'normalizedReadCount',
                        (Projection.ZSCORE_PROJECTION):        'zscore'
                ),
                new AllDataProjectionFactory(dataProperties, rowProperties)
        ]
    }

    @Override
    HibernateCriteriaBuilder prepareDataQuery(Projection projection, SessionImplementor session) {
        HibernateCriteriaBuilder criteriaBuilder =
            createCriteriaBuilder(DeSubjectRnaseqData, 'rnaseqdata', session)

        criteriaBuilder.with {
            createAlias 'jRegion', 'region', INNER_JOIN

            projections {
                property 'rnaseqdata.assay.id',               'assayId'
                property 'rnaseqdata.readCount',              'readCount'
                property 'rnaseqdata.normalizedReadCount',    'normalizedReadCount'
                property 'rnaseqdata.logNormalizedReadCount', 'logNormalizedReadCount'
                property 'rnaseqdata.zscore',                 'zscore'

                property 'region.id',                         'regionId'
                property 'region.name',                       'name'
                property 'region.cytoband',                   'cytoband'
                property 'region.chromosome',                 'chromosome'
                property 'region.start',                      'start'
                property 'region.end',                        'end'
                property 'region.numberOfProbes',             'numberOfProbes'
                property 'region.geneSymbol',                 'geneSymbol'
                property 'region.geneId',                     'geneId'
            }

            order 'region.id', 'asc'
            order 'assay.id',  'asc' // important

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

        new DefaultHighDimensionTabularResult(
                rowsDimensionLabel:    'Regions',
                columnsDimensionLabel: 'Sample codes',
                indicesList:           assays,
                results:               results,
                allowMissingAssays:    true,
                assayIdFromRow:        { it[0].assayId },
                inSameGroup:           { a, b -> a.regionId == b.regionId },
                finalizeGroup:         { List list ->
                        if (list.size() != assays.size()) {
                            throw new UnexpectedResultException(
                                    "Expected group to be of size ${assays.size()}; got ${list.size()} objects")
                        }
                        def firstNonNullCell = list.find()
                        new RnaSeqDataRow(
                                regionId:       firstNonNullCell[0].regionId,
                                name:           firstNonNullCell[0].name,
                                cytoband:       firstNonNullCell[0].cytoband,
                                chromosome:     firstNonNullCell[0].chromosome,
                                start:          firstNonNullCell[0].start,
                                end:            firstNonNullCell[0].end,
                                numberOfProbes: firstNonNullCell[0].numberOfProbes,
                                geneSymbol:     firstNonNullCell[0].geneSymbol,
                                geneId:         firstNonNullCell[0].geneId,
                                assayIndexMap:  assayIndexMap,
                                data:           list.collect { projection.doWithResult it?.getAt(0) }
                        )
                }
        )
    }
}
