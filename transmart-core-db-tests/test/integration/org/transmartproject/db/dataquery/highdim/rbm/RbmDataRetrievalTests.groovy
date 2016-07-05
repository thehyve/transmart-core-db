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

package org.transmartproject.db.dataquery.highdim.rbm

import com.google.common.collect.Lists
import grails.test.mixin.TestMixin
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.transmartproject.core.dataquery.TabularResult
import org.transmartproject.core.dataquery.highdim.HighDimensionDataTypeResource
import org.transmartproject.core.dataquery.highdim.HighDimensionResource
import org.transmartproject.core.dataquery.highdim.assayconstraints.AssayConstraint
import org.transmartproject.core.dataquery.highdim.dataconstraints.DataConstraint
import org.transmartproject.core.dataquery.highdim.projections.Projection
import org.transmartproject.db.dataquery.highdim.HighDimTestData
import org.transmartproject.db.test.RuleBasedIntegrationTestMixin

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

@TestMixin(RuleBasedIntegrationTestMixin)
class RbmDataRetrievalTests {

    RbmTestData testData = new RbmTestData()

    HighDimensionResource highDimensionResourceService

    HighDimensionDataTypeResource rbmResource

    AssayConstraint trialNameConstraint

    Projection projection

    TabularResult result

    double DELTA = 0.0001

    @Test
    void testRetrievalByTrialNameAssayConstraint() {
        result = rbmResource.retrieveData([trialNameConstraint], [], projection)

        assertThat result, allOf(
                hasProperty('columnsDimensionLabel', equalTo('Sample codes')),
                hasProperty('rowsDimensionLabel', equalTo('Antigenes')),
        )

        def resultList = Lists.newArrayList result

        assertThat resultList, allOf(
                hasSize(3),
                everyItem(
                        hasProperty('data',
                                allOf(
                                        hasSize(2),
                                        everyItem(isA(Double))
                                )
                        )
                ),
                contains(
                        contains(
                                closeTo(testData.data[-1].zscore as Double, DELTA),
                                closeTo(testData.data[-2].zscore as Double, DELTA)),
                        contains(
                                closeTo(testData.data[-3].zscore as Double, DELTA),
                                closeTo(testData.data[-4].zscore as Double, DELTA)),
                        contains(
                                closeTo(testData.data[-5].zscore as Double, DELTA),
                                closeTo(testData.data[-6].zscore as Double, DELTA)),
                ),
                everyItem(
                        hasProperty('assayIndexMap', allOf(
                                isA(Map),
                                hasEntry(
                                        hasProperty('id', equalTo(-402L)), /* key */
                                        equalTo(0), /* value */
                                ),
                                hasEntry(
                                        hasProperty('id', equalTo(-401L)),
                                        equalTo(1),
                                ),
                        ))
                )
        )
    }

    @Test
    void testLogIntensityProjection() {
        def logIntensityProjection = rbmResource.createProjection(
                [:], Projection.LOG_INTENSITY_PROJECTION)

        result = rbmResource.retrieveData(
                [ trialNameConstraint ], [], logIntensityProjection)

        def resultList = Lists.newArrayList(result)

        assertThat(
                resultList.collect { it.data }.flatten(),
                containsInAnyOrder(testData.data.collect { closeTo(it.logIntensity as Double, DELTA) })
        )
    }

    @Test
    void testDefaultRealProjection() {
        result = rbmResource.retrieveData([trialNameConstraint], [],
            rbmResource.createProjection([:], Projection.DEFAULT_REAL_PROJECTION))

        def resultList = Lists.newArrayList result

        assertThat resultList, hasItem(allOf(
                hasProperty('label', is('Antigene1 (A)')),
                contains(
                        closeTo(testData.data[1].value as Double, DELTA),
                        closeTo(testData.data[0].value as Double, DELTA))))
    }

    @Test
    void testNoUnit() {
        testData.data.each { it.unit = null}
        HighDimTestData.save testData.data

        result = rbmResource.retrieveData([trialNameConstraint], [],
                rbmResource.createProjection([:], Projection.DEFAULT_REAL_PROJECTION))

        def resultList = Lists.newArrayList result

        assertThat resultList, allOf(
                hasItem(hasProperty('label', is('Antigene1'))),
                not(hasItem(hasProperty('label', is('Antigene1 (A)')))))
    }

    @Test
    void testRetrievalByUniProtNamesDataConstraint() {
        def proteinDataConstraint = rbmResource.createDataConstraint(
                [names: ['Adiponectin']],
                DataConstraint.PROTEINS_CONSTRAINT
        )

        result = rbmResource.retrieveData([trialNameConstraint], [proteinDataConstraint], projection)

        def resultList = Lists.newArrayList result

        assertThat resultList, allOf(
                everyItem(
                        allOf(
                                contains(
                                        closeTo(testData.data[-5].zscore as Double, DELTA),
                                        closeTo(testData.data[-6].zscore as Double, DELTA)))),
                contains(
                        allOf(
                                hasProperty('bioMarker', equalTo('PVR_HUMAN1')),
                                hasProperty('label', equalTo('Antigene1 (A)')))))
    }

    @Test
    void testRetrievalByUniProtIdsDataConstraint() {
        def proteinDataConstraint = rbmResource.createDataConstraint(
                [ids: ['Q15848']],
                DataConstraint.PROTEINS_CONSTRAINT
        )

        result = rbmResource.retrieveData([trialNameConstraint], [proteinDataConstraint], projection)

        def resultList = Lists.newArrayList result

        assertThat resultList, allOf(
                hasSize(1),
                everyItem(
                        hasProperty('data', allOf(
                                hasSize(2),
                                contains(
                                        closeTo(testData.data[-5].zscore as Double, DELTA),
                                        closeTo(testData.data[-6].zscore as Double, DELTA),
                                ))
                        )
                ),
                contains(hasProperty('label', equalTo('Antigene1 (A)')))
        )
    }

    @Test
    void testRetrievalByGeneNamesDataConstraint() {
        def geneDataConstraint = rbmResource.createDataConstraint(
                [names: ['SLC14A2']],
                DataConstraint.GENES_CONSTRAINT
        )

        result = rbmResource.retrieveData([trialNameConstraint], [geneDataConstraint], projection)

        def resultList = Lists.newArrayList result

        assertThat resultList, contains(
                allOf(
                        hasProperty('label', equalTo('Antigene2 (B)')),
                        contains(
                                closeTo(testData.data[-3].zscore as Double, DELTA),
                                closeTo(testData.data[-4].zscore as Double, DELTA))))
    }

    @Test
    void testRetrievalByGeneSkIdsDataConstraint() {
        def skId = testData.searchKeywords.find({ it.keyword == 'SLC14A2' }).id
        def geneDataConstraint = rbmResource.createDataConstraint(
                [keyword_ids: [skId]],
                DataConstraint.SEARCH_KEYWORD_IDS_CONSTRAINT
        )

        result = rbmResource.retrieveData([trialNameConstraint], [geneDataConstraint], projection)

        def resultList = Lists.newArrayList result

        assertThat resultList, allOf(
                everyItem(
                        hasProperty('data',
                                contains(
                                        closeTo(testData.data[-3].zscore as Double, DELTA),
                                        closeTo(testData.data[-4].zscore as Double, DELTA)))),
                contains(hasProperty('label', equalTo('Antigene2 (B)'))))
    }

    @Test
    void testRetrieDataRowThatMapsToMultipleProteins() {

        def proteinDataConstraint = rbmResource.createDataConstraint(
                [ids: ['Q15847', 'Q15850']],
                DataConstraint.PROTEINS_CONSTRAINT
        )

        result = rbmResource.retrieveData([trialNameConstraint], [proteinDataConstraint], projection)

        def resultList = Lists.newArrayList result

        assertThat resultList, allOf(
                hasSize(1),
                hasItem(
                        allOf(
                                hasProperty('bioMarker', equalTo('PVR_HUMAN4/PVR_HUMAN3')),
                                hasProperty('label', equalTo('Antigene3 (C)')),
                                contains(
                                        closeTo(testData.data[-1].zscore as Double, DELTA),
                                        closeTo(testData.data[-2].zscore as Double, DELTA))
                        )
                )
        )
    }

    @Test
    void testConstraintAvailability() {
        assertThat rbmResource.supportedAssayConstraints, containsInAnyOrder(
                AssayConstraint.ONTOLOGY_TERM_CONSTRAINT,
                AssayConstraint.PATIENT_SET_CONSTRAINT,
                AssayConstraint.TRIAL_NAME_CONSTRAINT,
                AssayConstraint.ASSAY_ID_LIST_CONSTRAINT,
                AssayConstraint.PATIENT_ID_LIST_CONSTRAINT,
                AssayConstraint.DISJUNCTION_CONSTRAINT)
        assertThat rbmResource.supportedDataConstraints, hasItems(
                DataConstraint.SEARCH_KEYWORD_IDS_CONSTRAINT,
                DataConstraint.DISJUNCTION_CONSTRAINT,
                DataConstraint.GENES_CONSTRAINT,
                DataConstraint.PROTEINS_CONSTRAINT,
                /* also others that may be added by registering new associations */
        )
        assertThat rbmResource.supportedProjections, containsInAnyOrder(
                Projection.DEFAULT_REAL_PROJECTION,
                Projection.LOG_INTENSITY_PROJECTION,
                Projection.ZSCORE_PROJECTION,
                Projection.ALL_DATA_PROJECTION)
    }

    @Before
    void setUp() {
        testData.saveAll()
        rbmResource = highDimensionResourceService.getSubResourceForType 'rbm'

        trialNameConstraint = rbmResource.createAssayConstraint(
                AssayConstraint.TRIAL_NAME_CONSTRAINT,
                name: RbmTestData.TRIAL_NAME,
        )
        projection = rbmResource.createProjection [:], Projection.ZSCORE_PROJECTION
    }

    @After
    void tearDown() {
        result?.close()
    }
}
