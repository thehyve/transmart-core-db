package org.transmartproject.db.dataquery.highdim.assayconstraints

import org.junit.Before
import org.junit.Test
import org.transmartproject.core.dataquery.highdim.AssayColumn
import org.transmartproject.core.querytool.QueryResult
import org.transmartproject.db.dataquery.highdim.AssayQuery
import org.transmartproject.db.dataquery.highdim.AssayTestData
import org.transmartproject.db.querytool.QtQueryMaster
import org.transmartproject.db.querytool.QueryResultData

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*

class DefaultPatientSetConstraintTests {

    /* patient set with only the first patient (AssayTestData.patients[0]) */
    QueryResult firstPatientResult

    @Before
    void setup() {
        AssayTestData.saveAll()

        QtQueryMaster master = QueryResultData.createQueryResult([
                AssayTestData.patients[0]
        ])

        master.save()
        firstPatientResult = master.
                queryInstances.iterator().next(). // QtQueryInstance
                queryResults.iterator().next()
    }

    @Test
    void basicTest() {
        AssayQuery assayQuery = new AssayQuery([
                new DefaultPatientSetConstraint(
                        queryResult: firstPatientResult
                )
        ])

        List<AssayColumn> assays = assayQuery.retrieveAssays()

        assertThat assays, allOf(
                everyItem(
                        hasProperty('patient', equalTo(AssayTestData.patients[0]))
                ),
                containsInAnyOrder(
                        /* see test data, -X01 ids are assays for the 1st patient */
                        hasProperty('id', equalTo(-201L)),
                        hasProperty('id', equalTo(-301L)),
                        hasProperty('id', equalTo(-401L)),
                )
        )
    }
}