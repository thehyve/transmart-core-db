package org.transmartproject.db.querytool

import grails.test.mixin.TestFor
import grails.test.mixin.TestMixin
import grails.test.mixin.support.GrailsUnitTestMixin
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.rules.ExpectedException
import org.transmartproject.core.exceptions.InvalidRequestException
import org.transmartproject.core.querytool.ConstraintByValue
import org.transmartproject.core.querytool.ConstraintByVcf
import org.transmartproject.core.querytool.Item
import org.transmartproject.core.querytool.Panel
import org.transmartproject.core.querytool.QueryDefinition
import org.transmartproject.db.concept.ConceptKey
import org.transmartproject.db.ontology.ConceptsResourceService
import org.transmartproject.db.ontology.I2b2
import org.transmartproject.db.support.DatabasePortabilityService

import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*
import static org.transmartproject.core.querytool.ConstraintByValue.Operator.*
import static org.transmartproject.core.querytool.ConstraintByValue.ValueType.FLAG
import static org.transmartproject.core.querytool.ConstraintByValue.ValueType.NUMBER
import static org.transmartproject.core.querytool.ConstraintByVcf.Value.WILDTYPE
import static org.transmartproject.core.querytool.ConstraintByVcf.Type.STATUS
import static org.transmartproject.db.support.DatabasePortabilityService.DatabaseType.POSTGRESQL

@TestMixin(GrailsUnitTestMixin)
@TestFor(PatientSetQueryBuilderService)
class PatientSetQueryBuilderServiceTests {

    QtQueryResultInstance resultInstance

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    void setUp() {
        // doWithDynamicMethods is not run for unit tests...
        // Maybe making this an integration test would be preferable
        String.metaClass.asLikeLiteral = { replaceAll(/[\\%_]/, '\\\\$0') }

        def conceptsResourceServiceStub = [
                getByKey: { String key ->
                    new I2b2(
                            factTableColumn    : 'concept_cd',
                            dimensionTableName : 'concept_dimension',
                            columnName         : 'concept_path',
                            columnDataType     : 'T',
                            operator           : 'LIKE',
                            dimensionCode      : new ConceptKey(key).conceptFullName.toString(),
                    )
                }
        ] as ConceptsResourceService
        service.conceptsResourceService = conceptsResourceServiceStub

        def databasePortabilityStub = [
                getDatabaseType: { -> POSTGRESQL }
        ] as DatabasePortabilityService
        service.databasePortabilityService = databasePortabilityStub

        resultInstance = new QtQueryResultInstance()
        resultInstance.id = 42
    }

    @Test
    void basicTest() {
        def conceptKey = '\\\\code\\full\\name\\'
        def definition = new QueryDefinition([
                new Panel(
                        invert: false,
                        items:  [
                                new Item(
                                        conceptKey: conceptKey,
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])
        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, allOf(
                startsWith('INSERT INTO qt_patient_set_collection'),
                containsString('SELECT patient_num as patient_identifier FROM ' +
                        'observation_fact'),
                containsString('concept_cd IN (SELECT concept_cd FROM ' +
                        'concept_dimension WHERE concept_path LIKE ' +
                        '\'\\\\full\\\\name\\\\%\')')
                );
    }

    @Test
    void testMultiplePanelsAndItems() {
        def definition = new QueryDefinition([
                new Panel(
                        invert: false,
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name1\\',
                                        type: "CLINICAL"
                                ),
                                new Item(
                                        conceptKey: '\\\\code\\full\\name2\\',
                                        type: "CLINICAL"
                                ),
                        ]
                ),
                new Panel(
                        items: [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name3\\',
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])
        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, allOf(
                startsWith('INSERT INTO qt_patient_set_collection'),
                containsString('UNION (SELECT patient_num as patient_identifier'),
                containsString('WHERE concept_cd IN (SELECT concept_cd FROM ' +
                        'concept_dimension WHERE concept_path LIKE ' +
                        '\'\\\\full\\\\name2\\\\%\')'),
                containsString('INTERSECT (SELECT patient_num as patient_identifier FROM ' +
                        'observation_fact WHERE concept_cd IN (SELECT ' +
                        'concept_cd FROM concept_dimension WHERE concept_path ' +
                        'LIKE \'\\\\full\\\\name3\\\\%\''),
        );
    }

    @Test
    void testPanelInversion() {
        def definition = new QueryDefinition([
                new Panel(
                        invert: true,
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name\\',
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])
        def sql = service.buildPatientSetQuery(resultInstance, definition)
        assertThat sql, containsString('SELECT patient_num as patient_identifier FROM ' +
                'patient_dimension EXCEPT (SELECT patient_num as patient_identifier FROM ' +
                'observation_fact WHERE concept_cd IN (SELECT concept_cd ' +
                'FROM concept_dimension WHERE concept_path ' +
                'LIKE \'\\\\full\\\\name\\\\%\') ' +
                'AND concept_cd != \'SECURITY\' GROUP BY patient_identifier) ORDER BY 1')
    }

    @Test
    void testPanelInversionPlacement() {
        def definition = new QueryDefinition([
                new Panel(
                        invert: true,
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\b\\',
                                        type: "CLINICAL"
                                )
                        ]
                ),
                new Panel(
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\a\\',
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])

        def sql = service.buildPatientSetQuery(resultInstance, definition)
        assertThat sql, containsString('EXCEPT (SELECT patient_num as patient_identifier ' +
                'FROM observation_fact WHERE concept_cd IN (SELECT ' +
                'concept_cd FROM concept_dimension WHERE concept_path ' +
                'LIKE \'\\\\b\\\\%\') AND concept_cd != \'SECURITY\' GROUP BY patient_identifier)')
    }

    @Test
    void testNumberSimpleConstraint() {
        def definition = new QueryDefinition([
                new Panel(
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name\\',
                                        constraint: new ConstraintByValue(
                                                operator: LOWER_THAN,
                                                valueType: NUMBER,
                                                constraint: '5.6'
                                        ),
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])

        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, containsString("AND ((valtype_cd = 'N' " +
                "AND nval_num < 5.6 AND tval_char IN ('E', 'LE')) OR (" +
                "valtype_cd = 'N' AND nval_num <= 5.6 AND tval_char = 'L'))")
    }

    @Test
    void testNumberBetweenConstraint() {
        def definition = new QueryDefinition([
                new Panel(
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name\\',
                                        constraint: new ConstraintByValue(
                                                operator: BETWEEN,
                                                valueType: NUMBER,
                                                constraint: '5.6 and 5.8'
                                        ),
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])

        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, containsString("AND ((valtype_cd = 'N' AND " +
                "nval_num BETWEEN 5.6 AND 5.8 AND tval_char = 'E'))")
    }

    @Test
    void testFlagConstraint() {
        def definition = new QueryDefinition([
                new Panel(
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name\\',
                                        constraint: new ConstraintByValue(
                                                operator: EQUAL_TO,
                                                valueType: FLAG,
                                                constraint: 'N'
                                        ),
                                        type: "CLINICAL"
                                )
                        ]
                )
        ])

        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, containsString("AND (valueflag_cd = 'N')")
    }

    @Test
    void basicVcfTest() {
        def conceptKey = '\\\\code\\full\\name\\'
        def definition = new QueryDefinition([
                new Panel(
                        invert: false,
                        items:  [
                                new Item(
                                        conceptKey: conceptKey,
                                        type: "VCF"
                                )
                        ]
                )
        ])
        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, allOf(
                startsWith('INSERT INTO qt_patient_set_collection'),
                containsString('SELECT patient_id as patient_identifier ' +
                        'FROM deapp.de_subject_sample_mapping WHERE platform = \'VCF\'')
        );
    }

    @Test
    void testVcfWildtypeConstraint() {
        def definition = new QueryDefinition([
                new Panel(
                        items:  [
                                new Item(
                                        conceptKey: '\\\\code\\full\\name\\',
                                        constraint: new ConstraintByVcf(
                                                location: "1:65529",
                                                type: STATUS,
                                                value: WILDTYPE
                                        ),
                                        type: "VCF"
                                )
                        ]
                )
        ])

        def sql = service.buildPatientSetQuery(resultInstance, definition)

        assertThat sql, containsString("subject_id IN (" +
                "SELECT subject_id " +
                "FROM deapp.de_variant_subject_summary "+
                "WHERE chr = '1' AND pos = 65529 AND allele1 = 0 AND allele2 = 0)")
    }

    // The rest are error tests

    @Test
    void testNoPanel() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('No panels were specified'))

        def definition = new QueryDefinition([])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testEmptyPanel() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('Found panel with no items'))

        def definition = new QueryDefinition([
                new Panel(items: [])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testNullItem() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('Found panel with null value ' +
                'in its item list'))

        def definition = new QueryDefinition([
                new Panel(items: [null])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testNullConceptKey() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(
                equalTo('Found item with null conceptKey'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(conceptKey: null)
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testConstraintWithoutOperator() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('Found item constraint with ' +
                'null operator'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        valueType: FLAG,
                                        constraint: 'N'
                                )
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testConstraintWithoutValueType() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('Found item constraint with ' +
                'null value type'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        operator: EQUAL_TO,
                                        constraint: 'N'
                                )
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testConstraintWithoutConstraintValue() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('Found item constraint with ' +
                'null constraint value'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        operator: EQUAL_TO,
                                        valueType: FLAG,
                                )
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testBogusConstraintFlagValue() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(containsString('A flag value ' +
                "constraint's operand must be either 'L', 'H' or 'N'"))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        operator: EQUAL_TO,
                                        valueType: FLAG,
                                        constraint: 'FOO'
                                ),
                                type: "CLINICAL"
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testBogusConstraintNumberValue() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(containsString('an invalid number ' +
                'constraint value'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        operator: EQUAL_TO,
                                        valueType: NUMBER,
                                        constraint: 'FOO'
                                ),
                                type: "CLINICAL"
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testBogusBetweenConstraintNumberValue() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(containsString('an invalid number ' +
                'constraint value'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        operator: BETWEEN,
                                        valueType: NUMBER,
                                        constraint: '5.6'
                                ),
                                type: "CLINICAL"
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

    @Test
    void testInvalidOperatorForFlagValueContraint() {
        expectedException.expect(isA(InvalidRequestException))
        expectedException.expectMessage(equalTo('Found item flag constraint ' +
                'with an operator different from EQUAL_TO'))

        def definition = new QueryDefinition([
                new Panel(items: [
                        new Item(
                                conceptKey: '\\\\code\\full\\name\\',
                                constraint: new ConstraintByValue(
                                        operator: LOWER_OR_EQUAL_TO,
                                        valueType: FLAG,
                                        constraint: '5.6'
                                )
                        )
                ])
        ])

        service.buildPatientSetQuery(resultInstance, definition)
    }

}
