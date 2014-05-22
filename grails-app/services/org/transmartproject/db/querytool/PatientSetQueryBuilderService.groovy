package org.transmartproject.db.querytool

import org.transmartproject.core.exceptions.InvalidRequestException
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.querytool.Constraint
import org.transmartproject.core.querytool.ConstraintByValue
import org.transmartproject.core.querytool.ConstraintByVcf
import org.transmartproject.core.querytool.Item
import org.transmartproject.core.querytool.Panel
import org.transmartproject.core.querytool.QueryDefinition
import org.transmartproject.db.ontology.AbstractQuerySpecifyingType

import static org.transmartproject.core.querytool.ConstraintByValue.Operator.*
import static org.transmartproject.db.support.DatabasePortabilityService.DatabaseType.ORACLE

class PatientSetQueryBuilderService {

    def conceptsResourceService

    def databasePortabilityService

    String buildPatientIdListQuery(QueryDefinition definition)
            throws InvalidRequestException {

        generalDefinitionValidation(definition)

        def panelNum = 1
        def panelClauses = definition.panels.collect { Panel panel ->

            def itemPredicates = panel.items.collect { Item it ->
                AbstractQuerySpecifyingType term
                try {
                    term = conceptsResourceService.getByKey(it.conceptKey)
                } catch (NoSuchResourceException nsr) {
                    throw new InvalidRequestException("No such concept key: " +
                            "$it.conceptKey", nsr)
                }

                String clause = doItem(term, it.constraint)
                String selectQuery
                if(it.type == "CLINICAL") {
                    selectQuery = "SELECT patient_num as patient_identifier " +
                            "FROM observation_fact WHERE $clause AND concept_cd != 'SECURITY' GROUP BY patient_identifier"
                } else if (it.type == "VCF") {
                    if(it.constraint ==  null) {
                        selectQuery = "SELECT patient_id as patient_identifier " +
                                "FROM deapp.de_subject_sample_mapping WHERE platform = 'VCF'"
                    } else {
                        selectQuery = "SELECT patient_id as patient_identifier " +
                            "FROM deapp.de_subject_sample_mapping WHERE $clause GROUP BY patient_identifier"
                    }
                }
                selectQuery
            }
            /*
             * itemPredicates are similar to this example:
             * concept_cd IN
             *  (SELECT concept_cd FROM concept_dimension WHERE concept_path
             *  LIKE '\\...\%')
             *  AND (
             *      (valtype_cd = 'N' AND nval_num > 50 AND tval_char IN
             *          ('E', 'GE'))
             *      OR
             *      (valtype_cd = 'N' AND nval_num >= 50 AND tval_char = 'G')
             * )
             */
            def bigPredicate = itemPredicates.collect { "($it)" }.join(' UNION ')

            if (panel.items.size() > 1) {
                bigPredicate = "$bigPredicate"
            }

            [
                id: panelNum++,
                select: bigPredicate,
                invert: panel.invert,
            ]
        }.sort { a, b ->
            (a.invert && !b.invert) ? 1
                    : (!a.invert && b.invert) ? -1
                    : (a.id - b.id)
        }

        def patientSubQuery
        if (panelClauses.size() == 1) {
            def panel = panelClauses[0]
            if (!panel.invert) {
                patientSubQuery =  panel.select
            } else {
                patientSubQuery = "SELECT patient_num as patient_identifier FROM patient_dimension " +
                        "EXCEPT $panel.select"
            }
        } else {
            patientSubQuery = panelClauses.inject("") { String acc, panel ->
                acc +
                        (acc.empty
                                ? ""
                                : panel.invert
                                ? " $databasePortabilityService.complementOperator "
                                : ' INTERSECT ') +
                        "$panel.select"
            }
        }
    }

    String buildPatientSetQuery(QtQueryResultInstance resultInstance,
                                QueryDefinition definition)
            throws InvalidRequestException {

        if (!resultInstance.id) {
            throw new RuntimeException('QtQueryResultInstance has not been persisted')
        }

        def patientSubQuery = buildPatientIdListQuery(definition)

        //$patientSubQuery has result set with single column: 'patient_num'
        def windowFunctionOrderBy = ''
        if (databasePortabilityService.databaseType == ORACLE) {
            //Oracle requires this, PostgreSQL supports it, and H2 rejects it
            windowFunctionOrderBy = 'ORDER BY patient_num'
        }

        def sql = "INSERT INTO qt_patient_set_collection (result_instance_id," +
                " patient_num, set_index) " +
                "SELECT ${resultInstance.id}, P.patient_identifier, " +
                " row_number() OVER ($windowFunctionOrderBy) " +
                "FROM ($patientSubQuery ORDER BY 1) P"

        log.debug "SQL statement: $sql"

        sql
    }

    /* Mapping between the number value constraint and the SQL predicates. The
     * value constraint may correspond to one or two SQL predicates ORed
     * together */
    private static final def NUMBER_QUERY_MAPPING = [
            (LOWER_THAN):          [['<',  ['E', 'LE']], ['<=', ['L']]],
            (LOWER_OR_EQUAL_TO):   [['<=', ['E', 'LE', 'L']]],
            (EQUAL_TO):            [['=',  ['E']]],
            (BETWEEN):             [['BETWEEN', ['E']]],
            (GREATER_THAN):        [['>',  ['E', 'GE']], ['>=', ['G']]],
            (GREATER_OR_EQUAL_TO): [['>=', ['E', 'GE', 'G']]]
    ]

    private String doItem(AbstractQuerySpecifyingType term,
                          Constraint constraint) {
        /* constraint represented by the ontology term */
        def clause = "$term.factTableColumn IN (${getQuerySql(term)})"

        /* additional (and optional) constraint by value */
        if (!constraint) {
            return clause
        }

        if(constraint instanceof ConstraintByValue) {
            clause = createValueConstraintQuery(constraint, clause)
        } else if (constraint instanceof ConstraintByVcf){
            clause = createVCFConstraintQuery(constraint)
        }

        clause
    }

    private String createValueConstraintQuery(ConstraintByValue constraint, String clause) {
            if (constraint.valueType == ConstraintByValue.ValueType.NUMBER) {
                def spec = NUMBER_QUERY_MAPPING[constraint.operator]
                def constraintValue = doConstraintNumber(constraint.operator,
                        constraint.constraint)

                def predicates = spec.collect {
                    "valtype_cd = 'N' AND nval_num ${it[0]} $constraintValue AND " +
                            "tval_char " + (it[1].size() == 1
                            ? "= '${it[1][0]}'"
                            : "IN (${it[1].collect { "'$it'" }.join ', '})")
                }

                clause += " AND (" + predicates.collect { "($it)" }.join(' OR ') + ")"
            } else if (constraint.valueType == ConstraintByValue.ValueType.FLAG) {
                clause += " AND (valueflag_cd = ${doConstraintFlag(constraint.constraint)})"
            } else {
                throw new InvalidRequestException('Unexpected value constraint type')
            }
        return clause
    }

    private String createVCFConstraintQuery(ConstraintByVcf constraint) {
        def clause = ""
        def (chromosome,position) = constraint.location.tokenize(':')
        def position2
        if(position.contains('-'))
            (position,position2) = position.tokenize('-')

        //(chromosome,position) = getVcfPosition(constraint)
        if (constraint.type == ConstraintByVcf.Type.STATUS) {
            clause += "subject_id IN ("+
            "SELECT subject_id "+
            "FROM deapp.de_variant_subject_summary " +
            "WHERE chr = '" + chromosome

            if(position2 == null) {
                clause += "' AND pos = " + position
            } else {
                clause += "' AND pos >= " + position + " AND pos <= " + position2
            }

            if(constraint.value == ConstraintByVcf.Value.WILDTYPE) {
                clause += " AND allele1 = 0 AND allele2 = 0)"
            } else if(constraint.value == ConstraintByVcf.Value.HETEROZYGOUS) {
                clause += " AND ((allele1 = 0 AND allele2 != 0) OR (allele1 != 0 AND allele2 = 0)))"
            } else if(constraint.value == ConstraintByVcf.Value.HOMOZYGOUS) {
                clause += " AND allele1 != 0 AND allele2 != 0)"
            }
        }
        return clause
    }

    private String getVcfPosition(ConstraintByVcf constraint) {
        //String chromosome = constraint.position.split(/:/)[0]
        //String position = constraint.position.split(/:/)[1]
        [chromosome,position]
    }

    /**
     * Returns the SQL for the query that this object represents.
     *
     * @return raw SQL of the query that this type represents
     */
    private String getQuerySql(AbstractQuerySpecifyingType term) {
        def res = "SELECT $term.factTableColumn " +
                "FROM $term.dimensionTableName " +
                "WHERE $term.columnName $term.operator $term.processedDimensionCode"
        if (databasePortabilityService.databaseType == ORACLE) {
            res += " ESCAPE '\\'"
        }
        res
    }

    private String doConstraintNumber(ConstraintByValue.Operator operator,
                                      String value) throws
            NumberFormatException, InvalidRequestException {

        /* validate constraint value to prevent injection */
        try {
            if (operator == BETWEEN) {
                def matcher = value =~
                        /([+-]?[0-9]+(?:\.[0-9]*)?)(?i: and )([+-]?[0-9]+(?:\.[0-9]*)?)/
                if (matcher.matches()) {
                    return Double.parseDouble(matcher.group(1).toString()) +
                            ' AND ' +
                            Double.parseDouble(matcher.group(2).toString())
                }
            } else {
                if (value =~ /[+-]?[0-9]+(?:\.[0-9]*)?/) {
                    return Double.parseDouble(value).toString()
                }
            }
        } catch (NumberFormatException nfe) {
            /* may fail because the number is too large, for instance.
             * We'd rather fail here than failing when the SQL statement is
             * compiled. */
            throw new InvalidRequestException("Error parsing " +
                    "constraint value: $nfe.message", nfe)
        }

        throw new InvalidRequestException("The value '$value' is an " +
                "invalid number constraint value for the operator $operator")
    }

    private String doConstraintFlag(String value) throws
            InvalidRequestException {

        if (['L', 'H', 'N'].contains(value)) {
            return "'$value'"
        } else {
            throw new InvalidRequestException("A flag value constraint's " +
                    "operand must be either 'L', 'H' or 'N'; got '$value'")
        }
    }

    private void generalDefinitionValidation(QueryDefinition definition) {
        if (!definition.panels) {
            throw new InvalidRequestException('No panels were specified')
        }

        if (definition.panels.any { Panel p -> !p.items }) {
            throw new InvalidRequestException('Found panel with no items')
        }

        def anyItem = { Closure c ->
            definition.panels.any { Panel p ->
                p.items.any { Item item ->
                    c(item)
                }
            }
        }
        if (anyItem { it == null }) {
            throw new InvalidRequestException('Found panel with null value in' +
                    ' its item list')
        }
        if(!anyItem {Item it -> it.constraint instanceof ConstraintByVcf}) {
        if (anyItem { Item it -> it.conceptKey == null }) {
            throw new InvalidRequestException('Found item with null conceptKey')
        }
        if (anyItem { it.constraint && it.constraint.constraint == null }) {
            throw new InvalidRequestException('Found item constraint with ' +
                    'null constraint value')
        }
        if (anyItem { it.constraint && it.constraint.operator == null }) {
            throw new InvalidRequestException('Found item constraint with ' +
                    'null operator')
        }
        if (anyItem { it.constraint && it.constraint.valueType == null }) {
            throw new InvalidRequestException('Found item constraint with ' +
                    'null value type')
        }
        if (anyItem { Item it -> it.constraint && it.constraint.valueType ==
                ConstraintByValue.ValueType.FLAG &&
                it.constraint.operator != EQUAL_TO }) {
            throw new InvalidRequestException('Found item flag constraint ' +
                    'with an operator different from EQUAL_TO')
        }
        }
    }

}
