package org.transmartproject.db

import grails.transaction.Transactional
import groovy.json.JsonException
import groovy.json.JsonSlurper
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.dataquery.highdim.HighDimensionResource
import org.transmartproject.core.dataquery.highdim.assayconstraints.AssayConstraint
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.ontology.ConceptsResource
import org.transmartproject.core.ontology.OntologyTerm
import org.transmartproject.export.DataTypeRetrieved
import org.transmartproject.export.Tasks.DataExportFetchTask
import org.transmartproject.export.Tasks.DataExportFetchTaskFactory

import static org.transmartproject.core.ontology.OntologyTerm.VisualAttributes.HIGH_DIMENSIONAL

@Transactional
class RestExportService {

    @Autowired
    DataExportFetchTaskFactory dataExportFetchTaskFactory

    @Autowired
    HighDimensionResource highDimensionResourceService

    ConceptsResource conceptsResourceService

    int cohortNumberID

    List<DataTypeRetrieved> dataTypes

    List<File> export(arguments) {
        DataExportFetchTask task = dataExportFetchTaskFactory.createTask(arguments)
        task.getTsv()
    }

    public def retrieveDataTypes(params) {
        if (!(params.containsKey('concepts'))) {
            throw new NoSuchResourceException("No parameter named concepts was given.")
        }

        if (params.get('concepts') == "") {
            throw new InvalidArgumentsException("Parameter concepts has no value.")
        }

        def jsonSlurper = new JsonSlurper()
        def conceptParameters = params.get('concepts').decodeURL()
        dataTypes = []
        try {
            def conceptArguments = jsonSlurper.parseText(conceptParameters)
            int cohortNumber = 1
            conceptArguments.each { it ->
                List conceptKeysList = it.conceptKeys
                cohortNumberID = cohortNumber
                conceptKeysList.collect { conceptKey ->
                    getDataType(conceptKey)
                }
                cohortNumber += 1
            }
            dataTypes
        } catch (JsonException e) {
            throw new InvalidArgumentsException("Given parameter was non valid JSON.")
        }
    }

    private void getDataType(String conceptKey) {
        OntologyTerm term = conceptsResourceService.getByKey(conceptKey)
        // Retrieve all descendant terms that have the HIGH_DIMENSIONAL attribute
        def terms = term.getAllDescendants() + term
        def highDimTerms = terms.findAll { it.visualAttributes.contains(HIGH_DIMENSIONAL) }

        if (highDimTerms) {
            // Put all high dimensional term keys in a disjunction constraint
            def constraint = highDimensionResourceService.createAssayConstraint(
                    AssayConstraint.DISJUNCTION_CONSTRAINT,
                    subconstraints: [
                            (AssayConstraint.ONTOLOGY_TERM_CONSTRAINT):
                                    highDimTerms.collect({
                                        [concept_key: it.key]
                                    })
                    ]
            )
            def datatypes = highDimensionResourceService.getSubResourcesAssayMultiMap([constraint])
            datatypes.collect({ key, value ->
                addDataType(term, key)
            })
        } else {
            // No high dimensional data found for this term, this means it is clinical data
            addDataType(term)
        }
    }

    private void addDataType(OntologyTerm term, datatype = null) {
        String dataTypeString = datatype ? datatype.dataTypeDescription : "Clinical data"
        String dataTypeCode = datatype ? datatype.dataTypeName : "clinical"
        List tempDataTypes = dataTypes.collect { it.dataType }
        if (dataTypeString in tempDataTypes) {
            int index = tempDataTypes.indexOf(dataTypeString)
            DataTypeRetrieved dataType = dataTypes[index]
            addOntologyTerm(term, dataType)
        } else {
            DataTypeRetrieved dataType = new DataTypeRetrieved(dataType: dataTypeString, dataTypeCode: dataTypeCode)
            addOntologyTerm(term, dataType)
            dataTypes.add(dataType)
        }
    }

    private void addOntologyTerm(OntologyTerm term, DataTypeRetrieved dataType) {
        if (cohortNumberID in dataType.OntologyTermsMap.keySet()) {
            dataType.OntologyTermsMap[cohortNumberID].add(term)
        } else {
            dataType.OntologyTermsMap[cohortNumberID] = [term]
        }

    }

}
