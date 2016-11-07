package org.transmartproject.db

import grails.transaction.Transactional
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.dataquery.highdim.assayconstraints.AssayConstraint
import org.transmartproject.core.ontology.OntologyTerm
import org.transmartproject.export.Tasks.DataExportFetchTask
import org.transmartproject.export.Tasks.DataExportFetchTaskFactory
import org.transmartproject.core.dataquery.highdim.HighDimensionResource
import org.transmartproject.core.ontology.ConceptsResource
import org.transmartproject.export.DataTypeRetrieved

import static org.transmartproject.core.ontology.OntologyTerm.VisualAttributes.HIGH_DIMENSIONAL

@Transactional
class RestExportService {

    @Autowired
    DataExportFetchTaskFactory dataExportFetchTaskFactory

    @Autowired
    HighDimensionResource highDimensionResourceService

    ConceptsResource conceptsResourceService

    int cohortNumberID

    List<File> export(arguments) {
        DataExportFetchTask task = dataExportFetchTaskFactory.createTask(arguments)
        task.getTsv()
    }

    public def getDataTypes(List conceptKeysList, List dataTypes, Integer cohortNumber){
        cohortNumberID = cohortNumber
        conceptKeysList.each { conceptKey ->
            OntologyTerm concept = conceptsResourceService.getByKey(conceptKey)
            dataTypes = getDataType(concept, dataTypes)
        }
        dataTypes
    }

    private def getDataType(OntologyTerm term, List dataTypes) {
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
            //TODO: List with datatype objects. This part gets moved to the serializationhelper
            def datatypes = highDimensionResourceService.getSubResourcesAssayMultiMap([constraint])
            datatypes.collect({ key, value ->
                dataTypes = addDataType(term, dataTypes, key)
            })
            dataTypes
        }
        else {
            // No high dimensional data found for this term, this means it is clinical data
            dataTypes = addDataType(term, dataTypes)
        dataTypes
        }
    }

    private List<DataTypeRetrieved> addDataType(OntologyTerm term, List<DataTypeRetrieved> dataTypes, datatype = null) {
        String dataTypeString = datatype ? datatype.dataTypeDescription :"Clinical data"
        String dataTypeCode =  datatype ? datatype.dataTypeName : "clinical"
        List tempDataTypes = dataTypes.collect {it.dataType}
        if (dataTypeString in tempDataTypes){
            int index = tempDataTypes.indexOf(dataTypeString)
            DataTypeRetrieved dataType = dataTypes[index]
            addOntologyTerm(term, dataType)
        } else{
            DataTypeRetrieved dataType = new DataTypeRetrieved(dataType: dataTypeString, dataTypeCode: dataTypeCode)
            addOntologyTerm(term, dataType)
            dataTypes.add(dataType)
        }
        dataTypes
    }

    private void addOntologyTerm(OntologyTerm term, DataTypeRetrieved dataType) {
        if(cohortNumberID in dataType.OntologyTermsMap.keySet()) {
            dataType.OntologyTermsMap[cohortNumberID].add(term)
        } else {
            dataType.OntologyTermsMap[cohortNumberID] = [term]
        }

    }

}
