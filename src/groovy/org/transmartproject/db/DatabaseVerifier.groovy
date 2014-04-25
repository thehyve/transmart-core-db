package org.transmartproject.db

class DatabaseVerifier extends DomainAnalyzer {

    Map<String, List<Problem>> getProblems() {
        Map<String, List<Problem>> result = [:]

        Map<String, List<String>> schemaTableMap = getSchemaTableMap()
        //schemaTableMap.each {println "${it.key} ${it.value}"}

        domainClasses.each {
            List<String> schemaAndTable = getSchemaAndTable(it)
            String schema = schemaAndTable[0]
            String tableName = schemaAndTable[1]

            if (schema == null) {
                //println "$tableName ${ToStringBuilder.reflectionToString(mapping.table)}"
                println "WARNING: could not get schema for entity ${it.name}"
                return
            }

            String key = "${schema}.${tableName}".toLowerCase()

            List<Problem> problems

            if (schema != null && schemaTableMap[schema]?.contains(tableName)) {
                //table exists: check missing columns
                Set<String> actualCols = getColumnsForTable(schema, tableName)
                if (actualCols.isEmpty()) {
                    println "WARNING: $tableName : table with no columns???"
                    return
                }
                Set<String> expectedCols = getColumnsForDomain(it)
                expectedCols.removeAll(actualCols)
                problems = expectedCols.collect { new Problem(item: it, cause: "Missing column") }

            } else {
                //no table
                problems = [ new Problem(item: key, cause: 'Missing table') ]
            }

            if (problems.size() > 0) {
                result.put(key, problems)
            }
        }
        result
    }

    static class Problem {
        String item
        String cause
    }
}
