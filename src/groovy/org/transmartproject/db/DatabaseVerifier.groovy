package org.transmartproject.db

import org.codehaus.groovy.grails.commons.GrailsDomainClass
import org.codehaus.groovy.grails.commons.GrailsDomainClassProperty
import org.codehaus.groovy.grails.orm.hibernate.cfg.GrailsDomainBinder
import org.codehaus.groovy.grails.orm.hibernate.cfg.Mapping
import org.codehaus.groovy.grails.orm.hibernate.cfg.PropertyConfig
import org.hibernate.SessionFactory
import org.hibernate.cfg.ImprovedNamingStrategy
import org.hibernate.cfg.NamingStrategy
import org.hibernate.jdbc.Work

import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.SQLException

class DatabaseVerifier {

    def grailsApplication

    private static String sessionFactoryBeanName = 'sessionFactory'

    @Lazy private DatabaseMetaData metaData = { retrieveMetadata() }()

    @Lazy private List<GrailsDomainClass> domainClasses = { grailsApplication.getArtefacts('Domain').toList() }()

    private GrailsDomainBinder binder = new GrailsDomainBinder()
    private NamingStrategy namingStrategy = ImprovedNamingStrategy.INSTANCE

    Map<String, List<Problem>> getProblems() {
        Map<String, List<Problem>> result = [:]

        Map<String, List<String>> schemaTableMap = getSchemaTableMap()
        //schemaTableMap.each {println "${it.key} ${it.value}"}

        domainClasses.each {
            Mapping mapping = binder.getMapping(it)
            String schema = mapping.table.schema?.toLowerCase()
            String tableName = mapping.table.name ?: namingStrategy.classToTableName(it.name)

            if (tableName.contains('.')) {
                //some domain classes have a composite <schema>.<table> table name
                String[] parts = tableName.split('\\.')
                schema = parts[0].toLowerCase()
                tableName = parts[1].toLowerCase()
            }

            if (schema == null) {
                //println "$tableName ${ToStringBuilder.reflectionToString(mapping.table)}"
                println "WARNING: could not get schema for entity ${it.name}"
                return
            }

            String key = "${schema}.${tableName}".toLowerCase()

            List<Problem> problems

            if (schema != null && schemaTableMap[schema]?.contains(tableName)) {
                //table exists: check missing columns
                Set<String> actualCols = getColumns(schema, tableName)
                if (actualCols.isEmpty()) {
                    println "WARNING: $tableName : table with no columns???"
                    return
                }
                Set<String> expectedCols = getColumns(it, mapping)
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


    private Map<String, List<String>> getSchemaTableMap() {
        String[] types = null
        types = ['TABLE', 'VIEW'].toArray()
        ResultSet rset = metaData.getTables(null, null,null, types)

        Map<String, List<String>> result = [:]
        while (rset.next()) {
            String schema = rset.getString(2).toLowerCase()
            String table = rset.getString(3).toLowerCase()
            List<String> list = result.get(schema)
            if (list == null) {
                list = []
                result.put(schema, list)
            }

            list.add(table)
        }

        result
    }

    private Set<String> getColumns(GrailsDomainClass clazz, Mapping mapping) {

        Map cols = mapping.getColumns()

        clazz.persistentProperties.toList().findAll {
            !it.oneToMany
        } collect { GrailsDomainClassProperty prop ->
            //println "${clazz.name} ${prop.name} ${ToStringBuilder.reflectionToString(prop)}"

            PropertyConfig cfg = cols.get(prop.name)
            if (cfg != null) {
                //println "${clazz.name} ${prop.name} ${cfg.column} ${cfg.columns}"
            }
            String column = cfg?.column
            if (column == null) {
                column = namingStrategy.propertyToColumnName(prop.name)
            }
            column.toLowerCase()
        } toSet()
    }

    private Set<String> getColumns(String schema, String tableName) {
        ResultSet rset = metaData.getColumns(null, schema, tableName, null)
        Set<String> result = new HashSet<>()
        while (rset.next()) {
            result.add(rset.getString(4).toLowerCase())
        }
        result
    }

    private DatabaseMetaData retrieveMetadata() {
        SessionFactory sf = grails.util.Holders.grailsApplication.mainContext.sessionFactory
        DatabaseMetaData md
        Work work = new Work() {
            @Override
            void execute(Connection connection) throws SQLException {
                md = connection.getMetaData()
            }
        }
        sf.currentSession.doWork(work)
        md

    }

    static class Problem {
        String item
        String cause
    }
}
