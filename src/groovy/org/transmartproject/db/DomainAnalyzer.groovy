package org.transmartproject.db

import com.google.common.collect.BiMap
import com.google.common.collect.HashBiMap
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

class DomainAnalyzer {

    private static String sessionFactoryBeanName = 'sessionFactory'

    def grailsApplication = grails.util.Holders.grailsApplication

    @Lazy List<GrailsDomainClass> domainClasses = { grailsApplication.getArtefacts('Domain').toList() }()
    @Lazy private DatabaseMetaData metaData = { retrieveMetadata() }()

    private GrailsDomainBinder binder = new GrailsDomainBinder()
    private NamingStrategy namingStrategy = ImprovedNamingStrategy.INSTANCE

    private DatabaseMetaData retrieveMetadata() {
        SessionFactory sf = grailsApplication.mainContext.sessionFactory
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

    List<String> getSchemaAndTable(GrailsDomainClass clazz) {
        Mapping mapping = binder.getMapping(clazz)
        String schema = mapping.table.schema?.toLowerCase()
        String tableName = mapping.table.name ?: namingStrategy.classToTableName(clazz.name)

        if (tableName.contains('.')) {
            //some domain classes have a composite <schema>.<table> table name
            String[] parts = tableName.split('\\.')
            schema = parts[0].toLowerCase()
            tableName = parts[1].toLowerCase()
        }

        [schema, tableName]
    }

    Map<String, List<String>> getSchemaTableMap() {
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

    Set<String> getColumnsForDomain(GrailsDomainClass clazz) {
        Mapping mapping = binder.getMapping(clazz)
        Map cols = mapping.getColumns()

        clazz.persistentProperties.toList().findAll {
            !it.oneToMany
        } collect { GrailsDomainClassProperty prop ->
            //println "${clazz.name} ${prop.name} ${ToStringBuilder.reflectionToString(prop)}"
            getColummnName(prop, cols)
        } toSet()
    }

    private String getColummnName(GrailsDomainClassProperty prop, Map cols) {
        PropertyConfig cfg = cols.get(prop.name)
        if (cfg != null) {
            //println "${clazz.name} ${prop.name} ${cfg.column} ${cfg.columns}"
        }
        String column = cfg?.column
        if (column == null) {
            column = namingStrategy.propertyToColumnName(prop.name)
        }
        column.toLowerCase()
    }


    Set<String> getColumnsForTable(String schema, String tableName) {
        ResultSet rset = metaData.getColumns(null, schema, tableName, null)
        Set<String> result = new HashSet<>()
        while (rset.next()) {
            result.add(rset.getString(4).toLowerCase())
        }
        result
    }

    BiMap<List<String>, GrailsDomainClass> getSchemaAndTableToClassMap() {
        BiMap<List<String>, GrailsDomainClass> result = new HashBiMap<>()

        domainClasses.each {
            List<String> schemaAndTable = getSchemaAndTable(it)
            if (schemaAndTable[0] != null) {
                result.put(schemaAndTable, it)
            }
        }
        result
    }

    List<List<String>> getDomainDependencies() {

        List result = []
        BiMap<GrailsDomainClass, List<String>> map = getSchemaAndTableToClassMap().inverse()

        map.keySet().each {
            result.addAll(getDependencies(map, it))
        }

        result
    }

    private List<List<String>> getDependencies(BiMap<GrailsDomainClass, List<String>> map, GrailsDomainClass clazz) {
        List<String> clazzSchemaTable = map.get(clazz)
        Mapping mapping = binder.getMapping(clazz)
        //@todo
        null
    }

}
