// configuration for plugin testing - will not be included in the plugin zip

/* Keep pre-2.3.0 behavior */
grails.databinding.convertEmptyStringsToNull = false
grails.databinding.trimStrings = false

///*
//Example configuration for using the reveng plugin
grails.plugin.reveng.defaultSchema = 'i2b2demodata'
grails.plugin.reveng.includeTables = ['modifier_dimension', 'modifier_metadata']
grails.plugin.reveng.packageName = 'org.transmartproject.db.i2b2data'
//*/

grails.converters.default.pretty.print=true

grails.views.default.codec="none" // none, html, base64
grails.views.gsp.encoding="UTF-8"
hibernate {
    cache.use_second_level_cache = true
    cache.use_query_cache = false
    cache.region.factory_class = 'org.hibernate.cache.ehcache.SingletonEhCacheRegionFactory'
}

// environment specific settings
environments {
    test {
        dataSource {
            driverClassName = "org.h2.Driver"
            url = "jdbc:h2:mem:testDb;MVCC=TRUE;LOCK_TIMEOUT=10000;INIT=RUNSCRIPT FROM './h2_init.sql'"
            username = "sa"
            password = ""

            logSql    = true
            formatSql = true

            dbCreate = "update"
            pooled = true

            properties {
                // these small values make it easier to find leaks
                maxActive = 3
                minIdle = 1
                maxIdle = 1
            }
        }
    }
    development {
        dataSource {
            driverClassName = 'org.postgresql.Driver'
            url             = 'jdbc:postgresql://localhost:5433/transmart'
            dialect         = 'org.hibernate.dialect.PostgreSQLDialect'

//            driverClassName = 'oracle.jdbc.driver.OracleDriver'
//            url             = 'jdbc:oracle:thin:@localhost:11521:CI'
//            dialect         = 'org.hibernate.dialect.Oracle10gDialect'

            username        = 'biomart_user'
            password        = 'biomart_user'
            dbCreate        = 'none'
        }
    }
}