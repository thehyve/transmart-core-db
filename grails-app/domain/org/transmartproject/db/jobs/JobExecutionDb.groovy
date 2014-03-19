package org.transmartproject.db.jobs

import grails.converters.JSON
import org.transmartproject.core.jobs.JobExecutionState
import org.transmartproject.db.user.User

class JobExecutionDb {

    String provider
    String job
    String localId

    String parametersString

    JobExecutionState state
    String latestStepDescription

    Date dateCreated
    Date lastUpdated

    static transients = ['parameters']

    static belongsTo = [user: User]

    static fetchMode = [user: 'eager']

    static mapping = {
        table schema: 'deapp', name: 'job_execution'

        autoTimestamp true
    }

    static constraints = {

        provider              minSize: 1, maxSize: 50,  validator: { it ==~ /[a-z0-9_]+/ }
        job                   minSize: 1, maxSize: 50,  validator: { it ==~ /[a-zA-Z0-9_]+/ }
        localId               (unique: ['provider', 'job'],
                               maxSize: 50, minSize: 1,
                               validator: { it ==~ /[a-z0-9_]+/ })

        parametersString      nullable: true, maxSize: 10000
        latestStepDescription nullable: true
    }

    void setParameters(Map parameters) {
        parametersString = (parameters as JSON).toString(false)
    }

    Map getParameters() {
        parametersString ? JSON.parse(parametersString) : null
    }

}
