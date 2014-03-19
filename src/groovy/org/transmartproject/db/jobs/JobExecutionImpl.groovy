package org.transmartproject.db.jobs

import org.transmartproject.core.jobs.Job
import org.transmartproject.core.jobs.JobExecutionState
import org.transmartproject.core.jobs.providers.JobExecutionInternal
import org.transmartproject.core.jobs.providers.JobExecutionProvided
import org.transmartproject.core.users.User

class JobExecutionImpl implements JobExecutionInternal {

    JobExecutionDb       jobExecutionDb
    JobExecutionProvided jobExecutionProvided /* may be null */
    Job                  job

    Long getId() {
        jobExecutionDb.id
    }

    String getLocalId() {
        jobExecutionDb.localId
    }

    String getFriendlyId() {
        "${jobExecutionDb.user.username}-${jobExecutionDb.provider}-" +
                "${jobExecutionDb.job}-${jobExecutionDb.id}"
    }

    User getUser() {
        jobExecutionDb.user
    }

    Map<String, Serializable> getParameters() {
        jobExecutionDb.parameters
    }

    JobExecutionState getState() {
        jobExecutionDb.state
    }

    String getLatestStepDescription() {
        jobExecutionDb.latestStepDescription
    }

    void start() throws IllegalStateException {
        if (jobExecutionProvided && state == JobExecutionState.CREATED) {
            jobExecutionProvided.start()
        } else {
            throw new IllegalStateException(
                    'start() cannot be called at this point')
        }
    }

    void requestCancellation() throws IllegalStateException {
        if (jobExecutionProvided && state == JobExecutionState.RUNNING) {
            jobExecutionProvided.requestCancellation()
        } else {
            throw new IllegalStateException(
                    'requestCancellation() cannot be called at this point')
        }
    }

    String getResultPath() throws IllegalStateException {
        if (jobExecutionProvided && state == JobExecutionState.FINISHED) {
            jobExecutionProvided.resultPath
        } else {
            throw new IllegalStateException(
                    'getResultPath() cannot be called at this point')
        }
    }

    void setState(JobExecutionState newState) throws IllegalStateException {
        if (state != JobExecutionState.CREATED &&
                state != JobExecutionState.RUNNING) {
            throw new IllegalStateException(
                    'The state cannot be changed at this point')
        }

        jobExecutionDb.state = newState
        jobExecutionDb.save(failOnError: true)
    }

    void setLatestStepDescription(String newDescription) throws IllegalStateException {
        jobExecutionDb.latestStepDescription = newDescription
        jobExecutionDb.save(failOnError: true)
    }
}
