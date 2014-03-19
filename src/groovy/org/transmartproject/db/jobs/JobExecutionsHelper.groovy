package org.transmartproject.db.jobs

import org.hibernate.ObjectNotFoundException
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.jobs.Job
import org.transmartproject.core.jobs.JobExecutionState
import org.transmartproject.core.jobs.JobsResource
import org.transmartproject.core.jobs.providers.JobExecutionInternal
import org.transmartproject.core.jobs.providers.JobExecutionProvided
import org.transmartproject.core.jobs.providers.JobProvider
import org.transmartproject.core.jobs.providers.JobsRegistry
import org.transmartproject.db.user.User

/**
 * Helper for building {@link JobExecutionInternal} objects.
 */
@Component
class JobExecutionsHelper {

    @Autowired
    JobsRegistry registry

    @Autowired
    JobsResource jobsResource

    JobExecutionInternal createJobExecution(User user,
                                            JobProvider provider,
                                            Job job,
                                            Map parameters,
                                            JobExecutionProvided jobExecutionProvided)
            throws InvalidArgumentsException {
        JobExecutionProvided executionProvided =
                provider.createExecution(parameters, job.name)

        JobExecutionDb jobExecutionDb = new JobExecutionDb(
                provider:   job.providerName,
                job:        job.name,
                localId:    executionProvided.localId,
                parameters: parameters,
                state:      JobExecutionState.CREATED,
                user:       user)

        jobExecutionDb.save(failOnError: true)

        new JobExecutionImpl(
                job:                  job,
                jobExecutionDb:       jobExecutionDb,
                jobExecutionProvided: executionProvided)
    }

    JobExecutionInternal loadJobExecution(String providerName,
                                          String jobName,
                                          String executionId) throws NoSuchResourceException {
        JobExecutionDb jobExecutionDb = JobExecutionDb.
                findByProviderAndJobAndLocalId providerName, jobName, executionId
        if (!jobExecutionDb) {
            throw new NoSuchResourceException("No job execution found with " +
                    "provider '$providerName', job '$jobName' and local id " +
                    "'$executionId'")
        }

        loadJobExecution jobExecutionDb
    }

    JobExecutionInternal loadJobExecution(Long id) throws NoSuchResourceException {
        try {
            loadJobExecution JobExecutionDb.load(id)
        } catch (ObjectNotFoundException onfe) {
            throw new NoSuchResourceException(
                    "No job execution with id $id exists")
        }
    }

    JobExecutionInternal loadJobExecution(JobExecutionDb jobExecutionDb) {
        Job job = jobsResource.getJobByName(
                jobExecutionDb.provider, jobExecutionDb.job)
        def result = new JobExecutionImpl(
                jobExecutionDb: jobExecutionDb,
                job:            job)

        if (jobExecutionDb.state == JobExecutionState.CREATED ||
                jobExecutionDb.state == JobExecutionState.RUNNING ||
                jobExecutionDb.state == JobExecutionState.FINISHED) {
            def provider = registry.getJobProvider(jobExecutionDb.provider)
            try {
                JobExecutionProvided providedExec = provider.getExecution(
                        jobExecutionDb.job, jobExecutionDb.localId)
                result.jobExecutionProvided = providedExec
            } catch (NoSuchResourceException nsre) {
                if (result.state == JobExecutionState.FINISHED) {
                    result.state = JobExecutionState.EXPIRED
                } else {
                    result.state = JobExecutionState.DEFUNCT
                }
            }
        }

        result
    }
}
