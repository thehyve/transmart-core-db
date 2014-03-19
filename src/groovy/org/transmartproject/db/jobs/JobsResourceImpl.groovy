package org.transmartproject.db.jobs

import com.google.common.collect.ImmutableSet
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.jobs.Job
import org.transmartproject.core.jobs.JobExecution
import org.transmartproject.core.jobs.JobsResource
import org.transmartproject.core.jobs.providers.JobDescriptor
import org.transmartproject.core.jobs.providers.JobProvider
import org.transmartproject.core.jobs.providers.JobsRegistry
import org.transmartproject.core.users.User

@Component
class JobsResourceImpl implements JobsResource {

    @Autowired
    JobExecutionsHelper executionsHelper

    @Autowired
    JobsRegistry jobsRegistry

    /* Jobs */

    @Override
    Set<Job> getJobs() {
        ImmutableSet.Builder builder = ImmutableSet.builder()

        jobsRegistry.providers.each { provider ->
            provider.jobDescriptors.each { descriptor ->
                builder.add createJob(provider, descriptor)
            }
        }

        builder.build()
    }

    @Override
    Job getJobByName(String providerName, String jobName) throws NoSuchResourceException {
        JobProvider provider = jobsRegistry.getJobProvider providerName

         createJob provider, provider.getDescriptorForJob(jobName)
    }

    private Job createJob(JobProvider provider, JobDescriptor descriptor) {
        new JobImpl(
                provider:          provider,
                descriptor:        descriptor,
                executionsHelper:  executionsHelper)
    }

    /* Job Executions */

    @Override
    List<JobExecution> searchJobExecution(Map params) throws InvalidArgumentsException {
        // provider, job, user, page, limit
        List<JobExecutionDb> execDbs = JobExecutionDb.withCriteria {
            if (params.provider != null) {
                eq 'provider', provider
            }
            if (params.job != null) {
                def job = params.job
                if (job instanceof Job) {
                    job = job.name
                }
                eq 'job', job
            }
            if (params.user != null) {
                def user_ = params.user
                if (user_ instanceof User) {
                    user_ = user_.username
                }
                user {
                    eq 'username', user_
                }
            }

            def limit = params.limit ?: 20
            maxResults limit
            if (params.page) {
                firstResult limit * (params.page - 1)
            }

            order 'lastUpdated', 'desc'
        }

        execDbs.collect {
            executionsHelper.loadJobExecution(it)
        }
    }

    @Override
    JobExecution getJobExecution(String providerName, String jobName, String jobExecutionId) {
        executionsHelper.loadJobExecution providerName, jobName, jobExecutionId
    }

    @Override
    JobExecution getJobExecution(Long globalId) {
        executionsHelper.loadJobExecution globalId
    }
}
