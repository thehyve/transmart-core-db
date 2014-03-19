package org.transmartproject.db.jobs

import org.transmartproject.core.exceptions.InvalidArgumentsException
import org.transmartproject.core.jobs.Job
import org.transmartproject.core.jobs.JobExecution
import org.transmartproject.core.jobs.providers.JobDescriptor
import org.transmartproject.core.jobs.providers.JobExecutionProvided

import org.transmartproject.core.jobs.providers.JobProvider

final class JobImpl implements Job {

    JobProvider provider
    JobDescriptor descriptor

    JobExecutionsHelper executionsHelper

    String getName() {
        descriptor.name
    }

    String getProviderName() {
        provider.name
    }

    String getUserReadableName() {
        descriptor.userReadableName
    }

    String getInitialPath() {
        provider.getJobInitialPath name
    }

    JobExecution createJobExecution(Map<String, Serializable> parameters)
            throws InvalidArgumentsException {
        JobExecutionProvided executionProvided =
                provider.createExecution parameters

        executionsHelper.createJobExecution parameters, executionProvided
    }

    String toString() {
        com.google.common.base.Objects.toStringHelper(this).
                add('name', name).
                add('userReadableMame', userReadableName).
                add('provider.name', provider.name).
                toString()
    }

    boolean equals(Object obj) {
        obj.getClass() == getClass() &&
                obj.descriptor == descriptor &&
                obj.provider.is(provider)
    }

    int hashCode() {
        Objects.hash(descriptor, provider.name)
    }
}
