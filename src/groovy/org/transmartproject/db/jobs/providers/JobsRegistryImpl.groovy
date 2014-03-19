package org.transmartproject.db.jobs.providers

import com.google.common.collect.ImmutableSet
import com.google.common.collect.Maps
import org.springframework.stereotype.Component
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.jobs.providers.JobProvider
import org.transmartproject.core.jobs.providers.JobsRegistry

@Component
class JobsRegistryImpl implements JobsRegistry {

    private Map<String, JobProvider> providers = Maps.newHashMap()

    @Override
    void registerJobProvider(JobProvider jobProvider) {
        providers[jobProvider.name] = jobProvider
    }

    @Override
    Set<JobProvider> getProviders() {
        ImmutableSet.copyOf providers.values()
    }

    @Override
    JobProvider getJobProvider(String name) throws NoSuchResourceException {
        providers[name] ?: {
            throw new NoSuchResourceException(
                    "No provider with name '$name' exists")
        }()
    }

    void reset() {
        // for tests
        providers = Maps.newHashMap()
    }
}
