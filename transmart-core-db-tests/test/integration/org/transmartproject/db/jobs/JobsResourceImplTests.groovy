package org.transmartproject.db.jobs

import com.google.common.collect.ImmutableSet
import grails.test.mixin.TestMixin
import grails.util.Holders
import junit.extensions.TestSetup
import org.gmock.WithGMock
import org.hibernate.SessionFactory
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.springframework.beans.factory.annotation.Autowired
import org.transmartproject.core.exceptions.NoSuchResourceException
import org.transmartproject.core.jobs.JobExecution
import org.transmartproject.core.jobs.JobExecutionState
import org.transmartproject.core.jobs.JobsResource
import org.transmartproject.core.jobs.providers.JobDescriptor
import org.transmartproject.core.jobs.providers.JobExecutionProvided
import org.transmartproject.core.jobs.providers.JobProvider
import org.transmartproject.db.jobs.providers.JobsRegistryImpl
import org.transmartproject.db.test.RuleBasedIntegrationTestMixin
import org.transmartproject.db.user.AccessLevelTestData
import org.transmartproject.db.user.User

import static groovy.test.GroovyAssert.shouldFail
import static org.hamcrest.MatcherAssert.assertThat
import static org.hamcrest.Matchers.*
import static org.transmartproject.db.dataquery.highdim.HighDimTestData.save

@TestMixin(RuleBasedIntegrationTestMixin)
@WithGMock
class JobsResourceImplTests {

    public static final String MOCK_PROVIDER_NAME = 'mock_provider'

    @Autowired
    JobsResource testee

    @Autowired
    JobsRegistryImpl jobsRegistry

    JobProvider mockJobProvider

    @Before
    void setUp() {
        mockJobProvider = mock(JobProvider)
        mockJobProvider.name.returns(MOCK_PROVIDER_NAME).atLeastOnce()
    }

    @After
    void tearDown() {
        jobsRegistry.reset()
    }

    @Test
    void testGetJobs() {
        mockJobProvider.jobDescriptors.returns(ImmutableSet.of(
                new JobDescriptor(name: 'job1', userReadableName: 'Job 1'),
                new JobDescriptor(name: 'job2', userReadableName: 'Job 2'),
        ))

        mockJobProvider.getJobInitialPath('job1').returns('/initialPath?job=job1')
        mockJobProvider.getJobInitialPath('job2').returns('/initialPath?job=job2')


        play {
            registerMockProvider()
            def result = testee.jobs

            assertThat result, allOf(
                    everyItem(
                            hasProperty('providerName', is(MOCK_PROVIDER_NAME))),
                    containsInAnyOrder(
                            allOf(
                                    hasProperty('name', is('job1')),
                                    hasProperty('userReadableName', is('Job 1')),
                                    hasProperty('initialPath', is('/initialPath?job=job1')),
                            ),
                            allOf(
                                    hasProperty('name', is('job2')),
                                    hasProperty('userReadableName', is('Job 2')),
                                    hasProperty('initialPath', is('/initialPath?job=job2')),
                            )))
        }
    }

    @Test
    void testGetJobByName() {
        mockJob 1

        play {
            registerMockProvider()
            def result = testee.getJobByName(MOCK_PROVIDER_NAME, 'job1')

            assertThat result, allOf(
                    hasProperty('providerName', is(MOCK_PROVIDER_NAME)),
                    hasProperty('name', is('job1')))
        }
    }

    @Test
    void testGetJobByNameNonExistingJob() {
        mockJobProvider.getDescriptorForJob('job1').raises(NoSuchResourceException)

        play {
            registerMockProvider()
            shouldFail NoSuchResourceException, {
                testee.getJobByName(MOCK_PROVIDER_NAME, 'job1')
            }
        }
    }

    @Test
    void testSearchJobExecutionsExpiredSearchByUser() {
        User u = AccessLevelTestData.createUsers(1, -100L)[0]
        List<JobExecutionDb> executions = createJobExecutionDbs(
                4, -200L, u, 'job1', JobExecutionState.EXPIRED)
        mockJob 1

        save([u])
        save executions

        // modify 2nd execution so it comes first
        executions[1].latestStepDescription = 'my description'

        play {
            registerMockProvider()
            def result = testee.searchJobExecution(user: u)

            assertThat result, allOf(
                    everyItem(allOf(
                            isA(JobExecution),
                            hasProperty('job',
                                    hasProperty('name', is('job1'))),
                            hasProperty('user', is(u)),
                            hasProperty('state', is(JobExecutionState.EXPIRED))
                    )))

            assertThat result, hasSize(4)
            assertThat result[0], hasProperty('latestStepDescription',
                    is('my description'))
        }

    }

    @Test
    void testSearchJobExecutionsExpiredSearchByJob() {
        User u = AccessLevelTestData.createUsers(1, -100L)[0]
        List<JobExecutionDb> executions = [
                *createJobExecutionDbs(
                        2, -200L, u, 'job1', JobExecutionState.EXPIRED),
                *createJobExecutionDbs(
                        2, -300L, u, 'job2', JobExecutionState.EXPIRED)]
        mockJob 2

        save([u])
        save executions

        play {
            registerMockProvider()
            def result = testee.searchJobExecution(user: u, job: 'job2')

            assertThat result, allOf(
                    hasSize(2),
                    everyItem(
                            hasProperty('job',
                                    hasProperty('name', is('job2')))))
        }
    }

    @Test
    void testSearchJobExecutionsExpiredLimitAndPage() {
        User u = AccessLevelTestData.createUsers(1, -100L)[0]
        List<JobExecutionDb> executions = createJobExecutionDbs(
                4, -200L, u, 'job1', JobExecutionState.EXPIRED)
        mockJob 1

        save([u])
        save executions

        /* change 2nd and 4th execution so they show up on the 1st page */
        executions[1].latestStepDescription = 'foo'
        executions[3].latestStepDescription = 'bar'

        play {
            registerMockProvider()
            // page 1 is the default
            def result = testee.searchJobExecution(user: u, limit: 2)

            assertThat result, containsInAnyOrder(
                hasProperty('localId', is(executions[1].localId)),
                hasProperty('localId', is(executions[3].localId)))

            result = testee.searchJobExecution(user: u, limit: 2, page: 2)
            assertThat result, containsInAnyOrder(
                    hasProperty('localId', is(executions[0].localId)),
                    hasProperty('localId', is(executions[2].localId)))
        }
    }

    @Test
    void testSearchJobExecutionsFinished() {
        JobExecutionDb exec = saveSingleJobExecution JobExecutionState.FINISHED

        JobExecutionProvided jobExecutionProvided = mock(JobExecutionProvided)
        mockJobProvider.getExecution('job1', exec.localId).
                returns(jobExecutionProvided)

        jobExecutionProvided.resultPath.returns('/path')

        play {
            registerMockProvider()

            def result = testee.searchJobExecution(user: exec.user)

            assertThat result, contains(
                    allOf(
                            hasProperty('job',
                                    hasProperty('name', is('job1'))),
                            hasProperty('resultPath', is('/path'))))
        }
    }

    @Test
    void testGetJobExecutionByLocalId() {
        JobExecutionDb exec = saveSingleJobExecution JobExecutionState.EXPIRED

        play {
            registerMockProvider()

            def result = testee.getJobExecution(MOCK_PROVIDER_NAME, 'job1',
                    exec.localId)

            assertThat result, allOf(
                    hasProperty('localId', is(exec.localId)),
                    hasProperty('job', allOf(
                            hasProperty('name', is('job1')),
                            hasProperty('providerName', is(MOCK_PROVIDER_NAME)))))
        }
    }

    @Test
    void testGetJobExecutionByLocalIdNotFound() {
        play {
            registerMockProvider()

            shouldFail NoSuchResourceException, {
                testee.getJobExecution(MOCK_PROVIDER_NAME, 'job1',
                        'non_existent_local_id')
            }
        }
    }

    @Test
    void testGetJobExecutionByGlobalId() {
        JobExecutionDb exec = saveSingleJobExecution JobExecutionState.EXPIRED

        assertThat exec.id, is(notNullValue())

        play {
            registerMockProvider()

            def result = testee.getJobExecution(exec.id)

            assertThat result, hasProperty('id', is(exec.id))
        }
    }

    @Test
    void testGetJobExecutionByGlobalIdNotFound() {
        play {
            registerMockProvider()

            shouldFail NoSuchResourceException, {
                testee.getJobExecution(-42 /* non existent id */)
            }
        }
    }

    @Test
    void testRequestCancellation() {
        JobExecutionDb exec = saveSingleJobExecution JobExecutionState.RUNNING

        JobExecutionProvided execProvided = mock JobExecutionProvided
        mockJobProvider.getExecution('job1', exec.localId).returns execProvided
        execProvided.requestCancellation().once()

        play {
            registerMockProvider()
            def jobExec = testee.getJobExecution(
                    MOCK_PROVIDER_NAME, 'job1', exec.localId)
            jobExec.requestCancellation()
        }
    }

    @Test
    void testStart() {
        JobExecutionDb exec = saveSingleJobExecution JobExecutionState.CREATED

        JobExecutionProvided execProvided = mock JobExecutionProvided
        mockJobProvider.getExecution('job1', exec.localId).returns execProvided
        execProvided.start().once()

        play {
            registerMockProvider()
            def jobExec = testee.getJobExecution(
                    MOCK_PROVIDER_NAME, 'job1', exec.localId)
            jobExec.start()
        }
    }

    JobExecutionDb saveSingleJobExecution(JobExecutionState state) {
        User u = AccessLevelTestData.createUsers(1, -100L)[0]
        List<JobExecutionDb> executions = createJobExecutionDbs(
                1, -200L, u, 'job1', state)
        mockJob 1

        save([u])
        save executions

        executions[0]
    }


    void mockJob(int i) {
        mockJobProvider.getDescriptorForJob('job' + i).returns(
                new JobDescriptor(name: 'job' + i, userReadableName: 'Job ' + i),
        ).atLeastOnce()
    }

    List<JobExecutionDb> createJobExecutionDbs(int count,
                                               long baseId,
                                               User user,
                                               String job,
                                               JobExecutionState state)
    {
        def base = [
                provider: MOCK_PROVIDER_NAME,
                job:      job,
                state:    state,
                user:     user,
        ]

        (1..count).collect {
            new JobExecutionDb([
                    *: base,
                    localId: 'foo_' + (
                            (baseId - it).toString().replaceAll(/-/, '_')),
            ])
        }

    }


    void registerMockProvider() {
        jobsRegistry.registerJobProvider(mockJobProvider)
    }
}

