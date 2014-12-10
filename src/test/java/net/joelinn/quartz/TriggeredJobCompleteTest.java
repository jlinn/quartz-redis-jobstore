package net.joelinn.quartz;

import org.junit.Before;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.Trigger;
import org.quartz.impl.triggers.CronTriggerImpl;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.verify;

/**
 * Joe Linn
 * 7/22/2014
 */
public class TriggeredJobCompleteTest extends BaseTest{
    protected JobDetail job;

    protected CronTriggerImpl trigger1;

    protected CronTriggerImpl trigger2;

    @Before
    public void setUp() throws JobPersistenceException {
        // store a job with some triggers
        job = getJobDetail("job1", "jobGroup1");
        trigger1 = getCronTrigger("trigger1", "triggerGroup1", job.getKey());
        trigger2 = getCronTrigger("trigger2", "triggerGroup1", job.getKey());
        storeJobAndTriggers(job, trigger1, trigger2);
    }

    @Test
    public void triggeredJobCompleteDelete() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.DELETE_TRIGGER);

        // ensure that the proper trigger was deleted
        assertThat(jobStore.retrieveTrigger(trigger1.getKey()), nullValue());
        assertThat(jobStore.retrieveTrigger(trigger2.getKey()), not(nullValue()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void triggeredJobCompleteComplete() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE);

        // ensure that neither trigger was deleted
        assertThat(jobStore.retrieveTrigger(trigger1.getKey()), not(nullValue()));
        assertThat(jobStore.retrieveTrigger(trigger2.getKey()), not(nullValue()));

        // ensure that the proper trigger was set to COMPLETE
        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void triggeredJobCompleteError() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_TRIGGER_ERROR);

        // ensure that the proper trigger was set to ERROR
        assertEquals(Trigger.TriggerState.ERROR, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void triggeredJobCompleteAllError() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);

        // ensure that both triggers were set to ERROR
        assertEquals(Trigger.TriggerState.ERROR, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.ERROR, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void triggeredJobCompleteAllComplete() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE);

        // ensure that both triggers were set to COMPLETE
        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void triggeredJobCompletePersist() throws JobPersistenceException {
        JobDetail jobPersist = JobBuilder.newJob(TestJobPersist.class)
                .withIdentity("testJobPersist1", "jobGroupPersist1")
                .usingJobData("timeout", 42)
                .withDescription("I am describing a job!")
                .build();
        CronTriggerImpl triggerPersist1 = getCronTrigger("triggerPersist1", "triggerPersistGroup1", jobPersist.getKey());
        CronTriggerImpl triggerPersist2 = getCronTrigger("triggerPersist2", "triggerPersistGroup1", jobPersist.getKey());
        storeJobAndTriggers(jobPersist, triggerPersist1, triggerPersist1);

        jobStore.triggeredJobComplete(triggerPersist1, jobPersist, Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE);

        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(triggerPersist1.getKey()));
    }

    @Test
    public void triggeredJobCompleteNonConcurrent() throws JobPersistenceException {
        JobDetail job = JobBuilder.newJob(TestJobNonConcurrent.class)
                .withIdentity("testJobNonConcurrent1", "jobGroupNonConcurrent1")
                .usingJobData("timeout", 42)
                .withDescription("I am describing a job!")
                .build();
        CronTriggerImpl trigger1 = getCronTrigger("triggerNonConcurrent1", "triggerNonConcurrentGroup1", job.getKey());
        CronTriggerImpl trigger2 = getCronTrigger("triggerNonConcurrent2", "triggerNonConcurrentGroup1", job.getKey());
        storeJobAndTriggers(job, trigger1, trigger2);

        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE);

        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(trigger1.getKey()));

        final String jobHashKey = schema.jobHashKey(job.getKey());
        assertFalse(jedis.sismember(schema.blockedJobsSet(), jobHashKey));
    }
}
