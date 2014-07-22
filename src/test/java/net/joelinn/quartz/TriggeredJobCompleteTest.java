package net.joelinn.quartz;

import org.junit.Before;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.Trigger;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
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
        Set<Trigger> triggersSet = new HashSet<>();
        triggersSet.add(trigger1);
        triggersSet.add(trigger2);
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        jobsAndTriggers.put(job, triggersSet);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);
    }

    @Test
    public void testTriggeredJobCompleteDelete() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.DELETE_TRIGGER);

        // ensure that the proper trigger was deleted
        assertThat(jobStore.retrieveTrigger(trigger1.getKey()), nullValue());
        assertThat(jobStore.retrieveTrigger(trigger2.getKey()), not(nullValue()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void testTriggeredJobCompleteComplete() throws JobPersistenceException {
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
    public void testTriggeredJobCompleteError() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_TRIGGER_ERROR);

        // ensure that the proper trigger was set to ERROR
        assertEquals(Trigger.TriggerState.ERROR, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void testTriggeredJobCompleteAllError() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR);

        // ensure that both triggers were set to ERROR
        assertEquals(Trigger.TriggerState.ERROR, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.ERROR, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void testTriggeredJobCompleteAllComplete() throws JobPersistenceException {
        jobStore.triggeredJobComplete(trigger1, job, Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE);

        // ensure that both triggers were set to COMPLETE
        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(trigger2.getKey()));

        verify(mockScheduleSignaler).signalSchedulingChange(0L);
    }

    @Test
    public void testTriggeredJobCompletePersist() throws JobPersistenceException {
        JobDetail jobPersist = JobBuilder.newJob(TestJobPersist.class)
                .withIdentity("testJobPersist1", "jobGroupPersist1")
                .usingJobData("timeout", 42)
                .withDescription("I am describing a job!")
                .build();
        CronTriggerImpl triggerPersist1 = getCronTrigger("triggerPersist1", "triggerPersistGroup1", jobPersist.getKey());
        CronTriggerImpl triggerPersist2 = getCronTrigger("triggerPersist2", "triggerPersistGroup1", jobPersist.getKey());
        Set<Trigger> triggersSet = new HashSet<>();
        triggersSet.add(triggerPersist1);
        triggersSet.add(triggerPersist2);
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        jobsAndTriggers.put(jobPersist, triggersSet);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        jobStore.triggeredJobComplete(triggerPersist1, jobPersist, Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE);

        assertEquals(Trigger.TriggerState.COMPLETE, jobStore.getTriggerState(triggerPersist1.getKey()));
    }
}
