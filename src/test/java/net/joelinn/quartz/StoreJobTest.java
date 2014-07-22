package net.joelinn.quartz;

import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.calendar.WeeklyCalendar;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;

/**
 * Joe Linn
 * 7/15/2014
 */
public class StoreJobTest extends BaseTest{

    @Test
    public void testStoreJob(){
        JobDetail testJob = getJobDetail();

        try {
            jobStore.storeJob(testJob, false);
        } catch (JobPersistenceException e) {
            e.printStackTrace();
            fail();
        }

        // ensure that the job was stored properly
        String jobHashKey = schema.jobHashKey(testJob.getKey());
        Map<String, String> jobMap = jedis.hgetAll(jobHashKey);

        assertNotNull(jobMap);
        assertThat(jobMap, hasKey("name"));
        assertEquals("testJob", jobMap.get("name"));

        Map<String, String> jobData = jedis.hgetAll(schema.jobDataMapHashKey(testJob.getKey()));
        assertNotNull(jobData);
        assertThat(jobData, hasKey("timeout"));
        assertEquals("42", jobData.get("timeout"));
    }

    @Test(expected = ObjectAlreadyExistsException.class)
    public void testStoreJobNoReplace() throws JobPersistenceException {
        jobStore.storeJob(getJobDetail(), false);
        jobStore.storeJob(getJobDetail(), false);
    }

    @Test
    public void testStoreJobWithReplace(){
        try {
            jobStore.storeJob(getJobDetail(), true);
            jobStore.storeJob(getJobDetail(), true);
        } catch (JobPersistenceException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testRetrieveJob(){
        JobDetail testJob = getJobDetail();
        try {
            jobStore.storeJob(testJob, false);
        } catch (JobPersistenceException e) {
            e.printStackTrace();
            fail();
        }

        // retrieve the job
        JobDetail retrievedJob = null;
        try {
            retrievedJob = jobStore.retrieveJob(testJob.getKey());
        } catch (JobPersistenceException e) {
            e.printStackTrace();
            fail();
        }

        assertEquals(testJob.getJobClass(), retrievedJob.getJobClass());
        assertEquals(testJob.getDescription(), retrievedJob.getDescription());
        JobDataMap retrievedJobJobDataMap = retrievedJob.getJobDataMap();
        for (String key : testJob.getJobDataMap().keySet()) {
            assertThat(retrievedJobJobDataMap, hasKey(key));
            assertEquals(String.valueOf(testJob.getJobDataMap().get(key)), retrievedJobJobDataMap.get(key));
        }
    }

    @Test
    public void testRetrieveNonExistentJob() throws JobPersistenceException {
        assertThat(jobStore.retrieveJob(new JobKey("foo", "bar")), nullValue());
    }

    @Test
    public void testRemoveJob() throws JobPersistenceException {
        // attempt to remove a non-existent job
        assertFalse(jobStore.removeJob(JobKey.jobKey("foo", "bar")));

        // create and store a job with multiple triggers
        JobDetail job = getJobDetail("job1", "jobGroup1");
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "triggerGroup1", job.getKey());
        CronTriggerImpl trigger2 = getCronTrigger("trigger2", "triggerGroup1", job.getKey());
        Set<Trigger> triggersSet = new HashSet<>();
        triggersSet.add(trigger1);
        triggersSet.add(trigger2);
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        jobsAndTriggers.put(job, triggersSet);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        assertTrue(jobStore.removeJob(job.getKey()));

        // ensure that the job and all of its triggers were removed
        assertThat(jobStore.retrieveJob(job.getKey()), nullValue());
        assertThat(jobStore.retrieveTrigger(trigger1.getKey()), nullValue());
        assertThat(jobStore.retrieveTrigger(trigger2.getKey()), nullValue());
    }

    @Test
    public void testRemoveJobs() throws JobPersistenceException {
        // create and store some jobs with triggers
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = getJobsAndTriggers(2, 2, 2, 2);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        List<JobKey> removeKeys = new ArrayList<>(2);
        for (JobDetail jobDetail : new ArrayList<>(jobsAndTriggers.keySet()).subList(0, 1)) {
            removeKeys.add(jobDetail.getKey());
        }

        assertTrue(jobStore.removeJobs(removeKeys));

        // ensure that only the proper jobs were removed
        for (JobKey removeKey : removeKeys) {
            assertThat(jobStore.retrieveJob(removeKey), nullValue());
        }
        for (JobDetail jobDetail : new ArrayList<>(jobsAndTriggers.keySet()).subList(1, 3)) {
            assertThat(jobStore.retrieveJob(jobDetail.getKey()), not(nullValue()));
        }
    }

    @Test
    public void testGetNumberOfJobs() throws JobPersistenceException {
        jobStore.storeJob(getJobDetail("job1", "group1"), false);
        jobStore.storeJob(getJobDetail("job2", "group1"), false);
        jobStore.storeJob(getJobDetail("job3", "group2"), false);

        int numberOfJobs = jobStore.getNumberOfJobs();

        assertEquals(3, numberOfJobs);
    }

    @Test
    public void testGetJobKeys() throws JobPersistenceException {
        jobStore.storeJob(getJobDetail("job1", "group1"), false);
        jobStore.storeJob(getJobDetail("job2", "group1"), false);
        jobStore.storeJob(getJobDetail("job3", "group2"), false);

        Set<JobKey> jobKeys = jobStore.getJobKeys(GroupMatcher.jobGroupEquals("group1"));

        assertThat(jobKeys, hasSize(2));
        assertThat(jobKeys, contains(new JobKey("job1", "group1"), new JobKey("job2", "group1")));
    }

    @Test
    public void testGetJobGroupNames() throws JobPersistenceException {
        List<String> jobGroupNames = jobStore.getJobGroupNames();

        assertThat(jobGroupNames, not(nullValue()));
        assertThat(jobGroupNames, hasSize(0));

        jobStore.storeJob(getJobDetail("job1", "group1"), false);
        jobStore.storeJob(getJobDetail("job2", "group1"), false);
        jobStore.storeJob(getJobDetail("job3", "group2"), false);

        jobGroupNames = jobStore.getJobGroupNames();

        assertThat(jobGroupNames, hasSize(2));
        assertThat(jobGroupNames, contains("group1", "group2"));
    }

    @Test
    public void testPauseJob() throws JobPersistenceException {
        // create and store a job with multiple triggers
        JobDetail job = getJobDetail("job1", "jobGroup1");
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "triggerGroup1", job.getKey());
        CronTriggerImpl trigger2 = getCronTrigger("trigger2", "triggerGroup1", job.getKey());
        Set<Trigger> triggersSet = new HashSet<>();
        triggersSet.add(trigger1);
        triggersSet.add(trigger2);
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        jobsAndTriggers.put(job, triggersSet);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        // pause the job
        jobStore.pauseJob(job.getKey());

        // ensure that the job's triggers were paused
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger2.getKey()));
    }

    @Test
    public void testPauseJobs() throws JobPersistenceException {
        // create and store some jobs with triggers
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = getJobsAndTriggers(2, 2, 2, 2);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        // pause jobs from one of the groups
        String pausedGroupName = new ArrayList<>(jobsAndTriggers.keySet()).get(0).getKey().getGroup();
        jobStore.pauseJobs(GroupMatcher.jobGroupEquals(pausedGroupName));

        // ensure that the appropriate triggers have been paused
        for (Map.Entry<JobDetail, Set<? extends Trigger>> entry : jobsAndTriggers.entrySet()) {
            for (Trigger trigger : entry.getValue()) {
                if(entry.getKey().getKey().getGroup().equals(pausedGroupName)){
                    Trigger.TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
                    assertEquals(Trigger.TriggerState.PAUSED, triggerState);
                }
                else{
                    assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger.getKey()));
                }
            }
        }
    }

    @Test
    public void testResumeJob() throws JobPersistenceException {
        // create and store a job with multiple triggers
        JobDetail job = getJobDetail("job1", "jobGroup1");
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "triggerGroup1", job.getKey());
        trigger1.computeFirstFireTime(new WeeklyCalendar());
        CronTriggerImpl trigger2 = getCronTrigger("trigger2", "triggerGroup1", job.getKey());
        trigger2.computeFirstFireTime(new WeeklyCalendar());
        Set<Trigger> triggersSet = new HashSet<>();
        triggersSet.add(trigger1);
        triggersSet.add(trigger2);
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        jobsAndTriggers.put(job, triggersSet);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        // pause the job
        jobStore.pauseJob(job.getKey());

        // ensure that the job's triggers have been paused
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger2.getKey()));

        // resume the job
        jobStore.resumeJob(job.getKey());

        // ensure that the triggers have been resumed
        assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger2.getKey()));
    }

    @Test
    public void testResumeJobs() throws JobPersistenceException {
        // attempt to resume jobs for a non-existent job group
        Collection<String> resumedJobGroups = jobStore.resumeJobs(GroupMatcher.jobGroupEquals("foobar"));
        assertThat(resumedJobGroups, hasSize(0));

        // store some jobs with triggers
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = getJobsAndTriggers(2, 2, 2, 2);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        // pause one of the job groups
        String pausedGroupName = new ArrayList<>(jobsAndTriggers.keySet()).get(0).getKey().getGroup();
        jobStore.pauseJobs(GroupMatcher.jobGroupEquals(pausedGroupName));

        // ensure that the appropriate triggers have been paused
        for (Map.Entry<JobDetail, Set<? extends Trigger>> entry : jobsAndTriggers.entrySet()) {
            for (Trigger trigger : entry.getValue()) {
                if(entry.getKey().getKey().getGroup().equals(pausedGroupName)){
                    Trigger.TriggerState triggerState = jobStore.getTriggerState(trigger.getKey());
                    assertEquals(Trigger.TriggerState.PAUSED, triggerState);
                }
                else{
                    assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger.getKey()));
                }
            }
        }

        // resume the paused jobs
        resumedJobGroups = jobStore.resumeJobs(GroupMatcher.jobGroupEquals(pausedGroupName));

        assertThat(resumedJobGroups, hasSize(1));
        assertEquals(pausedGroupName, new ArrayList<>(resumedJobGroups).get(0));

        for (Trigger trigger : new ArrayList<>(jobsAndTriggers.entrySet()).get(0).getValue()) {
            assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger.getKey()));
        }
    }

}
