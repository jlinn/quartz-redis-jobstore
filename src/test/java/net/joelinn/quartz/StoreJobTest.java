package net.joelinn.quartz;

import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.*;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.*;

/**
 * Joe Linn
 * 7/15/2014
 */
public class StoreJobTest extends BaseTest{

    @Test
    public void storeJob() throws Exception {
        JobDetail testJob = getJobDetail();

        jobStore.storeJob(testJob, false);

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
    public void storeJobNoReplace() throws Exception {
        jobStore.storeJob(getJobDetail(), false);
        jobStore.storeJob(getJobDetail(), false);
    }

    @Test
    public void storeJobWithReplace() throws Exception {
        jobStore.storeJob(getJobDetail(), true);
        jobStore.storeJob(getJobDetail(), true);
    }

    @Test
    public void retrieveJob() throws Exception {
        JobDetail testJob = getJobDetail();
        jobStore.storeJob(testJob, false);

        // retrieve the job
        JobDetail retrievedJob = jobStore.retrieveJob(testJob.getKey());

        assertEquals(testJob.getJobClass(), retrievedJob.getJobClass());
        assertEquals(testJob.getDescription(), retrievedJob.getDescription());
        JobDataMap retrievedJobJobDataMap = retrievedJob.getJobDataMap();
        for (String key : testJob.getJobDataMap().keySet()) {
            assertThat(retrievedJobJobDataMap, hasKey(key));
            assertEquals(String.valueOf(testJob.getJobDataMap().get(key)), retrievedJobJobDataMap.get(key));
        }
    }

    @Test
    public void retrieveNonExistentJob() throws Exception {
        assertThat(jobStore.retrieveJob(new JobKey("foo", "bar")), nullValue());
    }

    @Test
    public void removeJob() throws Exception {
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
        assertThat(jedis.get(schema.triggerHashKey(trigger1.getKey())), nullValue());
    }

    @Test
    public void removeJobs() throws Exception {
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
    public void getNumberOfJobs() throws Exception {
        jobStore.storeJob(getJobDetail("job1", "group1"), false);
        jobStore.storeJob(getJobDetail("job2", "group1"), false);
        jobStore.storeJob(getJobDetail("job3", "group2"), false);

        int numberOfJobs = jobStore.getNumberOfJobs();

        assertEquals(3, numberOfJobs);
    }

    @Test
    public void getJobKeys() throws Exception {
        jobStore.storeJob(getJobDetail("job1", "group1"), false);
        jobStore.storeJob(getJobDetail("job2", "group1"), false);
        jobStore.storeJob(getJobDetail("job3", "group2"), false);

        Set<JobKey> jobKeys = jobStore.getJobKeys(GroupMatcher.jobGroupEquals("group1"));

        assertThat(jobKeys, hasSize(2));
        assertThat(jobKeys, containsInAnyOrder(new JobKey("job1", "group1"), new JobKey("job2", "group1")));

        jobStore.storeJob(getJobDetail("job4", "awesomegroup1"), false);

        jobKeys = jobStore.getJobKeys(GroupMatcher.jobGroupContains("group"));

        assertThat(jobKeys, hasSize(4));
        assertThat(jobKeys, containsInAnyOrder(new JobKey("job1", "group1"), new JobKey("job2", "group1"),
                new JobKey("job4", "awesomegroup1"), new JobKey("job3", "group2")));

        jobKeys = jobStore.getJobKeys(GroupMatcher.jobGroupStartsWith("awe"));

        assertThat(jobKeys, hasSize(1));
        assertThat(jobKeys, containsInAnyOrder(new JobKey("job4", "awesomegroup1")));

        jobKeys = jobStore.getJobKeys(GroupMatcher.jobGroupEndsWith("1"));

        assertThat(jobKeys, hasSize(3));
        assertThat(jobKeys, containsInAnyOrder(new JobKey("job1", "group1"), new JobKey("job2", "group1"),
                new JobKey("job4", "awesomegroup1")));
    }

    @Test
    public void getJobGroupNames() throws Exception {
        List<String> jobGroupNames = jobStore.getJobGroupNames();

        assertThat(jobGroupNames, not(nullValue()));
        assertThat(jobGroupNames, hasSize(0));

        jobStore.storeJob(getJobDetail("job1", "group1"), false);
        jobStore.storeJob(getJobDetail("job2", "group1"), false);
        jobStore.storeJob(getJobDetail("job3", "group2"), false);

        jobGroupNames = jobStore.getJobGroupNames();

        assertThat(jobGroupNames, hasSize(2));
        assertThat(jobGroupNames, containsInAnyOrder("group1", "group2"));
    }

    @Test
    public void pauseJob() throws Exception {
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
    public void pauseJobsEquals() throws Exception {
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
    public void pauseJobsStartsWith() throws Exception {
        JobDetail job1 = getJobDetail("job1", "jobGroup1");
        storeJobAndTriggers(job1, getCronTrigger("trigger1", "triggerGroup1", job1.getKey()), getCronTrigger("trigger2", "triggerGroup1", job1.getKey()));
        JobDetail job2 = getJobDetail("job2", "yobGroup1");
        CronTriggerImpl trigger3 = getCronTrigger("trigger3", "triggerGroup3", job2.getKey());
        CronTriggerImpl trigger4 = getCronTrigger("trigger4", "triggerGroup4", job2.getKey());
        storeJobAndTriggers(job2, trigger3, trigger4);

        // pause jobs with groups beginning with "yob"
        Collection<String> pausedJobs = jobStore.pauseJobs(GroupMatcher.jobGroupStartsWith("yob"));
        assertThat(pausedJobs, hasSize(1));
        assertThat(pausedJobs, containsInAnyOrder("yobGroup1"));

        // ensure that the job was added to the paused jobs set
        assertTrue(jedis.sismember(schema.pausedJobGroupsSet(), schema.jobGroupSetKey(job2.getKey())));

        // ensure that the job's triggers have been paused
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger3.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger4.getKey()));
    }

    @Test
    public void pauseJobsEndsWith() throws Exception {
        JobDetail job1 = getJobDetail("job1", "jobGroup1");
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "triggerGroup1", job1.getKey());
        CronTriggerImpl trigger2 = getCronTrigger("trigger2", "triggerGroup1", job1.getKey());
        storeJobAndTriggers(job1, trigger1, trigger2);
        JobDetail job2 = getJobDetail("job2", "yobGroup2");
        CronTriggerImpl trigger3 = getCronTrigger("trigger3", "triggerGroup3", job2.getKey());
        CronTriggerImpl trigger4 = getCronTrigger("trigger4", "triggerGroup4", job2.getKey());
        storeJobAndTriggers(job2, trigger3, trigger4);

        // pause job groups ending with "1"
        Collection<String> pausedJobs = jobStore.pauseJobs(GroupMatcher.jobGroupEndsWith("1"));
        assertThat(pausedJobs, hasSize(1));
        assertThat(pausedJobs, containsInAnyOrder("jobGroup1"));

        // ensure that the job was added to the paused jobs set
        assertTrue(jedis.sismember(schema.pausedJobGroupsSet(), schema.jobGroupSetKey(job1.getKey())));

        // ensure that the job's triggers have been paused
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger2.getKey()));
    }

    @Test
    public void pauseJobsContains() throws Exception {
        JobDetail job1 = getJobDetail("job1", "jobGroup1");
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "triggerGroup1", job1.getKey());
        CronTriggerImpl trigger2 = getCronTrigger("trigger2", "triggerGroup1", job1.getKey());
        storeJobAndTriggers(job1, trigger1, trigger2);
        JobDetail job2 = getJobDetail("job2", "yobGroup2");
        CronTriggerImpl trigger3 = getCronTrigger("trigger3", "triggerGroup3", job2.getKey());
        CronTriggerImpl trigger4 = getCronTrigger("trigger4", "triggerGroup4", job2.getKey());
        storeJobAndTriggers(job2, trigger3, trigger4);

        // Pause job groups containing "foo". Should result in no jobs being paused.
        Collection<String> pausedJobs = jobStore.pauseJobs(GroupMatcher.jobGroupContains("foo"));
        assertThat(pausedJobs, hasSize(0));

        // pause jobs containing "Group"
        pausedJobs = jobStore.pauseJobs(GroupMatcher.jobGroupContains("Group"));
        assertThat(pausedJobs, hasSize(2));
        assertThat(pausedJobs, containsInAnyOrder("jobGroup1", "yobGroup2"));

        // ensure that both jobs were added to the paused jobs set
        assertTrue(jedis.sismember(schema.pausedJobGroupsSet(), schema.jobGroupSetKey(job1.getKey())));
        assertTrue(jedis.sismember(schema.pausedJobGroupsSet(), schema.jobGroupSetKey(job2.getKey())));

        // ensure that all triggers were paused
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger1.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger2.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger3.getKey()));
        assertEquals(Trigger.TriggerState.PAUSED, jobStore.getTriggerState(trigger4.getKey()));
    }

    @Test
    public void resumeJob() throws Exception {
        // create and store a job with multiple triggers
        JobDetail job = getJobDetail("job1", "jobGroup1");
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "triggerGroup1", job.getKey());
        CronTriggerImpl trigger2 = getCronTrigger("trigger2", "triggerGroup1", job.getKey());
        storeJobAndTriggers(job, trigger1, trigger2);

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
    public void resumeJobsEquals() throws Exception {
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

    @Test
    public void resumeJobsEndsWith() throws Exception {
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = getJobsAndTriggers(2, 2, 2, 2);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        // pause one of the job groups
        String pausedGroupName = new ArrayList<>(jobsAndTriggers.keySet()).get(0).getKey().getGroup();
        String substring = pausedGroupName.substring(pausedGroupName.length() - 1, pausedGroupName.length());
        Collection<String> pausedGroups = jobStore.pauseJobs(GroupMatcher.jobGroupEndsWith(substring));

        assertThat(pausedGroups, hasSize(1));
        assertThat(pausedGroups, containsInAnyOrder(pausedGroupName));

        // resume the paused jobs
        Collection<String> resumedGroups = jobStore.resumeJobs(GroupMatcher.jobGroupEndsWith(substring));

        assertThat(resumedGroups, hasSize(1));
        assertThat(resumedGroups, containsInAnyOrder(pausedGroupName));

        for (Trigger trigger : new ArrayList<>(jobsAndTriggers.entrySet()).get(0).getValue()) {
            assertEquals(Trigger.TriggerState.NORMAL, jobStore.getTriggerState(trigger.getKey()));
        }
    }
}
