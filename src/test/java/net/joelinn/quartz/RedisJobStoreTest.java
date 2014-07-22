package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisJobStore;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasKey;

/**
 * Joe Linn
 * 7/22/2014
 */
public class RedisJobStoreTest extends BaseTest{
    @Test
    public void testRedisJobStoreWithScheduler() throws SchedulerException {
        Properties quartzProperties = new Properties();
        quartzProperties.setProperty("org.quartz.scheduler.instanceName", "testScheduler");
        quartzProperties.setProperty("org.quartz.threadPool.threadCount", "3");
        quartzProperties.setProperty("org.quartz.jobStore.class", RedisJobStore.class.getName());
        quartzProperties.setProperty("org.quartz.jobStore.host", "localhost");
        quartzProperties.setProperty("org.quartz.jobStore.port", "6379");
        quartzProperties.setProperty("org.quartz.jobStore.lockTimeout", "2000");

        StdSchedulerFactory schedulerFactory = new StdSchedulerFactory();
        schedulerFactory.initialize(quartzProperties);

        Scheduler scheduler = schedulerFactory.getScheduler();
        scheduler.start();

        JobDetail job = getJobDetail("testJob1", "testJobGroup1");
        CronTriggerImpl trigger = getCronTrigger("testTrigger1", "testTriggerGroup1", job.getKey(), "0/5 * * * * ?");

        scheduler.scheduleJob(job, trigger);

        // ensure that the job was scheduled
        JobDetail retrievedJob = jobStore.retrieveJob(job.getKey());
        assertThat(retrievedJob, not(nullValue()));
        assertThat(retrievedJob.getJobDataMap(), hasKey("timeout"));

        CronTriggerImpl retrievedTrigger = (CronTriggerImpl) jobStore.retrieveTrigger(trigger.getKey());
        assertThat(retrievedTrigger, not(nullValue()));
        assertEquals(trigger.getCronExpression(), retrievedTrigger.getCronExpression());

        scheduler.deleteJob(job.getKey());

        assertThat(jobStore.retrieveJob(job.getKey()), nullValue());
        assertThat(jobStore.retrieveTrigger(trigger.getKey()), nullValue());
    }
}
