package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisJobStore;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Trigger;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Joe Linn
 * 7/22/2014
 */
public class RedisJobStoreTest extends BaseTest{

    @Test
    public void redisJobStoreWithScheduler() throws Exception {
        Properties quartzProperties = new Properties();
        quartzProperties.setProperty("org.quartz.scheduler.instanceName", "testScheduler");
        quartzProperties.setProperty("org.quartz.threadPool.threadCount", "3");
        quartzProperties.setProperty("org.quartz.jobStore.class", RedisJobStore.class.getName());
        quartzProperties.setProperty("org.quartz.jobStore.host", host);
        quartzProperties.setProperty("org.quartz.jobStore.port", Integer.toString(port));
        quartzProperties.setProperty("org.quartz.jobStore.lockTimeout", "2000");
        quartzProperties.setProperty("org.quartz.jobStore.database", "1");

        testJobStore(quartzProperties);
    }

    @Test
    public void clearAllSchedulingData() throws Exception {
        // create and store some jobs, triggers, and calendars
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = getJobsAndTriggers(2, 2, 2, 2);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);

        // ensure that the jobs, triggers, and calendars were stored
        assertEquals(2, (long) jedis.scard(schema.jobGroupsSet()));
        assertEquals(4, (long) jedis.scard(schema.jobsSet()));
        assertEquals(8, (long) jedis.scard(schema.triggerGroupsSet()));
        assertEquals(16, (long) jedis.scard(schema.triggersSet()));

        jobStore.clearAllSchedulingData();

        assertEquals(0, (long) jedis.scard(schema.jobGroupsSet()));
        assertEquals(0, (long) jedis.scard(schema.jobsSet()));
        assertEquals(0, (long) jedis.scard(schema.triggerGroupsSet()));
        assertEquals(0, (long) jedis.scard(schema.triggersSet()));
    }


}
