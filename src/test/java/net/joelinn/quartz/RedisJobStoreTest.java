package net.joelinn.quartz;

import com.google.common.base.Joiner;
import net.joelinn.quartz.jobstore.RedisJobStore;
import net.joelinn.quartz.jobstore.RedisJobStoreSchema;
import org.junit.Test;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.JobStore;
import org.quartz.spi.SchedulerSignaler;
import redis.clients.jedis.*;
import redis.clients.util.Pool;
import redis.embedded.RedisCluster;
import redis.embedded.util.JedisUtil;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

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
