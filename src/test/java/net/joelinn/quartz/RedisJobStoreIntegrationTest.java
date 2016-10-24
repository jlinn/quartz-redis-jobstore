package net.joelinn.quartz;

import com.google.common.base.Strings;
import net.jodah.concurrentunit.Waiter;
import net.joelinn.quartz.jobstore.RedisJobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.NameMatcher;
import org.quartz.simpl.PropertySettingJobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.util.Pool;
import redis.embedded.RedisServer;

import java.util.Properties;

import static net.joelinn.quartz.TestUtils.*;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Joe Linn
 *         10/4/2016
 */
public class RedisJobStoreIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(RedisJobStoreIntegrationTest.class);

    private RedisServer redisServer;
    private Scheduler scheduler;
    private Pool<Jedis> jedisPool;

    @Before
    public void setUp() throws Exception {
        final int port = getPort();
        final String host = "localhost";
        redisServer = RedisServer.builder()
                .port(port)
                .build();
        redisServer.start();

        jedisPool = new JedisPool(host, port);

        Properties config = new Properties();
        config.setProperty("org.quartz.jobStore.class", RedisJobStore.class.getName());
        config.setProperty("org.quartz.jobStore.host", host);
        config.setProperty("org.quartz.jobStore.port", String.valueOf(port));
        config.setProperty("org.quartz.threadPool.threadCount", "1");
        config.setProperty("org.quartz.jobStore.misfireThreshold", "500");
        scheduler = new StdSchedulerFactory(config).getScheduler();
        scheduler.start();
    }


    @After
    public void tearDown() throws Exception {
        scheduler.shutdown();
        if (jedisPool != null) {
            jedisPool.close();
        }
        redisServer.stop();
    }


    @Test
    public void testCompleteListener() throws Exception {
        final String jobName = "oneJob";
        JobDetail jobDetail = createJob(TestJob.class, jobName, "oneGroup");

        final String triggerName = "trigger1";
        CronTrigger trigger = createCronTrigger(triggerName, "oneGroup", "* * * * * ?");

        Waiter waiter = new Waiter();
        scheduler.scheduleJob(jobDetail, trigger);
        scheduler.getListenerManager().addTriggerListener(new CompleteListener(waiter), NameMatcher.triggerNameEquals(triggerName));

        // wait for CompleteListener.triggerComplete() to be called
        waiter.await(1500);
    }


    @Test
    public void testMisfireListener() throws Exception {
        final String jobName = "oneJob";
        JobDetail jobDetail = createJob(TestJob.class, jobName, "oneGroup");

        final String triggerName = "trigger1";
        final String everySecond = "* * * * * ?";
        CronTrigger trigger = createCronTrigger(triggerName, "oneGroup", everySecond);


        JobDetail sleepJob = createJob(SleepJob.class, "sleepJob", "twoGroup");
        CronTrigger sleepTrigger = createCronTrigger("sleepTrigger", "twoGroup", everySecond);
        Waiter waiter = new Waiter();
        scheduler.scheduleJob(sleepJob, sleepTrigger);
        scheduler.scheduleJob(jobDetail, trigger);

        scheduler.getListenerManager().addTriggerListener(new MisfireListener(waiter), NameMatcher.triggerNameEquals(triggerName));

        // wait for MisfireListener.triggerMisfired() to be called
        waiter.await(2500);
    }


    @Test
    public void testTriggerData() throws Exception {
        final String jobName = "good";
        JobDetail jobDetail = createJob(DataJob.class, jobName, "goodGroup");

        final String triggerName = "trigger1";
        final String everySecond = "* * * * * ?";
        CronTrigger trigger = createCronTrigger(triggerName, "oneGroup", everySecond);
        trigger = trigger.getTriggerBuilder()
                .usingJobData("foo", "bar")
                .build();
        scheduler.setJobFactory(new RedisJobFactory());
        scheduler.scheduleJob(jobDetail, trigger);
        Waiter waiter = new Waiter();
        scheduler.getListenerManager().addTriggerListener(new CompleteListener(waiter), NameMatcher.triggerNameEquals(triggerName));

        // wait for CompleteListener.triggerComplete() to be called
        waiter.await(1500);

        try (Jedis jedis = jedisPool.getResource()) {
            assertThat(jedis.get("foo"), equalTo("bar"));
        }
    }


    private class RedisJobFactory extends PropertySettingJobFactory {
        @Override
        protected void setBeanProps(Object obj, JobDataMap data) throws SchedulerException {
            data.put("jedisPool", jedisPool);
            super.setBeanProps(obj, data);
        }
    }


    public static class DataJob implements Job {
        private Pool<Jedis> jedisPool;

        public void setJedisPool(Pool<Jedis> jedisPool) {
            this.jedisPool = jedisPool;
        }

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            String foo = context.getTrigger().getJobDataMap().getString("foo");
            if (!Strings.isNullOrEmpty(foo)) {
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.set("foo", foo);
                }
            } else {
                log.error("Null or empty string retrieved from Redis.");
            }
        }
    }


    public static class SleepJob implements Job {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                throw new JobExecutionException("Interrupted while sleeping.", e);
            }
        }
    }


    private class CompleteListener implements TriggerListener {
        private final Waiter waiter;

        private CompleteListener(Waiter waiter) {
            this.waiter = waiter;
        }

        @Override
        public String getName() {
            return "Inigo Montoya";
        }

        @Override
        public void triggerFired(Trigger trigger, JobExecutionContext context) {

        }

        @Override
        public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
            return false;
        }

        @Override
        public void triggerMisfired(Trigger trigger) {

        }

        @Override
        public void triggerComplete(Trigger trigger, JobExecutionContext context, Trigger.CompletedExecutionInstruction triggerInstructionCode) {
            waiter.resume();
        }
    }


    private class MisfireListener implements TriggerListener {
        private final Waiter waiter;

        private MisfireListener(Waiter waiter) {
            this.waiter = waiter;
        }

        @Override
        public String getName() {
            return "Rugen";
        }

        @Override
        public void triggerFired(Trigger trigger, JobExecutionContext context) {

        }

        @Override
        public boolean vetoJobExecution(Trigger trigger, JobExecutionContext context) {
            return false;
        }

        @Override
        public void triggerMisfired(Trigger trigger) {
            waiter.resume();
        }

        @Override
        public void triggerComplete(Trigger trigger, JobExecutionContext context, Trigger.CompletedExecutionInstruction triggerInstructionCode) {

        }
    }
}
