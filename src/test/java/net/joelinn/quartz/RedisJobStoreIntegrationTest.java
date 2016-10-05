package net.joelinn.quartz;

import net.jodah.concurrentunit.Waiter;
import net.joelinn.quartz.jobstore.RedisJobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.NameMatcher;
import redis.embedded.RedisServer;

import java.util.Properties;

import static net.joelinn.quartz.TestUtils.createCronTrigger;
import static net.joelinn.quartz.TestUtils.createJob;
import static net.joelinn.quartz.TestUtils.getPort;

/**
 * @author Joe Linn
 *         10/4/2016
 */
public class RedisJobStoreIntegrationTest {
    private RedisServer redisServer;
    private Scheduler scheduler;

    @Before
    public void setUp() throws Exception {
        final int port = getPort();
        final String host = "localhost";
        redisServer = RedisServer.builder()
                .port(port)
                .build();
        redisServer.start();

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
