package net.joelinn.quartz;

import net.jodah.concurrentunit.Waiter;
import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.impl.matchers.NameMatcher;
import redis.clients.jedis.Jedis;

import java.util.Properties;

import static net.joelinn.quartz.TestUtils.createCronTrigger;
import static net.joelinn.quartz.TestUtils.createJob;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Joe Linn
 *         10/4/2016
 */
public class MultiThreadedIntegrationTest extends BaseIntegrationTest {

    @Override
    protected Properties schedulerConfig(String host, int port) {
        Properties config = super.schedulerConfig(host, port);
        config.setProperty("org.quartz.threadPool.threadCount", "2");
        return config;
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
    
    
    @Test
    public void testDisallowConcurrent() throws Exception {
        JobDetail job1 = createJob(SingletonSleepJob.class, "job1", "group1");
        CronTrigger trigger1 = createCronTrigger("trigger1", "group1", "* * * * * ?");
        CronTrigger trigger2 = createCronTrigger("trigger2", "group2", "* * * * * ?")
                .getTriggerBuilder()
                .forJob(job1)
                .build();

        Waiter waiter = new Waiter();
        scheduler.getListenerManager().addTriggerListener(new CompleteListener(waiter), NameMatcher.triggerNameEquals(trigger1.getKey().getName()));
        scheduler.scheduleJob(job1, trigger1);
        //scheduler.scheduleJob(trigger2);

        waiter.await(6000, 2);

        assertThat(SingletonSleepJob.concurrentExecutions.get(), equalTo(0));
    }
}
