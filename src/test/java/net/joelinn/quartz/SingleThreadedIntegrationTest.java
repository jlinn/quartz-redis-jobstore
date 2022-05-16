package net.joelinn.quartz;

import net.jodah.concurrentunit.Waiter;
import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.matchers.NameMatcher;

import static net.joelinn.quartz.TestUtils.createCronTrigger;
import static net.joelinn.quartz.TestUtils.createJob;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;

/**
 * @author Joe Linn
 *         12/4/2016
 */
public class SingleThreadedIntegrationTest extends BaseIntegrationTest {
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
        waiter.await(3000);
    }


    @Test
    public void testSingleExecution() throws Exception {
        final String jobName = "oneJob";
        JobDetail jobDetail = createJob(TestJob.class, jobName, "oneGroup");

        SimpleTrigger trigger = TriggerBuilder.newTrigger().withSchedule(simpleSchedule().withRepeatCount(0).withIntervalInMilliseconds(200)).build();

        Waiter waiter = new Waiter();
        scheduler.getListenerManager().addTriggerListener(new CompleteListener(waiter), NameMatcher.triggerNameEquals(trigger.getKey().getName()));

        scheduler.scheduleJob(jobDetail, trigger);

        waiter.await(3000);
    }
}
