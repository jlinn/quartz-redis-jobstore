package net.joelinn.quartz;

import net.jodah.concurrentunit.Waiter;
import org.junit.Test;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.impl.matchers.NameMatcher;

import static net.joelinn.quartz.TestUtils.createCronTrigger;
import static net.joelinn.quartz.TestUtils.createJob;

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
        waiter.await(2500);
    }
}
