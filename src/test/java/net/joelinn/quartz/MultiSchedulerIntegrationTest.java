package net.joelinn.quartz;

import net.jodah.concurrentunit.Waiter;
import net.joelinn.junit.Retry;
import net.joelinn.junit.RetryRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.NameMatcher;
import org.quartz.simpl.SimpleInstanceIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.Properties;

import static junit.framework.TestCase.fail;
import static net.joelinn.quartz.TestUtils.createCronTrigger;
import static net.joelinn.quartz.TestUtils.createJob;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/**
 * @author Joe Linn
 *         12/10/2016
 */
public class MultiSchedulerIntegrationTest extends BaseIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(MultiSchedulerIntegrationTest.class);

    private static final String KEY_ID = "id";


    @Rule
    public RetryRule retryRule = new RetryRule();

    private Scheduler scheduler2;


    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        Properties props = schedulerConfig(HOST, port);
        props.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_NAME, "second");
        props.setProperty(StdSchedulerFactory.PROP_SCHED_BATCH_TIME_WINDOW, "500");
        props.setProperty(StdSchedulerFactory.PROP_SCHED_IDLE_WAIT_TIME, "1000");
        scheduler2 = new StdSchedulerFactory(props).getScheduler();
    }


    @After
    @Override
    public void tearDown() throws Exception {
        scheduler2.shutdown(true);
        super.tearDown();
    }


    @Override
    protected Properties schedulerConfig(String host, int port) {
        Properties config = super.schedulerConfig(host, port);
        config.setProperty("org.quartz.threadPool.threadCount", "2");
        config.setProperty("org.quartz.scheduler.instanceId", "AUTO");
        config.setProperty(StdSchedulerFactory.PROP_SCHED_INSTANCE_ID_GENERATOR_CLASS, SimpleInstanceIdGenerator.class.getName());
        return config;
    }

    @Test
    @Retry(5)
    public void testMultipleSchedulers() throws Exception {
        scheduler.setJobFactory(new RedisJobFactory());
        scheduler2.setJobFactory(new RedisJobFactory());

        assertThat(scheduler.getSchedulerInstanceId(), notNullValue());
        assertThat(scheduler2.getSchedulerInstanceId(), notNullValue());
        assertThat(scheduler.getSchedulerInstanceId(), not(equalTo(scheduler2.getSchedulerInstanceId())));

        JobDetail job = createJob(SchedulerIDCheckingJob.class, "testJob", "group");
        final String triggerName = "test-trigger";
        CronTrigger trigger = createCronTrigger(triggerName, "group", "* * * * * ?");

        Waiter waiter = new Waiter();
        scheduler.getListenerManager().addTriggerListener(new CompleteListener(waiter), NameMatcher.triggerNameEquals(triggerName));
        scheduler.scheduleJob(job, trigger);

        waiter.await(1500);

        try (Jedis jedis = jedisPool.getResource()) {
            assertThat(jedis.get(KEY_ID), equalTo(scheduler.getSchedulerInstanceId()));
        }

        scheduler.shutdown(true);
        waiter = new Waiter();
        scheduler2.getListenerManager().addTriggerListener(new CompleteListener(waiter), NameMatcher.triggerNameEquals(triggerName));
        if (log.isDebugEnabled()) {
            log.debug("Starting second scheduler.");
        }
        scheduler2.start();

        waiter.await(1500);

        try (Jedis jedis = jedisPool.getResource()) {
            assertThat(jedis.get(KEY_ID), equalTo(scheduler2.getSchedulerInstanceId()));
        }
    }


    @DisallowConcurrentExecution
    public static class SchedulerIDCheckingJob extends DataJob {
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            try {
                final String schedulerID = context.getScheduler().getSchedulerInstanceId();
                try (Jedis jedis = jedisPool.getResource()) {
                    if (jedis.setnx(KEY_ID, schedulerID) == 0) {
                        // we already have an ID stored
                        final String storedID = jedis.get(KEY_ID);
                        if (storedID.equals(schedulerID)) {
                            fail("The same schedule executed the job twice.");
                        } else {
                            jedis.set(KEY_ID, schedulerID);
                        }
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug("Completed job on behalf of scheduler {} at {}", schedulerID, System.currentTimeMillis());
                }
            } catch (SchedulerException e) {
                log.error("Unable to obtain scheduler instance ID.", e);
                fail("Failed to obtain scheduler instance ID.");
            }
        }
    }
}
