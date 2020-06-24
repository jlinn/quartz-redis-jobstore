package net.joelinn.quartz;

import com.google.common.base.Strings;
import net.jodah.concurrentunit.Waiter;
import net.joelinn.quartz.jobstore.RedisJobStore;
import org.junit.After;
import org.junit.Before;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.simpl.PropertySettingJobFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.Pool;
import redis.embedded.RedisServer;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static net.joelinn.quartz.TestUtils.getPort;

/**
 * @author Joe Linn
 *         12/4/2016
 */
public abstract class BaseIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(BaseIntegrationTest.class);

    protected RedisServer redisServer;
    protected Scheduler scheduler;
    protected Pool<Jedis> jedisPool;

    protected int port;
    protected static final String HOST = "localhost";


    @Before
    public void setUp() throws Exception {
        port = getPort();
        redisServer = RedisServer.builder()
                .port(port)
                .build();
        redisServer.start();

        jedisPool = new JedisPool(HOST, port);


        scheduler = new StdSchedulerFactory(schedulerConfig(HOST, port)).getScheduler();
        scheduler.start();
    }


    protected Properties schedulerConfig(String host, int port) {
        Properties config = new Properties();
        config.setProperty(StdSchedulerFactory.PROP_JOB_STORE_CLASS, RedisJobStore.class.getName());
        config.setProperty("org.quartz.jobStore.host", host);
        config.setProperty("org.quartz.jobStore.port", String.valueOf(port));
        config.setProperty("org.quartz.threadPool.threadCount", "1");
        config.setProperty("org.quartz.jobStore.misfireThreshold", "500");
        // config.setProperty(StdSchedulerFactory.PROP_SCHED_SKIP_UPDATE_CHECK, "true");
        return config;
    }


    @After
    public void tearDown() throws Exception {
        scheduler.shutdown(true);
        if (jedisPool != null) {
            jedisPool.close();
        }
        redisServer.stop();
    }


    public static class DataJob implements Job {
        protected Pool<Jedis> jedisPool;

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


    @DisallowConcurrentExecution
    public static class SingletonSleepJob extends SleepJob {
        public static final AtomicInteger currentlyExecuting = new AtomicInteger(0);
        public static final AtomicInteger concurrentExecutions = new AtomicInteger(0);

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            log.info("Starting job: " + context.getJobDetail().getKey() + " due to trigger " + context.getTrigger().getKey());
            if (currentlyExecuting.incrementAndGet() > 1) {
                log.error("Concurrent execution detected!!");
                concurrentExecutions.incrementAndGet();
                throw new JobExecutionException("Concurrent execution not allowed!");
            }
            try {
                Thread.sleep(1000);     // add some extra sleep time to ensure that concurrent execution will be attempted
            } catch (InterruptedException e) {
                throw new JobExecutionException("Interrupted while sleeping.", e);
            }
            super.execute(context);
            currentlyExecuting.decrementAndGet();
        }
    }


    protected class CompleteListener implements TriggerListener {
        private final Waiter waiter;

        protected CompleteListener(Waiter waiter) {
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


    protected class MisfireListener implements TriggerListener {
        private final Waiter waiter;

        protected MisfireListener(Waiter waiter) {
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


    protected class RedisJobFactory extends PropertySettingJobFactory {
        @Override
        protected void setBeanProps(Object obj, JobDataMap data) throws SchedulerException {
            data.put("jedisPool", jedisPool);
            super.setBeanProps(obj, data);
        }
    }
}
