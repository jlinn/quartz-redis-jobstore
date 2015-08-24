package net.joelinn.quartz;

import net.joelinn.quartz.jobstore.RedisJobStore;
import net.joelinn.quartz.jobstore.RedisJobStoreSchema;
import org.junit.After;
import org.junit.Before;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.calendar.WeeklyCalendar;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.spi.SchedulerSignaler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;

import static org.mockito.Mockito.mock;

/**
 * Joe Linn
 * 7/15/2014
 */
public abstract class BaseTest {
    private static final Logger logger = LoggerFactory.getLogger(BaseTest.class);

    protected RedisServer redisServer;

    protected JedisPool jedisPool;

    protected RedisJobStore jobStore;

    protected RedisJobStoreSchema schema;

    protected Jedis jedis;

    protected SchedulerSignaler mockScheduleSignaler;

    protected int port;

    protected String host = "localhost";

    @Before
    public void setUpRedis() throws IOException, SchedulerConfigException {
        port = getPort();
        logger.debug("Attempting to start embedded Redis server on port " + port);
        redisServer = RedisServer.builder()
                .port(port)
                .build();
        redisServer.start();
        final short database = 1;
        jedisPool = new JedisPool(new JedisPoolConfig(), host, port, Protocol.DEFAULT_TIMEOUT, null, database);

        jobStore = new RedisJobStore();
        jobStore.setHost(host);
        jobStore.setLockTimeout(2000);
        jobStore.setPort(port);
        jobStore.setInstanceId("testJobStore1");
        jobStore.setDatabase(database);
        mockScheduleSignaler = mock(SchedulerSignaler.class);
        jobStore.initialize(null, mockScheduleSignaler);
        schema = new RedisJobStoreSchema();

        jedis = jedisPool.getResource();
        jedis.flushDB();
    }


    protected int getPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }


    @After
    public void tearDownRedis() throws InterruptedException {
        jedis.close();
        jedisPool.destroy();
        redisServer.stop();
    }

    protected JobDetail getJobDetail(){
        return getJobDetail("testJob", "testGroup");
    }

    protected JobDetail getJobDetail(final String name, final String group){
        return JobBuilder.newJob(TestJob.class)
                .withIdentity(name, group)
                .usingJobData("timeout", 42)
                .withDescription("I am describing a job!")
                .build();
    }

    protected CronTriggerImpl getCronTrigger(){
        String cron = "0/5 * * * * ?";
        return (CronTriggerImpl) TriggerBuilder.newTrigger()
                .forJob("testJob", "testGroup")
                .withIdentity("testTrigger", "testTriggerGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .usingJobData("timeout", 5)
                .withDescription("A description!")
                .build();
    }

    protected CronTriggerImpl getCronTrigger(final String name, final String group, final JobKey jobKey){
        return getCronTrigger(name, group, jobKey, "0 * * * * ?");
    }

    protected CronTriggerImpl getCronTrigger(final String name, final String group, final JobKey jobKey, String cron){
        CronTriggerImpl trigger = (CronTriggerImpl) TriggerBuilder.newTrigger()
                .forJob(jobKey)
                .withIdentity(name, group)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .usingJobData("timeout", 5)
                .withDescription("A description!")
                .build();
        WeeklyCalendar calendar = new WeeklyCalendar();
        calendar.setDaysExcluded(new boolean[]{false, false, false, false, false, false, false, false, false});
        trigger.computeFirstFireTime(calendar);
        trigger.setCalendarName("testCalendar");
        return trigger;
    }

    protected Calendar getCalendar(){
        WeeklyCalendar calendar = new WeeklyCalendar();
        // exclude weekends
        calendar.setDayExcluded(1, true);
        calendar.setDayExcluded(7, true);
        calendar.setDescription("Only run on weekdays.");
        return calendar;
    }

    protected Map<JobDetail, Set<? extends Trigger>> getJobsAndTriggers(int jobGroups, int jobsPerGroup, int triggerGroupsPerJob, int triggersPerGroup){
        return getJobsAndTriggers(jobGroups, jobsPerGroup, triggerGroupsPerJob, triggersPerGroup, "0 * * * * ?");
    }

    protected Map<JobDetail, Set<? extends Trigger>> getJobsAndTriggers(int jobGroups, int jobsPerGroup, int triggerGroupsPerJob, int triggersPerGroup, String cron){
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        for(int jobGroup = 0; jobGroup < jobGroups; jobGroup++){
            String jobGroupName = String.format("jobGroup%s", jobGroup);
            for(int job = 0; job < jobsPerGroup; job++){
                String jobName = String.format("%sjob%s", jobGroupName, job);
                JobDetail jobDetail = getJobDetail(jobName, jobGroupName);
                Set<Trigger> triggerSet = new HashSet<>();
                for(int triggerGroup = 0; triggerGroup < triggerGroupsPerJob; triggerGroup++){
                    String triggerGroupName = String.format("%striggerGroup%s", jobName, triggerGroup);
                    for(int trigger = 0; trigger < triggersPerGroup; trigger++){
                        String triggerName = String.format("%strigger%s", triggerGroupName, trigger);
                        triggerSet.add(getCronTrigger(triggerName, triggerGroupName, jobDetail.getKey(), cron));
                    }
                }
                jobsAndTriggers.put(jobDetail, triggerSet);
            }
        }
        return jobsAndTriggers;
    }

    protected void storeJobAndTriggers(JobDetail job, Trigger... triggers) throws JobPersistenceException {
        Set<Trigger> triggersSet = new HashSet<>(triggers.length);
        Collections.addAll(triggersSet, triggers);
        Map<JobDetail, Set<? extends Trigger>> jobsAndTriggers = new HashMap<>();
        jobsAndTriggers.put(job, triggersSet);
        jobStore.storeJobsAndTriggers(jobsAndTriggers, false);
    }
}
