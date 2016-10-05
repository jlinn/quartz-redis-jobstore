package net.joelinn.quartz;


import com.google.common.base.Joiner;
import net.joelinn.quartz.jobstore.RedisJobStore;
import net.joelinn.quartz.jobstore.RedisJobStoreSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SchedulerConfigException;
import org.quartz.spi.SchedulerSignaler;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;
import redis.embedded.RedisCluster;
import redis.embedded.util.JedisUtil;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static net.joelinn.quartz.TestUtils.getPort;
import static org.mockito.Mockito.mock;

public class RedisSentinelJobStoreTest extends BaseTest {

    private JedisSentinelPool jedisSentinelPool;
    private String joinedHosts;
    private RedisCluster redisCluster;

    @Before
    public void setUpRedis() throws IOException, SchedulerConfigException {
        final List<Integer> sentinels = Arrays.asList(getPort(), getPort());
        final List<Integer> group1 = Arrays.asList(getPort(), getPort());
        final List<Integer> group2 = Arrays.asList(getPort(), getPort());
        //creates a cluster with 3 sentinels, quorum size of 2 and 3 replication groups, each with one master and one slave
        redisCluster = RedisCluster.builder().sentinelPorts(sentinels).quorumSize(2)
            .serverPorts(group1).replicationGroup("master1", 1)
            .serverPorts(group2).replicationGroup("master2", 1)
            .ephemeralServers().replicationGroup("master3", 1)
            .build();
        redisCluster.start();


        Set<String> jedisSentinelHosts = JedisUtil.sentinelHosts(redisCluster);

        joinedHosts = Joiner.on(",").join(jedisSentinelHosts);

        final short database = 1;
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setTestOnBorrow(true);
        jedisPoolConfig.setTestOnCreate(true);
        jedisPoolConfig.setTestOnReturn(true);
        jedisPoolConfig.setMaxWaitMillis(2000);
        jedisPoolConfig.setMaxTotal(20);
        jedisPool = new JedisSentinelPool("master1", jedisSentinelHosts, jedisPoolConfig);
        jobStore = new RedisJobStore();
        jobStore.setHost(joinedHosts);
        jobStore.setJedisPool(jedisSentinelPool);
        jobStore.setLockTimeout(2000);
        jobStore.setMasterGroupName("master1");
        jobStore.setRedisSentinel(true);
        jobStore.setInstanceId("testJobStore1");
        jobStore.setDatabase(database);
        mockScheduleSignaler = mock(SchedulerSignaler.class);
        jobStore.initialize(null, mockScheduleSignaler);
        schema = new RedisJobStoreSchema();

        jedis = jedisPool.getResource();
        jedis.flushDB();

    }

    @After
    public void tearDownRedis() throws InterruptedException {
        if (jedis != null) {
            jedis.close();
        }
        if (jedisPool != null) {
            jedisPool.close();
        }
        redisCluster.stop();
    }

    @Test
    public void redisSentinelJobStoreWithScheduler() throws Exception {
        Properties quartzProperties = new Properties();
        quartzProperties.setProperty("org.quartz.scheduler.instanceName", "testScheduler");
        quartzProperties.setProperty("org.quartz.threadPool.threadCount", "3");
        quartzProperties.setProperty("org.quartz.jobStore.class", RedisJobStore.class.getName());
        quartzProperties.setProperty("org.quartz.jobStore.host", joinedHosts);
        quartzProperties.setProperty("org.quartz.jobStore.redisSentinel", String.valueOf(true));
        quartzProperties.setProperty("org.quartz.jobStore.masterGroupName", "master1");
        quartzProperties.setProperty("org.quartz.jobStore.lockTimeout", "2000");
        quartzProperties.setProperty("org.quartz.jobStore.database", "1");
        testJobStore(quartzProperties);
    }
}
