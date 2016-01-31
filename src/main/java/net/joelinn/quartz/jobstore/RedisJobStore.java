package net.joelinn.quartz.jobstore;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.joelinn.quartz.jobstore.mixin.CronTriggerMixin;
import net.joelinn.quartz.jobstore.mixin.JobDetailMixin;
import net.joelinn.quartz.jobstore.mixin.TriggerMixin;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.util.*;

/**
 * Joe Linn
 * 7/12/2014
 */
@SuppressWarnings("unchecked")
public class RedisJobStore implements JobStore {
    private static final Logger logger = LoggerFactory.getLogger(RedisJobStore.class);

    private Pool<Jedis> jedisPool;

    private JedisCluster jedisCluster;

    /**
     * Redis lock timeout in milliseconds
     */
    protected int lockTimeout = 30_000;

    /**
     * Redis host
     */
    protected String host;

    /**
     * Redis port
     */
    protected int port = 6379;

    /**
     * Redis database
     */
    protected short database = 0;

    /**
     * Redis sentinel master group name
     */
    protected String masterGroupName;

    /**
     * Redis key prefix
     */
    protected String keyPrefix = "";

    /**
     * Redis key delimiter
     */
    protected String keyDelimiter = ":";

    /**
     * Set to true if a {@link JedisCluster} should be used. {@link #host} will be split on ',', and the resulting
     * strings will be used as hostanmes for the cluster nodes.
     */
    private boolean redisCluster;

    /**
     * Set to true if a {@link JedisSentinelPool} should be used. {@link #host} will be split on ',', and the
     * resulting strings will be used as hostnames for the sentinel nodes. {@link #masterGroupName} will be
     * used as the master group name.
     */
    private boolean redisSentinel;

    protected String instanceId;

    protected AbstractRedisStorage storage;


    public RedisJobStore setJedisPool(Pool<Jedis> jedisPool) {
        this.jedisPool = jedisPool;
        return this;
    }


    public RedisJobStore setJedisCluster(JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
        return this;
    }

    /**
     * Called by the QuartzScheduler before the <code>JobStore</code> is
     * used, in order to give the it a chance to initialize.
     *
     * @param loadHelper class loader helper
     * @param signaler schedule signaler object
     */
    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        final RedisJobStoreSchema redisSchema = new RedisJobStoreSchema(keyPrefix, keyDelimiter);

        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixIn(CronTrigger.class, CronTriggerMixin.class);
        mapper.addMixIn(SimpleTrigger.class, TriggerMixin.class);
        mapper.addMixIn(JobDetail.class, JobDetailMixin.class);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        if (redisCluster && jedisCluster == null) {
            Set<HostAndPort> nodes = buildNodesSetFromHost();
            jedisCluster = new JedisCluster(nodes);
            storage = new RedisClusterStorage(redisSchema, mapper, signaler, instanceId, lockTimeout);
        } else if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setTestOnBorrow(true);
            if (redisSentinel) {
                Set<HostAndPort> nodes = buildNodesSetFromHost();
                Set<String> nodesAsStrings = new HashSet<>();
                for (HostAndPort node : nodes) {
                    nodesAsStrings.add(node.toString());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("Instantiating JedisSentinelPool using master " + masterGroupName + " and hosts " + host);
                }
                jedisPool = new JedisSentinelPool(masterGroupName, nodesAsStrings, jedisPoolConfig);
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Instantiating JedisPool using host " + host + " and port " + port);
                }
                jedisPool = new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null, database);
            }
            storage = new RedisStorage(redisSchema, mapper, signaler, instanceId, lockTimeout);
        }
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * the scheduler has started.
     */
    @Override
    public void schedulerStarted() throws SchedulerException {

    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * the scheduler has been paused.
     */
    @Override
    public void schedulerPaused() {
        // nothing to do
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * the scheduler has resumed after being paused.
     */
    @Override
    public void schedulerResumed() {
        // nothing to do
    }

    /**
     * Called by the QuartzScheduler to inform the <code>JobStore</code> that
     * it should free up all of it's resources because the scheduler is
     * shutting down.
     */
    @Override
    public void shutdown() {
        if(jedisPool != null){
            jedisPool.destroy();
        }
    }

    @Override
    public boolean supportsPersistence() {
        return true;
    }

    /**
     * How long (in milliseconds) the <code>JobStore</code> implementation
     * estimates that it will take to release a trigger and acquire a new one.
     */
    @Override
    public long getEstimatedTimeToReleaseAndAcquireTrigger() {
        return 100;
    }

    /**
     * Whether or not the <code>JobStore</code> implementation is redisCluster.
     */
    @Override
    public boolean isClustered() {
        return true;
    }

    /**
     * Store the given <code>{@link org.quartz.JobDetail}</code> and <code>{@link org.quartz.Trigger}</code>.
     *
     * @param newJob     The <code>JobDetail</code> to be stored.
     * @param newTrigger The <code>Trigger</code> to be stored.
     * @throws org.quartz.ObjectAlreadyExistsException if a <code>Job</code> with the same name/group already
     *                                                 exists.
     */
    @Override
    public void storeJobAndTrigger(final JobDetail newJob, final OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        try {
            doWithLock(new LockCallbackWithoutResult() {
                @Override
                public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                    storage.storeJob(newJob, false, jedis);
                    storage.storeTrigger(newTrigger, false, jedis);
                    return null;
                }
            });
        } catch (ObjectAlreadyExistsException e) {
            logger.info("Job and / or trigger already exist in storage.", e);
            throw e;
        } catch (Exception e) {
            logger.error("Could not store job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    /**
     * Store the given <code>{@link org.quartz.JobDetail}</code>.
     *
     * @param newJob          The <code>JobDetail</code> to be stored.
     * @param replaceExisting If <code>true</code>, any <code>Job</code> existing in the
     *                        <code>JobStore</code> with the same name & group should be
     *                        over-written.
     * @throws org.quartz.ObjectAlreadyExistsException if a <code>Job</code> with the same name/group already
     *                                                 exists, and replaceExisting is set to false.
     */
    @Override
    public void storeJob(final JobDetail newJob, final boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        try {
            doWithLock(new LockCallbackWithoutResult() {
                @Override
                public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                    storage.storeJob(newJob, replaceExisting, jedis);
                    return null;
                }
            });
        } catch (ObjectAlreadyExistsException e) {
            logger.info("Job hash already exists");
            throw e;
        } catch (Exception e) {
            logger.error("Could not store job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    @Override
    public void storeJobsAndTriggers(final Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, final boolean replace) throws ObjectAlreadyExistsException, JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                for (Map.Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
                    storage.storeJob(entry.getKey(), replace, jedis);
                    for (Trigger trigger : entry.getValue()) {
                        storage.storeTrigger((OperableTrigger) trigger, replace, jedis);
                    }
                }
                return null;
            }
        }, "Could not store jobs and triggers.");
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Job}</code> with the given
     * key, and any <code>{@link org.quartz.Trigger}</code> s that reference
     * it.
     * <p/>
     * <p>
     * If removal of the <code>Job</code> results in an empty group, the
     * group should be removed from the <code>JobStore</code>'s list of
     * known group names.
     * </p>
     *
     * @param jobKey the key of the job to be removed
     * @return <code>true</code> if a <code>Job</code> with the given name &
     * group was found and removed from the store.
     */
    @Override
    public boolean removeJob(final JobKey jobKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.removeJob(jobKey, jedis);
            }
        }, "Could not remove job.");
    }

    @Override
    public boolean removeJobs(final List<JobKey> jobKeys) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                boolean removed = jobKeys.size() > 0;
                for (JobKey jobKey : jobKeys) {
                    removed = storage.removeJob(jobKey, jedis) && removed;
                }
                return removed;
            }
        }, "Could not remove jobs.");
    }

    /**
     * Retrieve the <code>{@link org.quartz.JobDetail}</code> for the given
     * <code>{@link org.quartz.Job}</code>.
     *
     * @param jobKey the {@link org.quartz.JobKey} describing the desired job
     * @return The desired <code>Job</code>, or null if there is no match.
     */
    @Override
    public JobDetail retrieveJob(final JobKey jobKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<JobDetail>() {
            @Override
            public JobDetail doWithLock(JedisCommands jedis) throws JobPersistenceException {
                try {
                    return storage.retrieveJob(jobKey, jedis);
                } catch (ClassNotFoundException e) {
                    throw new JobPersistenceException("Error retrieving job: " + e.getMessage(), e);
                }
            }
        }, "Could not retrieve job.");
    }

    /**
     * Store the given <code>{@link org.quartz.Trigger}</code>.
     *
     * @param newTrigger      The <code>Trigger</code> to be stored.
     * @param replaceExisting If <code>true</code>, any <code>Trigger</code> existing in
     *                        the <code>JobStore</code> with the same name & group should
     *                        be over-written.
     * @throws org.quartz.ObjectAlreadyExistsException if a <code>Trigger</code> with the same name/group already
     *                                                 exists, and replaceExisting is set to false.
     * @see #pauseTriggers(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public void storeTrigger(final OperableTrigger newTrigger, final boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.storeTrigger(newTrigger, replaceExisting, jedis);
                return null;
            }
        }, "Could not store trigger.");
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
     * given key.
     * <p/>
     * <p>
     * If removal of the <code>Trigger</code> results in an empty group, the
     * group should be removed from the <code>JobStore</code>'s list of
     * known group names.
     * </p>
     * <p/>
     * <p>
     * If removal of the <code>Trigger</code> results in an 'orphaned' <code>Job</code>
     * that is not 'durable', then the <code>Job</code> should be deleted
     * also.
     * </p>
     *
     * @param triggerKey the key of the trigger to be removed
     * @return <code>true</code> if a <code>Trigger</code> with the given
     * name & group was found and removed from the store.
     */
    @Override
    public boolean removeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                try {
                    return storage.removeTrigger(triggerKey, jedis);
                } catch (ClassNotFoundException e) {
                    throw new JobPersistenceException("Error removing trigger: " + e.getMessage(), e);
                }
            }
        }, "Could not remove trigger.");
    }

    @Override
    public boolean removeTriggers(final List<TriggerKey> triggerKeys) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                boolean removed = triggerKeys.size() > 0;
                for (TriggerKey triggerKey : triggerKeys) {
                    try {
                        removed = storage.removeTrigger(triggerKey, jedis) && removed;
                    } catch (ClassNotFoundException e) {
                        throw new JobPersistenceException(e.getMessage(), e);
                    }
                }
                return removed;
            }
        }, "Could not remove trigger.");
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
     * given key, and store the new given one - which must be associated
     * with the same job.
     *
     * @param triggerKey the key of the trigger to be replaced
     * @param newTrigger The new <code>Trigger</code> to be stored.
     * @return <code>true</code> if a <code>Trigger</code> with the given
     * name & group was found and removed from the store.
     */
    @Override
    public boolean replaceTrigger(final TriggerKey triggerKey, final OperableTrigger newTrigger) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                try {
                    return storage.replaceTrigger(triggerKey, newTrigger, jedis);
                } catch (ClassNotFoundException e) {
                    throw new JobPersistenceException(e.getMessage(), e);
                }
            }
        }, "Could not remove trigger.");
    }

    /**
     * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
     *
     * @param triggerKey the key of the desired trigger
     * @return The desired <code>Trigger</code>, or null if there is no
     * match.
     */
    @Override
    public OperableTrigger retrieveTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<OperableTrigger>() {
            @Override
            public OperableTrigger doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.retrieveTrigger(triggerKey, jedis);
            }
        }, "Could not retrieve trigger.");
    }

    /**
     * Determine whether a {@link org.quartz.Job} with the given identifier already
     * exists within the scheduler.
     *
     * @param jobKey the identifier to check for
     * @return true if a Job exists with the given identifier
     * @throws org.quartz.JobPersistenceException
     */
    @Override
    public boolean checkExists(final JobKey jobKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.checkExists(jobKey, jedis);
            }
        }, "Could not check if job exists: " + jobKey);
    }

    /**
     * Determine whether a {@link org.quartz.Trigger} with the given identifier already
     * exists within the scheduler.
     *
     * @param triggerKey the identifier to check for
     * @return true if a Trigger exists with the given identifier
     * @throws org.quartz.JobPersistenceException
     */
    @Override
    public boolean checkExists(final TriggerKey triggerKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.checkExists(triggerKey, jedis);
            }
        }, "Could not check if trigger exists: " + triggerKey);
    }

    /**
     * Clear (delete!) all scheduling data - all {@link org.quartz.Job}s, {@link org.quartz.Trigger}s
     * {@link org.quartz.Calendar}s.
     *
     * @throws org.quartz.JobPersistenceException
     */
    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                try {
                    storage.clearAllSchedulingData(jedis);
                } catch (ClassNotFoundException e) {
                    throw new JobPersistenceException("Could not clear scheduling data.");
                }
                return null;
            }
        }, "Could not clear scheduling data.");
    }

    /**
     * Store the given <code>{@link org.quartz.Calendar}</code>.
     *
     * @param name The name of the calendar
     * @param calendar        The <code>Calendar</code> to be stored.
     * @param replaceExisting If <code>true</code>, any <code>Calendar</code> existing
     *                        in the <code>JobStore</code> with the same name & group
     *                        should be over-written.
     * @param updateTriggers  If <code>true</code>, any <code>Trigger</code>s existing
     *                        in the <code>JobStore</code> that reference an existing
     *                        Calendar with the same name with have their next fire time
     *                        re-computed with the new <code>Calendar</code>.
     * @throws org.quartz.ObjectAlreadyExistsException if a <code>Calendar</code> with the same name already
     *                                                 exists, and replaceExisting is set to false.
     */
    @Override
    public void storeCalendar(final String name, final Calendar calendar, final boolean replaceExisting, final boolean updateTriggers) throws ObjectAlreadyExistsException, JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.storeCalendar(name, calendar, replaceExisting, updateTriggers, jedis);
                return null;
            }
        }, "Could not store calendar.");
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Calendar}</code> with the
     * given name.
     * <p/>
     * <p>
     * If removal of the <code>Calendar</code> would result in
     * <code>Trigger</code>s pointing to non-existent calendars, then a
     * <code>JobPersistenceException</code> will be thrown.</p>
     * *
     *
     * @param calName The name of the <code>Calendar</code> to be removed.
     * @return <code>true</code> if a <code>Calendar</code> with the given name
     * was found and removed from the store.
     */
    @Override
    public boolean removeCalendar(final String calName) throws JobPersistenceException {
        return doWithLock(new LockCallback<Boolean>() {
            @Override
            public Boolean doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.removeCalendar(calName, jedis);
            }
        }, "Could not remove calendar.");
    }

    /**
     * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
     *
     * @param calName The name of the <code>Calendar</code> to be retrieved.
     * @return The desired <code>Calendar</code>, or null if there is no
     * match.
     */
    @Override
    public Calendar retrieveCalendar(final String calName) throws JobPersistenceException {
        return doWithLock(new LockCallback<Calendar>() {
            @Override
            public Calendar doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.retrieveCalendar(calName, jedis);
            }
        }, "Could not retrieve calendar.");
    }

    /**
     * Get the number of <code>{@link org.quartz.Job}</code> s that are
     * stored in the <code>JobsStore</code>.
     */
    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        return  doWithLock(new LockCallback<Integer>() {
            @Override
            public Integer doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getNumberOfJobs(jedis);
            }
        }, "Could not get number of jobs.");
    }

    /**
     * Get the number of <code>{@link org.quartz.Trigger}</code> s that are
     * stored in the <code>JobsStore</code>.
     */
    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        return doWithLock(new LockCallback<Integer>() {
            @Override
            public Integer doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getNumberOfTriggers(jedis);
            }
        }, "Could not get number of jobs.");
    }

    /**
     * Get the number of <code>{@link org.quartz.Calendar}</code> s that are
     * stored in the <code>JobsStore</code>.
     */
    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        return doWithLock(new LockCallback<Integer>() {
            @Override
            public Integer doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getNumberOfCalendars(jedis);
            }
        }, "Could not get number of jobs.");
    }

    /**
     * Get the keys of all of the <code>{@link org.quartz.Job}</code> s that
     * have the given group name.
     * <p/>
     * <p>
     * If there are no jobs in the given group name, the result should be
     * an empty collection (not <code>null</code>).
     * </p>
     *
     * @param matcher the matcher for job key comparison
     */
    @Override
    public Set<JobKey> getJobKeys(final GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return doWithLock(new LockCallback<Set<JobKey>>() {
            @Override
            public Set<JobKey> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getJobKeys(matcher, jedis);
            }
        }, "Could not retrieve JobKeys.");
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code> s
     * that have the given group name.
     * <p/>
     * <p>
     * If there are no triggers in the given group name, the result should be a
     * zero-length array (not <code>null</code>).
     * </p>
     *
     * @param matcher the matcher with which to compare trigger groups
     */
    @Override
    public Set<TriggerKey> getTriggerKeys(final GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return doWithLock(new LockCallback<Set<TriggerKey>>() {
            @Override
            public Set<TriggerKey> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getTriggerKeys(matcher, jedis);
            }
        }, "Could not retrieve TriggerKeys.");
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Job}</code>
     * groups.
     * <p/>
     * <p>
     * If there are no known group names, the result should be a zero-length
     * array (not <code>null</code>).
     * </p>
     */
    @Override
    public List<String> getJobGroupNames() throws JobPersistenceException {
        return doWithLock(new LockCallback<List<String>>() {
            @Override
            public List<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getJobGroupNames(jedis);
            }
        }, "Could not retrieve job group names.");
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code>
     * groups.
     * <p/>
     * <p>
     * If there are no known group names, the result should be a zero-length
     * array (not <code>null</code>).
     * </p>
     */
    @Override
    public List<String> getTriggerGroupNames() throws JobPersistenceException {
        return doWithLock(new LockCallback<List<String>>() {
            @Override
            public List<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getTriggerGroupNames(jedis);
            }
        }, "Could not retrieve trigger group names.");
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Calendar}</code> s
     * in the <code>JobStore</code>.
     * <p/>
     * <p>
     * If there are no Calendars in the given group name, the result should be
     * a zero-length array (not <code>null</code>).
     * </p>
     */
    @Override
    public List<String> getCalendarNames() throws JobPersistenceException {
        return doWithLock(new LockCallback<List<String>>() {
            @Override
            public List<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getCalendarNames(jedis);
            }
        }, "Could not retrieve calendar names.");
    }

    /**
     * Get all of the Triggers that are associated to the given Job.
     * <p/>
     * <p>
     * If there are no matches, a zero-length array should be returned.
     * </p>
     *
     * @param jobKey the key of the job for which to retrieve triggers
     */
    @Override
    public List<OperableTrigger> getTriggersForJob(final JobKey jobKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<List<OperableTrigger>>() {
            @Override
            public List<OperableTrigger> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getTriggersForJob(jobKey, jedis);
            }
        }, "Could not retrieve triggers for job.");
    }

    /**
     * Get the current state of the identified <code>{@link org.quartz.Trigger}</code>.
     *
     * @param triggerKey the key of the trigger for which to retrieve state
     * @see org.quartz.Trigger.TriggerState
     */
    @Override
    public Trigger.TriggerState getTriggerState(final TriggerKey triggerKey) throws JobPersistenceException {
        return doWithLock(new LockCallback<Trigger.TriggerState>() {
            @Override
            public Trigger.TriggerState doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getTriggerState(triggerKey, jedis);
            }
        }, "Could not retrieve trigger state.");
    }

    /**
     * Pause the <code>{@link org.quartz.Trigger}</code> with the given key.
     *
     * @param triggerKey the key for the trigger to be paused
     * @see #resumeTrigger(org.quartz.TriggerKey)
     */
    @Override
    public void pauseTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.pauseTrigger(triggerKey, jedis);
                return null;
            }
        }, "Could not pause trigger.");
    }

    /**
     * Pause all of the <code>{@link org.quartz.Trigger}s</code> in the
     * given group.
     * <p/>
     * <p/>
     * <p>
     * The JobStore should "remember" that the group is paused, and impose the
     * pause on any new triggers that are added to the group while the group is
     * paused.
     * </p>
     *
     * @param matcher a trigger group matcher
     */
    @Override
    public Collection<String> pauseTriggers(final GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return doWithLock(new LockCallback<Collection<String>>() {
            @Override
            public Collection<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.pauseTriggers(matcher, jedis);
            }
        }, "Could not pause triggers.");
    }

    /**
     * Pause the <code>{@link org.quartz.Job}</code> with the given name - by
     * pausing all of its current <code>Trigger</code>s.
     *
     * @param jobKey the key of the job to be paused
     * @see #resumeJob(org.quartz.JobKey)
     */
    @Override
    public void pauseJob(final JobKey jobKey) throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.pauseJob(jobKey, jedis);
                return null;
            }
        }, "Could not pause job.");
    }

    /**
     * Pause all of the <code>{@link org.quartz.Job}s</code> in the given
     * group - by pausing all of their <code>Trigger</code>s.
     * <p/>
     * <p>
     * The JobStore should "remember" that the group is paused, and impose the
     * pause on any new jobs that are added to the group while the group is
     * paused.
     * </p>
     *
     * @param groupMatcher the mather which will determine which job group should be paused
     * @see #resumeJobs(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Collection<String> pauseJobs(final GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        return doWithLock(new LockCallback<Collection<String>>() {
            @Override
            public Collection<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.pauseJobs(groupMatcher, jedis);
            }
        }, "Could not pause jobs.");
    }

    /**
     * Resume (un-pause) the <code>{@link org.quartz.Trigger}</code> with the
     * given key.
     * <p/>
     * <p>
     * If the <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @param triggerKey the key of the trigger to be resumed
     * @see #pauseTrigger(org.quartz.TriggerKey)
     */
    @Override
    public void resumeTrigger(final TriggerKey triggerKey) throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.resumeTrigger(triggerKey, jedis);
                return null;
            }
        }, "Could not resume trigger.");
    }

    /**
     * Resume (un-pause) all of the <code>{@link org.quartz.Trigger}s</code>
     * in the given group.
     * <p/>
     * <p>
     * If any <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @param matcher a trigger group matcher
     * @see #pauseTriggers(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Collection<String> resumeTriggers(final GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        return doWithLock(new LockCallback<Collection<String>>() {
            @Override
            public Collection<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.resumeTriggers(matcher, jedis);
            }
        }, "Could not resume trigger group(s).");
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        return doWithLock(new LockCallback<Set<String>>() {
            @Override
            public Set<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.getPausedTriggerGroups(jedis);
            }
        }, "Could not retrieve paused trigger groups.");
    }

    /**
     * Resume (un-pause) the <code>{@link org.quartz.Job}</code> with the
     * given key.
     * <p/>
     * <p>
     * If any of the <code>Job</code>'s<code>Trigger</code> s missed one
     * or more fire-times, then the <code>Trigger</code>'s misfire
     * instruction will be applied.
     * </p>
     *
     * @param jobKey the key of the job to be resumed
     * @see #pauseJob(org.quartz.JobKey)
     */
    @Override
    public void resumeJob(final JobKey jobKey) throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.resumeJob(jobKey, jedis);
                return null;
            }
        }, "Could not resume job.");
    }

    /**
     * Resume (un-pause) all of the <code>{@link org.quartz.Job}s</code> in
     * the given group.
     * <p/>
     * <p>
     * If any of the <code>Job</code> s had <code>Trigger</code> s that
     * missed one or more fire-times, then the <code>Trigger</code>'s
     * misfire instruction will be applied.
     * </p>
     *
     * @param matcher the matcher for job group name comparison
     * @see #pauseJobs(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public Collection<String> resumeJobs(final GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        return doWithLock(new LockCallback<Collection<String>>() {
            @Override
            public Collection<String> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                return storage.resumeJobs(matcher, jedis);
            }
        }, "Could not resume jobs.");
    }

    /**
     * Pause all triggers - equivalent of calling <code>pauseTriggerGroup(group)</code>
     * on every group.
     * <p/>
     * <p>
     * When <code>resumeAll()</code> is called (to un-pause), trigger misfire
     * instructions WILL be applied.
     * </p>
     *
     * @see #resumeAll()
     * @see #pauseTriggers(org.quartz.impl.matchers.GroupMatcher)
     */
    @Override
    public void pauseAll() throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.pauseAll(jedis);
                return null;
            }
        }, "Could not pause all triggers.");
    }

    /**
     * Resume (un-pause) all triggers - equivalent of calling <code>resumeTriggerGroup(group)</code>
     * on every group.
     * <p/>
     * <p>
     * If any <code>Trigger</code> missed one or more fire-times, then the
     * <code>Trigger</code>'s misfire instruction will be applied.
     * </p>
     *
     * @see #pauseAll()
     */
    @Override
    public void resumeAll() throws JobPersistenceException {
        doWithLock(new LockCallbackWithoutResult() {
            @Override
            public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                storage.resumeAll(jedis);
                return null;
            }
        }, "Could not resume all triggers.");
    }

    /**
     * Get a handle to the next trigger to be fired, and mark it as 'reserved'
     * by the calling scheduler.
     *
     * @param noLaterThan If > 0, the JobStore should only return a Trigger
     *                    that will fire no later than the time represented in this value as
     *                    milliseconds.
     * @param maxCount the maximum number of triggers to return
     * @param timeWindow  @see #releaseAcquiredTrigger(Trigger)
     */
    @Override
    public List<OperableTrigger> acquireNextTriggers(final long noLaterThan, final int maxCount, final long timeWindow) throws JobPersistenceException {
        return doWithLock(new LockCallback<List<OperableTrigger>>() {
            @Override
            public List<OperableTrigger> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                try {
                    return storage.acquireNextTriggers(noLaterThan, maxCount, timeWindow, jedis);
                } catch (ClassNotFoundException e) {
                    throw new JobPersistenceException(e.getMessage(), e);
                }
            }
        }, "Could not acquire next triggers.");
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler no longer plans to
     * fire the given <code>Trigger</code>, that it had previously acquired
     * (reserved).
     *
     * @param trigger the trigger to be released
     */
    @Override
    public void releaseAcquiredTrigger(OperableTrigger trigger) {
        try {
            doWithLock(new LockCallbackWithoutResult() {
                @Override
                public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                    return null;
                }
            }, "Could not release acquired trigger.");
        } catch (JobPersistenceException e) {
            logger.error("Could not release acquired trigger.", e);
        }
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler is now firing the
     * given <code>Trigger</code> (executing its associated <code>Job</code>),
     * that it had previously acquired (reserved).
     *
     * @param triggers a list of triggers which are being fired by the scheduler
     * @return may return null if all the triggers or their calendars no longer exist, or
     * if the trigger was not successfully put into the 'executing'
     * state.  Preference is to return an empty list if none of the triggers
     * could be fired.
     */
    @Override
    public List<TriggerFiredResult> triggersFired(final List<OperableTrigger> triggers) throws JobPersistenceException {
        return doWithLock(new LockCallback<List<TriggerFiredResult>>() {
            @Override
            public List<TriggerFiredResult> doWithLock(JedisCommands jedis) throws JobPersistenceException {
                try {
                    return storage.triggersFired(triggers, jedis);
                } catch (ClassNotFoundException e) {
                    throw new JobPersistenceException(e.getMessage(), e);
                }
            }
        }, "Could not set triggers as fired.");
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler has completed the
     * firing of the given <code>Trigger</code> (and the execution of its
     * associated <code>Job</code> completed, threw an exception, or was vetoed),
     * and that the <code>{@link org.quartz.JobDataMap}</code>
     * in the given <code>JobDetail</code> should be updated if the <code>Job</code>
     * is stateful.
     *
     * @param trigger the completetd trigger
     * @param jobDetail the completed job
     * @param triggerInstCode the trigger completion code
     */
    @Override
    public void triggeredJobComplete(final OperableTrigger trigger, final JobDetail jobDetail, final Trigger.CompletedExecutionInstruction triggerInstCode) {
        try {
            doWithLock(new LockCallbackWithoutResult() {
                @Override
                public Void doWithLock(JedisCommands jedis) throws JobPersistenceException {
                    try {
                        storage.triggeredJobComplete(trigger, jobDetail, triggerInstCode, jedis);
                    } catch (ClassNotFoundException e) {
                        logger.error("Could not handle job completion.", e);
                    }
                    return null;
                }
            });
        } catch (JobPersistenceException e) {
            logger.error("Could not handle job completion.", e);
        }
    }


    /**
     * Perform Redis operations while possessing lock
     * @param callback operation(s) to be performed during lock
     * @param <T> return type
     * @return response from callback, if any
     * @throws JobPersistenceException
     */
    private <T> T doWithLock(LockCallback<T> callback) throws JobPersistenceException {
        return doWithLock(callback, null);
    }


    /**
     * Perform a redis operation while lock is acquired
     * @param callback a callback containing the actions to perform during lock
     * @param errorMessage optional error message to include in exception should an error arise
     * @param <T> return class
     * @return the result of the actions performed while locked, if any
     * @throws JobPersistenceException
     */
    private <T> T doWithLock(LockCallback<T> callback, String errorMessage) throws JobPersistenceException {
        JedisCommands jedis = null;
        try {
            jedis = getResource();
            try {
                storage.waitForLock(jedis);
                return callback.doWithLock(jedis);
            } catch (ObjectAlreadyExistsException e) {
                throw e;
            } catch (Exception e) {
                if (errorMessage == null || errorMessage.isEmpty()) {
                    errorMessage = "Job storage error.";
                }
                throw new JobPersistenceException(errorMessage, e);
            } finally {
                storage.unlock(jedis);
            }
        } finally {
            if (jedis != null && jedis instanceof Jedis) {
                // only close if we're not using a JedisCluster instance
                ((Jedis) jedis).close();
            }
        }
    }


    private JedisCommands getResource() throws JobPersistenceException {
        if (jedisCluster != null) {
            return jedisCluster;
        } else {
            return jedisPool.getResource();
        }
    }


    private interface LockCallback<T> {
        T doWithLock(JedisCommands jedis) throws JobPersistenceException;
    }

    private abstract class LockCallbackWithoutResult implements LockCallback<Void> {}

    public void setLockTimeout(int lockTimeout) {
        this.lockTimeout = lockTimeout;
    }

    public void setLockTimeout(String lockTimeout){
        setLockTimeout(Integer.valueOf(lockTimeout));
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setPort(String port){
        setPort(Integer.valueOf(port));
    }

    public void setDatabase(int database){
        this.database = (short) database;
    }

    public void setDatabase(String database){
        setDatabase(Short.valueOf(database));
    }

    public void setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
    }

    public void setKeyDelimiter(String keyDelimiter) {
        this.keyDelimiter = keyDelimiter;
    }

    public void setRedisCluster(boolean clustered) {
        this.redisCluster = clustered;
    }

    public void setRedisSentinel(boolean sentinel) {
        this.redisSentinel = sentinel;
    }

    public void setMasterGroupName(String masterGroupName) {
        this.masterGroupName = masterGroupName;
    }

    /**
     * Inform the <code>JobStore</code> of the Scheduler instance's Id,
     * prior to initialize being invoked.
     *
     * @param schedInstId the instanceid for the current scheduler
     * @since 1.7
     */
    @Override
    public void setInstanceId(String schedInstId) {
        instanceId = schedInstId;
    }

    /**
     * Inform the <code>JobStore</code> of the Scheduler instance's name,
     * prior to initialize being invoked.
     *
     * @param schedName the name of the current scheduler
     * @since 1.7
     */
    @Override
    public void setInstanceName(String schedName) {
        // nothing to do
    }

    /**
     * Tells the JobStore the pool size used to execute jobs
     *
     * @param poolSize amount of threads allocated for job execution
     * @since 2.0
     */
    @Override
    public void setThreadPoolSize(int poolSize) {
        // nothing to do
    }

    private Set<HostAndPort> buildNodesSetFromHost() {
        Set<HostAndPort> nodes = new HashSet<>();
        for (String hostName : host.split(",")) {
            int hostPort = port;
            if (hostName.contains(":")) {
                String[] parts = hostName.split(":");
                hostName = parts[0];
                hostPort = Integer.valueOf(parts[1]);
            }
            nodes.add(new HostAndPort(hostName, hostPort));
        }
        return nodes;
    }

}
