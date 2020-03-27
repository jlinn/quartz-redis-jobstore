package net.joelinn.quartz.jobstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredResult;
import org.quartz.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.util.*;

/**
 * Joe Linn
 * 7/16/2014
 */
public abstract class AbstractRedisStorage<T extends JedisCommands> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractRedisStorage.class);

    protected static final String TRIGGER_CLASS = "trigger_class";
    protected static final String CALENDAR_CLASS = "calendar_class";
    protected static final String CALENDAR_JSON = "calendar_json";

    /**
     * The name of the trigger hash property which stores the trigger's next fire time
     */
    protected static final String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";
    /**
     * Name of trigger hash property which stores the trigger's last fire time
     */
    protected static final String TRIGGER_PREVIOUS_FIRE_TIME = "previousFireTime";

    protected final RedisJobStoreSchema redisSchema;

    protected final ObjectMapper mapper;

    protected final SchedulerSignaler signaler;

    protected final String schedulerInstanceId;

    protected final int lockTimeout;

    protected final int TRIGGER_LOCK_TIMEOUT = 10 * 60 * 1000;  // 10 minutes in milliseconds

    protected int misfireThreshold = 60_000;

    protected long clusterCheckInterval = 4 * 60 * 1000;

    /**
     * The value of the currently held Redis lock (if any)
     */
    protected UUID lockValue;

    public AbstractRedisStorage(RedisJobStoreSchema redisSchema, ObjectMapper mapper, SchedulerSignaler signaler, String schedulerInstanceId, int lockTimeout) {
        this.signaler = signaler;
        this.schedulerInstanceId = schedulerInstanceId;
        this.redisSchema = redisSchema;
        this.mapper = mapper;
        this.lockTimeout = lockTimeout;
    }

    public AbstractRedisStorage setMisfireThreshold(int misfireThreshold) {
        this.misfireThreshold = misfireThreshold;
        return this;
    }

    public AbstractRedisStorage setClusterCheckInterval(long clusterCheckInterval) {
        this.clusterCheckInterval = clusterCheckInterval;
        return this;
    }

    /**
     * Attempt to acquire a lock
     * @return true if lock was successfully acquired; false otherwise
     */
    public boolean lock(T jedis){
        UUID lockId = UUID.randomUUID();
        final String setResponse = jedis.set(redisSchema.lockKey(), lockId.toString(), SetParams.setParams().nx().px(lockTimeout));
        boolean lockAcquired = !isNullOrEmpty(setResponse) && setResponse.equals("OK");
        if(lockAcquired){
            // save the random value used to lock so that we can successfully unlock later
            lockValue = lockId;
        }
        return lockAcquired;
    }

    /**
     * Attempt to acquire lock. If lock cannot be acquired, wait until lock is successfully acquired.
     * @param jedis a thread-safe Redis connection
     */
    public void waitForLock(T jedis){
        while(!lock(jedis)){
            try {
                logger.debug("Waiting for Redis lock.");
                Thread.sleep(randomInt(75, 125));
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for lock.", e);
            }
        }
    }

    /**
     * Attempt to remove lock
     * @return true if lock was successfully removed; false otherwise
     */
    public boolean unlock(T jedis){
        final String currentLock = jedis.get(redisSchema.lockKey());
        if(!isNullOrEmpty(currentLock) && UUID.fromString(currentLock).equals(lockValue)){
            // This is our lock.  We can remove it.
            jedis.del(redisSchema.lockKey());
            return true;
        }
        return false;
    }

    /**
     * Get a random integer within the specified bounds
     * @param min the minimum possible value
     * @param max the maximum possible value
     * @return a random integer between min and max, inclusive
     */
    protected int randomInt(final int min, final int max){
        return new Random().nextInt((max - min) + 1) + min;
    }

    /**
     * Clear (delete!) all scheduling data - all {@link Job}s, {@link Trigger}s {@link Calendar}s.
     * @param jedis a thread-safe Redis connection
     */
    public void clearAllSchedulingData(T jedis) throws JobPersistenceException, ClassNotFoundException {
        // delete triggers
        for (String jobHashKey : jedis.smembers(redisSchema.jobsSet())) {
            removeJob(redisSchema.jobKey(jobHashKey), jedis);
        }

        // delete remaining jobs
        for (String triggerHashKey : jedis.smembers(redisSchema.triggersSet())) {
            removeTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
        }

        // remove calendars
        for (String calendarHashKey : jedis.smembers(redisSchema.calendarsSet())) {
            removeCalendar(redisSchema.calendarName(calendarHashKey), jedis);
        }
    }

    /**
     * Store a job in Redis
     * @param jobDetail the {@link org.quartz.JobDetail} object to be stored
     * @param replaceExisting if true, any existing job with the same group and name as the given job will be overwritten
     * @param jedis a thread-safe Redis connection
     * @throws org.quartz.ObjectAlreadyExistsException
     */
    public abstract void storeJob(JobDetail jobDetail, boolean replaceExisting, T jedis) throws ObjectAlreadyExistsException;

    /**
     * Convert a {@link org.quartz.JobDataMap} to a {@link java.util.HashMap} with String keys and values
     * @param jobDataMap the job data map to be converted
     * @return a map with string keys and values
     */
    protected Map<String, String> getStringDataMap(final JobDataMap jobDataMap){
        Map<String, String> stringMap = new HashMap<>();
        for (Map.Entry<String, Object> entry : jobDataMap.entrySet()) {
            if(entry.getValue() != null){
                stringMap.put(entry.getKey(), entry.getValue().toString());
            }
        }
        return stringMap;
    }

    /**
     * Retrieve a job from redis
     * @param jobKey the job key detailing the identity of the job to be retrieved
     * @param jedis a thread-safe Redis connection
     * @return the {@link org.quartz.JobDetail} of the desired job
     * @throws JobPersistenceException if the desired job does not exist
     * @throws ClassNotFoundException
     */
    public JobDetail retrieveJob(JobKey jobKey, T jedis) throws JobPersistenceException, ClassNotFoundException{
        final String jobHashKey = redisSchema.jobHashKey(jobKey);
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobKey);

        final Map<String, String> jobDetailMap = jedis.hgetAll(jobHashKey);
        if(jobDetailMap == null || jobDetailMap.size() == 0){
            // desired job does not exist
            return null;
        }
        JobDetailImpl jobDetail = mapper.convertValue(jobDetailMap, JobDetailImpl.class);
        jobDetail.setKey(jobKey);

        final Map<String, String> jobData = jedis.hgetAll(jobDataMapHashKey);
        if(jobData != null && !jobData.isEmpty()){
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.putAll(jobData);
            jobDetail.setJobDataMap(jobDataMap);
        }

        return jobDetail;
    }


    /**
     * Remove the given job from Redis
     * @param jobKey the job to be removed
     * @param jedis a thread-safe Redis connection
     * @return true if the job was removed; false if it did not exist
     */
    public abstract boolean removeJob(JobKey jobKey, T jedis) throws JobPersistenceException;

    /**
     * Store a trigger in redis
     * @param trigger the trigger to be stored
     * @param replaceExisting true if an existing trigger with the same identity should be replaced
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException
     * @throws ObjectAlreadyExistsException
     */
    public abstract void storeTrigger(OperableTrigger trigger, boolean replaceExisting, T jedis) throws JobPersistenceException;

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the given key.
     * If the associated job is non-durable and has no triggers after the given trigger is removed, the job will be
     * removed, as well.
     * @param triggerKey the key of the trigger to be removed
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger was found and removed
     */
    public boolean removeTrigger(TriggerKey triggerKey, T jedis) throws JobPersistenceException, ClassNotFoundException {
        return removeTrigger(triggerKey, true, jedis);
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the given key.
     * @param triggerKey the key of the trigger to be removed
     * @param removeNonDurableJob if true, the job associated with the given trigger will be removed if it is non-durable
     *                            and has no other triggers
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger was found and removed
     */
    protected abstract boolean removeTrigger(TriggerKey triggerKey, boolean removeNonDurableJob, T jedis) throws JobPersistenceException, ClassNotFoundException;

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
     * given key, and store the new given one - which must be associated
     * with the same job.
     * @param triggerKey the key of the trigger to be replaced
     * @param newTrigger the replacement trigger
     * @param jedis a thread-safe Redis connection
     * @return true if the target trigger was found, removed, and replaced
     */
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger, T jedis) throws JobPersistenceException, ClassNotFoundException {
        OperableTrigger oldTrigger = retrieveTrigger(triggerKey, jedis);
        boolean found = oldTrigger != null;
        if(found){
            if(!oldTrigger.getJobKey().equals(newTrigger.getJobKey())){
                throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
            }
            removeTrigger(triggerKey, false, jedis);
            storeTrigger(newTrigger, false, jedis);
        }
        return found;
    }

    /**
     * Retrieve a trigger from Redis
     * @param triggerKey the trigger key
     * @param jedis a thread-safe Redis connection
     * @return the requested {@link org.quartz.spi.OperableTrigger} if it exists; null if it does not
     * @throws JobPersistenceException if the job associated with the retrieved trigger does not exist
     */
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey, T jedis) throws JobPersistenceException{
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Map<String, String> triggerMap = jedis.hgetAll(triggerHashKey);
        if(triggerMap == null || triggerMap.isEmpty()){
            logger.debug(String.format("No trigger exists for key %s", triggerHashKey));
            return null;
        }
        Class triggerClass;
        try {
            triggerClass = Class.forName(triggerMap.get(TRIGGER_CLASS));
        } catch (ClassNotFoundException e) {
            throw new JobPersistenceException(String.format("Could not find class %s for trigger.", triggerMap.get(TRIGGER_CLASS)), e);
        }
        triggerMap.remove(TRIGGER_CLASS);
        OperableTrigger operableTrigger = (OperableTrigger) mapper.convertValue(triggerMap, triggerClass);
        operableTrigger.setFireInstanceId(schedulerInstanceId + "-" + operableTrigger.getKey() + "-" + operableTrigger.getStartTime().getTime());
        final Map<String, String> jobData = jedis.hgetAll(redisSchema.triggerDataMapHashKey(triggerKey));
        if (jobData != null && !jobData.isEmpty()){
            JobDataMap jobDataMap = new JobDataMap();
            jobDataMap.putAll(jobData);
            operableTrigger.setJobDataMap(jobDataMap);
        }
        return operableTrigger;
    }

    /**
     * Retrieve triggers associated with the given job
     * @param jobKey the job for which to retrieve triggers
     * @param jedis a thread-safe Redis connection
     * @return a list of triggers associated with the given job
     */
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey, T jedis) throws JobPersistenceException {
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(jobKey);
        final Set<String> triggerHashKeys = jedis.smembers(jobTriggerSetKey);
        List<OperableTrigger> triggers = new ArrayList<>();
        for (String triggerHashKey : triggerHashKeys) {
            triggers.add(retrieveTrigger(redisSchema.triggerKey(triggerHashKey), jedis));
        }
        return  triggers;
    }


    /**
     * Unsets the state of the given trigger key by removing the trigger from all trigger state sets.
     * @param triggerHashKey the redis key of the desired trigger hash
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger was removed, false if the trigger was stateless
     * @throws org.quartz.JobPersistenceException if the unset operation failed
     */
    public abstract boolean unsetTriggerState(final String triggerHashKey, T jedis) throws JobPersistenceException;

    /**
     * Set a trigger state by adding the trigger to the relevant sorted set, using its next fire time as the score.
     * @param state the new state to be set
     * @param score the trigger's next fire time
     * @param triggerHashKey the trigger hash key
     * @param jedis a thread-safe Redis connection
     * @return true if set, false if the trigger was already a member of the given state's sorted set and its score was updated
     * @throws JobPersistenceException if the set operation fails
     */
    public boolean setTriggerState(final RedisTriggerState state, final double score, final String triggerHashKey, T jedis) throws JobPersistenceException{
        boolean success = false;
        if(state != null){
            unsetTriggerState(triggerHashKey, jedis);
            success = jedis.zadd(redisSchema.triggerStateKey(state), score, triggerHashKey) == 1;
        }
        return success;
    }

    /**
     * Check if the job identified by the given key exists in storage
     * @param jobKey the key of the desired job
     * @param jedis a thread-safe Redis connection
     * @return true if the job exists; false otherwise
     */
    public boolean checkExists(JobKey jobKey, T jedis){
        return jedis.exists(redisSchema.jobHashKey(jobKey));
    }

    /**
     * Check if the trigger identified by the given key exists
     * @param triggerKey the key of the desired trigger
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger exists; false otherwise
     */
    public boolean checkExists(TriggerKey triggerKey, T jedis){
        return jedis.exists(redisSchema.triggerHashKey(triggerKey));
    }


    /**
     * Store a {@link org.quartz.Calendar}
     * @param name the name of the calendar
     * @param calendar the calendar object to be stored
     * @param replaceExisting if true, any existing calendar with the same name will be overwritten
     * @param updateTriggers if true, any existing triggers associated with the calendar will be updated
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException
     */
    public abstract void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers, T jedis) throws JobPersistenceException;

    /**
     * Remove (delete) the <code>{@link org.quartz.Calendar}</code> with the given name.
     * @param calendarName the name of the calendar to be removed
     * @param jedis a thread-safe Redis connection
     * @return true if a calendar with the given name was found and removed
     */
    public abstract boolean removeCalendar(String calendarName, T jedis) throws JobPersistenceException;

    /**
     * Retrieve a calendar
     * @param name the name of the calendar to be retrieved
     * @param jedis a thread-safe Redis connection
     * @return the desired calendar if it exists; null otherwise
     */
    public Calendar retrieveCalendar(String name, T jedis) throws JobPersistenceException{
        final String calendarHashKey = redisSchema.calendarHashKey(name);
        Calendar calendar;
        try{
            final Map<String, String> calendarMap = jedis.hgetAll(calendarHashKey);
            if(calendarMap == null || calendarMap.isEmpty()){
                return null;
            }
            final Class<?> calendarClass = Class.forName(calendarMap.get(CALENDAR_CLASS));
            calendar = (Calendar) mapper.readValue(calendarMap.get(CALENDAR_JSON), calendarClass);
        } catch (ClassNotFoundException e) {
            logger.error("Class not found for calendar " + name);
            throw new JobPersistenceException(e.getMessage(), e);
        } catch (IOException e) {
            logger.error("Unable to deserialize calendar json for calendar " + name);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        return  calendar;
    }

    /**
     * Get the number of stored jobs
     * @param jedis a thread-safe Redis connection
     * @return the number of jobs currently persisted in the jobstore
     */
    public int getNumberOfJobs(T jedis){
        return jedis.scard(redisSchema.jobsSet()).intValue();
    }

    /**
     * Get the number of stored triggers
     * @param jedis a thread-safe Redis connection
     * @return the number of triggers currently persisted in the jobstore
     */
    public int getNumberOfTriggers(T jedis){
        return jedis.scard(redisSchema.triggersSet()).intValue();
    }

    /**
     * Get the number of stored calendars
     * @param jedis a thread-safe Redis connection
     * @return the number of calendars currently persisted in the jobstore
     */
    public int getNumberOfCalendars(T jedis){
        return jedis.scard(redisSchema.calendarsSet()).intValue();
    }

    /**
     * Get the keys of all of the <code>{@link org.quartz.Job}</code> s that have the given group name.
     * @param matcher the matcher with which to compare group names
     * @param jedis a thread-safe Redis connection
     * @return the set of all JobKeys which have the given group name
     */
    public abstract Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher, T jedis);

    /**
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code> s that have the given group name.
     * @param matcher the matcher with which to compare group names
     * @param jedis a thread-safe Redis connection
     * @return the set of all TriggerKeys which have the given group name
     */
    public abstract Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher, T jedis);

    /**
     * Get the names of all of the <code>{@link org.quartz.Job}</code> groups.
     * @param jedis a thread-safe Redis connection
     * @return the names of all of the job groups or an empty list if no job groups exist
     */
    public List<String> getJobGroupNames(T jedis){
        final Set<String> groupsSet = jedis.smembers(redisSchema.jobGroupsSet());
        List<String> groups = new ArrayList<>(groupsSet.size());
        for (String group : groupsSet) {
            groups.add(redisSchema.jobGroup(group));
        }
        return groups;
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code> groups.
     * @param jedis a thread-safe Redis connection
     * @return the names of all trigger groups or an empty list if no trigger groups exist
     */
    public List<String> getTriggerGroupNames(T jedis){
        final Set<String> groupsSet = jedis.smembers(redisSchema.triggerGroupsSet());
        List<String> groups = new ArrayList<>(groupsSet.size());
        for (String group : groupsSet) {
            groups.add(redisSchema.triggerGroup(group));
        }
        return groups;
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Calendar}</code> s in the <code>JobStore</code>.
     * @param jedis a thread-safe Redis connection
     * @return the names of all calendars or an empty list if no calendars exist
     */
    public List<String> getCalendarNames(T jedis){
        final Set<String> calendarsSet = jedis.smembers(redisSchema.calendarsSet());
        List<String> calendars = new ArrayList<>(calendarsSet.size());
        for (String group : calendarsSet) {
            calendars.add(redisSchema.calendarName(group));
        }
        return calendars;
    }

    /**
     * Get the current state of the identified <code>{@link org.quartz.Trigger}</code>.
     * @param triggerKey the key of the desired trigger
     * @param jedis a thread-safe Redis connection
     * @return the state of the trigger
     */
    public abstract Trigger.TriggerState getTriggerState(TriggerKey triggerKey, T jedis);

    /**
     * Pause the trigger with the given key
     * @param triggerKey the key of the trigger to be paused
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException if the desired trigger does not exist
     */
    public abstract void pauseTrigger(TriggerKey triggerKey, T jedis) throws JobPersistenceException;


    /**
     * Pause all of the <code>{@link org.quartz.Trigger}s</code> in the given group.
     * @param matcher matcher for the trigger groups to be paused
     * @param jedis a thread-safe Redis connection
     * @return a collection of names of trigger groups which were matched and paused
     * @throws JobPersistenceException
     */
    public abstract Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher, T jedis) throws JobPersistenceException;

    /**
     * Pause a job by pausing all of its triggers
     * @param jobKey the key of the job to be paused
     * @param jedis a thread-safe Redis connection
     */
    public void pauseJob(JobKey jobKey, T jedis) throws JobPersistenceException {
        for (OperableTrigger trigger : getTriggersForJob(jobKey, jedis)) {
            pauseTrigger(trigger.getKey(), jedis);
        }
    }

    /**
     * Pause all of the <code>{@link org.quartz.Job}s</code> in the given group - by pausing all of their
     * <code>Trigger</code>s.
     * @param groupMatcher the mather which will determine which job group should be paused
     * @param jedis a thread-safe Redis connection
     * @return a collection of names of job groups which have been paused
     * @throws JobPersistenceException
     */
    public abstract Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher, T jedis) throws JobPersistenceException;

    /**
     * Resume (un-pause) a {@link org.quartz.Trigger}
     * @param triggerKey the key of the trigger to be resumed
     * @param jedis a thread-safe Redis connection
     */
    public abstract void resumeTrigger(TriggerKey triggerKey, T jedis) throws JobPersistenceException;

    /**
     * Determine whether or not the given trigger has misfired.
     * If so, notify the {@link org.quartz.spi.SchedulerSignaler} and update the trigger.
     * @param trigger the trigger to check for misfire
     * @param jedis a thread-safe Redis connection
     * @return false if the trigger has misfired; true otherwise
     * @throws JobPersistenceException
     */
    protected boolean applyMisfire(OperableTrigger trigger, T jedis) throws JobPersistenceException {
        long misfireTime = System.currentTimeMillis();
        if(misfireThreshold > 0){
            misfireTime -= misfireThreshold;
        }
        final Date nextFireTime = trigger.getNextFireTime();
        if(nextFireTime == null || nextFireTime.getTime() > misfireTime
                || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY){
            return false;
        }

        Calendar calendar = null;
        if(trigger.getCalendarName() != null){
            calendar = retrieveCalendar(trigger.getCalendarName(), jedis);
        }
        signaler.notifyTriggerListenersMisfired((OperableTrigger) trigger.clone());

        trigger.updateAfterMisfire(calendar);

        storeTrigger(trigger, true, jedis);
        if(trigger.getNextFireTime() == null){
            setTriggerState(RedisTriggerState.COMPLETED, (double) System.currentTimeMillis(), redisSchema.triggerHashKey(trigger.getKey()), jedis);
            signaler.notifySchedulerListenersFinalized(trigger);
        }
        else if(nextFireTime.equals(trigger.getNextFireTime())){
            return false;
        }
        return true;
    }

    /**
     * Resume (un-pause) all of the <code>{@link org.quartz.Trigger}s</code> in the given group.
     * @param matcher matcher for the trigger groups to be resumed
     * @param jedis a thread-safe Redis connection
     * @return the names of trigger groups which were resumed
     */
    public abstract Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher, T jedis) throws JobPersistenceException;

    /**
     * Retrieve all currently paused trigger groups
     * @param jedis a thread-safe Redis connection
     * @return a set containing the names of all currently paused trigger groups
     */
    public Set<String> getPausedTriggerGroups(T jedis){
        final Set<String> triggerGroupSetKeys = jedis.smembers(redisSchema.pausedTriggerGroupsSet());
        Set<String> names = new HashSet<>(triggerGroupSetKeys.size());
        for (String triggerGroupSetKey : triggerGroupSetKeys) {
            names.add(redisSchema.triggerGroup(triggerGroupSetKey));
        }
        return names;
    }

    /**
     * Resume (un-pause) the <code>{@link org.quartz.Job}</code> with the given key.
     * @param jobKey the key of the job to be resumed
     * @param jedis a thread-safe Redis connection
     */
    public void resumeJob(JobKey jobKey, T jedis) throws JobPersistenceException {
        for (OperableTrigger trigger : getTriggersForJob(jobKey, jedis)) {
            resumeTrigger(trigger.getKey(), jedis);
        }
    }

    /**
     * Resume (un-pause) all of the <code>{@link org.quartz.Job}s</code> in the given group.
     * @param matcher the matcher with which to compare job group names
     * @param jedis a thread-safe Redis connection
     * @return the set of job groups which were matched and resumed
     */
    public abstract Collection<String> resumeJobs(GroupMatcher<JobKey> matcher, T jedis) throws JobPersistenceException;

    /**
     * Pause all triggers - equivalent of calling <code>pauseTriggerGroup(group)</code> on every group.
     * @param jedis a thread-safe Redis connection
     */
    public void pauseAll(T jedis) throws JobPersistenceException {
        for (String triggerGroupSet : jedis.smembers(redisSchema.triggerGroupsSet())) {
            pauseTriggers(GroupMatcher.triggerGroupEquals(redisSchema.triggerGroup(triggerGroupSet)), jedis);
        }
    }

    /**
     * Resume (un-pause) all triggers - equivalent of calling <code>resumeTriggerGroup(group)</code> on every group.
     * @param jedis a thread-safe Redis connection
     */
    public void resumeAll(T jedis) throws JobPersistenceException {
        for (String triggerGroupSet : jedis.smembers(redisSchema.triggerGroupsSet())) {
            resumeTriggers(GroupMatcher.triggerGroupEquals(redisSchema.triggerGroup(triggerGroupSet)), jedis);
        }
    }

    /**
     * Determine if the instance with the given id has been active in the last 4 minutes
     * @param instanceId the instance to check
     * @param jedis a thread-safe Redis connection
     * @return true if the instance with the given id has been active in the last 4 minutes
     */
    protected boolean isActiveInstance(String instanceId, T jedis) {
        boolean isActive = ( System.currentTimeMillis() - getLastInstanceActiveTime(instanceId, jedis) < clusterCheckInterval);
        if (!isActive) {
            removeLastInstanceActiveTime(instanceId, jedis);
        }
        return isActive;
    }

    /**
     * Release triggers from the given current state to the new state if its locking scheduler has not
     * registered as alive in the last 10 minutes
     * @param currentState the current state of the orphaned trigger
     * @param newState the new state of the orphaned trigger
     * @param jedis a thread-safe Redis connection
     */
    protected void releaseOrphanedTriggers(RedisTriggerState currentState, RedisTriggerState newState, T jedis) throws JobPersistenceException {
        for (Tuple triggerTuple : jedis.zrangeWithScores(redisSchema.triggerStateKey(currentState), 0, -1)) {
            final String lockId = jedis.get(redisSchema.triggerLockKey(redisSchema.triggerKey(triggerTuple.getElement())));
            if(isNullOrEmpty(lockId) || !isActiveInstance(lockId, jedis)){
                // Lock key has expired. We can safely alter the trigger's state.
                logger.debug(String.format("Changing state of orphaned trigger %s from %s to %s.", triggerTuple.getElement(), currentState, newState));
                setTriggerState(newState, triggerTuple.getScore(), triggerTuple.getElement(), jedis);
            }
        }
    }

    /**
     * Release triggers currently held by schedulers which have ceased to function
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException
     */
    protected void releaseTriggersCron(T jedis) throws JobPersistenceException {
        // has it been more than 10 minutes since we last released orphaned triggers
        // or is this the first check upon initialization
        if(isTriggerLockTimeoutExceeded(jedis) || !isActiveInstance(schedulerInstanceId, jedis)){
            releaseOrphanedTriggers(RedisTriggerState.ACQUIRED, RedisTriggerState.WAITING, jedis);
            releaseOrphanedTriggers(RedisTriggerState.BLOCKED, RedisTriggerState.WAITING, jedis);
            releaseOrphanedTriggers(RedisTriggerState.PAUSED_BLOCKED, RedisTriggerState.PAUSED, jedis);
            settLastTriggerReleaseTime(System.currentTimeMillis(), jedis);
        }
    }

    /**
     * Determine if the last trigger release time exceeds the trigger lock timeout.
     * @param jedis a thread-safe Redis connection
     * @return if the last trigger release time exceeds the trigger lock timeout.
     */
    protected boolean isTriggerLockTimeoutExceeded(T jedis) {
        return System.currentTimeMillis() - getLastTriggersReleaseTime(jedis) > TRIGGER_LOCK_TIMEOUT;
    }

    /**
     * Retrieve the last time (in milliseconds) that orphaned triggers were released
     * @param jedis a thread-safe Redis connection
     * @return a unix timestamp in milliseconds
     */
    protected long getLastTriggersReleaseTime(T jedis){
        final String lastReleaseTime = jedis.get(redisSchema.lastTriggerReleaseTime());
        if(lastReleaseTime == null){
            return 0;
        }
        return Long.parseLong(lastReleaseTime);
    }

    /**
     * Set the last time at which orphaned triggers were released
     * @param time a unix timestamp in milliseconds
     * @param jedis a thread-safe Redis connection
     */
    protected void settLastTriggerReleaseTime(long time, T jedis){
        jedis.set(redisSchema.lastTriggerReleaseTime(), Long.toString(time));
    }

    /**
     * Retrieve the last time (in milliseconds) that the instance was active
     * @param instanceId The instance to check
     * @param jedis a thread-safe Redis connection
     * @return a unix timestamp in milliseconds
     */
    protected long getLastInstanceActiveTime(String instanceId, T jedis){
        final String lastActiveTime = jedis.hget(redisSchema.lastInstanceActiveTime(), instanceId);
        if(lastActiveTime == null){
            return 0;
        }
        return Long.parseLong(lastActiveTime);
    }

    /**
     * Set the last time at which this instance was active
     * @param time a unix timestamp in milliseconds
     * @param jedis a thread-safe Redis connection
     */
    protected void setLastInstanceActiveTime(String instanceId, long time, T jedis){
        jedis.hset(redisSchema.lastInstanceActiveTime(), instanceId, Long.toString(time));
    }

    /**
     * Remove the given instance from the hash
     * @param instanceId The instance id to remove
     * @param jedis a thread-safe Redis connection
     */
    protected void removeLastInstanceActiveTime(String instanceId, T jedis){
        jedis.hdel(redisSchema.lastInstanceActiveTime(), instanceId);
    }

    /**
     * Determine if the given job is blocked by an active instance
     * @param jobHashKey the job in question
     * @param jedis a thread-safe Redis connection
     * @return true if the given job is blocked by an active instance
     */
    protected boolean isBlockedJob(String jobHashKey, T jedis) {
        JobKey jobKey = redisSchema.jobKey(jobHashKey);
        return jedis.sismember(redisSchema.blockedJobsSet(), jobHashKey) &&
                isActiveInstance(jedis.get(redisSchema.jobBlockedKey(jobKey)), jedis);
    }

    /**
     * Determine if the given job class disallows concurrent execution
     * @param jobClass the job class in question
     * @return true if concurrent execution is NOT allowed; false if concurrent execution IS allowed
     */
    protected boolean isJobConcurrentExecutionDisallowed(Class<? extends Job> jobClass){
        return ClassUtils.isAnnotationPresent(jobClass, DisallowConcurrentExecution.class);
    }

    /**
     * Lock the trigger with the given key to the current jobstore instance
     * @param triggerKey the key of the desired trigger
     * @param jedis a thread-safe Redis connection
     * @return true if lock was acquired successfully; false otherwise
     */
    protected boolean lockTrigger(TriggerKey triggerKey, T jedis){
        return jedis.set(redisSchema.triggerLockKey(triggerKey), schedulerInstanceId, SetParams.setParams().nx().px(TRIGGER_LOCK_TIMEOUT)).equals("OK");
    }

    /**
     * Get a handle to the next trigger to be fired, and mark it as 'reserved'
     * by the calling scheduler.
     *
     * @param noLaterThan If > 0, the JobStore should only return a Trigger
     *                    that will fire no later than the time represented in this value as
     *                    milliseconds.
     * @param maxCount  the maximum number of triggers to return
     * @param timeWindow    the time window within which the triggers must fire next
     * @param jedis a thread-safe Redis connection
     * @return the acquired triggers
     * @throws JobPersistenceException
     */
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow, T jedis) throws JobPersistenceException, ClassNotFoundException {
        releaseTriggersCron(jedis);
        setLastInstanceActiveTime(schedulerInstanceId, System.currentTimeMillis(), jedis);
        List<OperableTrigger> acquiredTriggers = new ArrayList<>();
        boolean retry;
        do{
            retry = false;
            Set<String> acquiredJobHashKeysForNoConcurrentExec = new HashSet<>();
            if (logger.isTraceEnabled()) {
                logger.trace("Current time is {}. Attempting to acquire triggers firing no later than {}", System.currentTimeMillis(), (noLaterThan + timeWindow));
            }
            for (Tuple triggerTuple : jedis.zrangeByScoreWithScores(redisSchema.triggerStateKey(RedisTriggerState.WAITING), 0, (double) (noLaterThan + timeWindow), 0, maxCount)) {
                OperableTrigger trigger = retrieveTrigger(redisSchema.triggerKey(triggerTuple.getElement()), jedis);

                // Trigger data of a waiting trigger not found -> clean up
                if (trigger == null) {
                    jedis.zrem(redisSchema.triggerStateKey(RedisTriggerState.WAITING), triggerTuple.getElement());
                    jedis.srem(redisSchema.triggersSet(), triggerTuple.getElement());
                    jedis.del(redisSchema.triggerDataMapHashKey(redisSchema.triggerKey(triggerTuple.getElement())));
                    continue;
                }

                if(applyMisfire(trigger, jedis)){
                    if (logger.isDebugEnabled()) {
                        logger.debug("misfired trigger: " + triggerTuple.getElement());
                    }
                    retry = true;
                    break;
                }
                if(trigger.getNextFireTime() == null){
                    // Trigger has no next fire time. Unset its WAITING state.
                    unsetTriggerState(triggerTuple.getElement(), jedis);
                    continue;
                }
                final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
                JobDetail job = retrieveJob(trigger.getJobKey(), jedis);
                if(job != null && isJobConcurrentExecutionDisallowed(job.getJobClass())){
                    if (logger.isTraceEnabled()) {
                        logger.trace("Attempting to acquire job " + job.getKey() + " with concurrent execution disallowed.");
                    }
                    if (acquiredJobHashKeysForNoConcurrentExec.contains(jobHashKey)) {
                        // a trigger is already acquired for this job
                        if (logger.isTraceEnabled()) {
                            logger.trace("Job " + job.getKey() + " with concurrent execution disallowed already acquired.");
                        }
                        continue;
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Job " + job.getKey() + " with concurrent execution disallowed not yet acquired. Acquiring.");
                        }
                        acquiredJobHashKeysForNoConcurrentExec.add(jobHashKey);
                    }
                }
                // acquire the trigger
                setTriggerState(RedisTriggerState.ACQUIRED, triggerTuple.getScore(), triggerTuple.getElement(), jedis);
                if (job != null && isJobConcurrentExecutionDisallowed(job.getJobClass())) {
                    // setting the trigger state above will have removed any lock which was present, so we need to lock the trigger, again
                    lockTrigger(trigger.getKey(), jedis);
                }
                acquiredTriggers.add(trigger);
                logger.debug(String.format("Trigger %s acquired", triggerTuple.getElement()));
            }
        }while (retry);
        return acquiredTriggers;
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler no longer plans to
     * fire the given <code>Trigger</code>, that it had previously acquired
     * (reserved).
     * @param trigger the trigger to be released
     * @param jedis a thread-safe Redis connection
     */
    public void releaseAcquiredTrigger(OperableTrigger trigger, T jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
        if(jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.ACQUIRED), triggerHashKey) != null){
            if(trigger.getNextFireTime() != null){
                setTriggerState(RedisTriggerState.WAITING, (double) trigger.getNextFireTime().getTime(), triggerHashKey, jedis);
            }
            else{
                unsetTriggerState(triggerHashKey, jedis);
            }
        }
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler is now firing the
     * given <code>Trigger</code> (executing its associated <code>Job</code>),
     * that it had previously acquired (reserved).
     * @param triggers a list of triggers
     * @param jedis a thread-safe Redis connection
     * @return may return null if all the triggers or their calendars no longer exist, or
     * if the trigger was not successfully put into the 'executing'
     * state.  Preference is to return an empty list if none of the triggers
     * could be fired.
     */
    public abstract List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers, T jedis) throws JobPersistenceException, ClassNotFoundException;

    protected boolean isPersistJobDataAfterExecution(Class<? extends Job> jobClass){
        return ClassUtils.isAnnotationPresent(jobClass, PersistJobDataAfterExecution.class);
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler has completed the
     * firing of the given <code>Trigger</code> (and the execution of its
     * associated <code>Job</code> completed, threw an exception, or was vetoed),
     * and that the <code>{@link org.quartz.JobDataMap}</code>
     * in the given <code>JobDetail</code> should be updated if the <code>Job</code>
     * is stateful.
     * @param trigger the trigger which was completed
     * @param jobDetail the job which was completed
     * @param triggerInstCode the status of the completed job
     * @param jedis a thread-safe Redis connection
     */
    public abstract void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction triggerInstCode, T jedis) throws JobPersistenceException, ClassNotFoundException;

    /**
     * Check if a string is null or empty
     * @param string a string to check
     * @return true if the string is null or empty; false otherwise
     */
    protected boolean isNullOrEmpty(String string){
        return string == null || string.length() == 0;
    }
}
