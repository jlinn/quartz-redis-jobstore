package net.joelinn.quartz.jobstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.JobDetailImpl;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.*;
import org.quartz.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Tuple;

import java.io.IOException;
import java.util.*;

/**
 * Joe Linn
 * 7/16/2014
 */
public class RedisStorage {
    private static final Logger logger = LoggerFactory.getLogger(RedisStorage.class);

    protected static final String TRIGGER_CLASS = "trigger_class";
    protected static final String CALENDAR_CLASS = "calendar_class";
    protected static final String CALENDAR_JSON = "calendar_json";

    /**
     * The name of the trigger hash property which stores the trigger's next fire time
     */
    protected static final String TRIGGER_NEXT_FIRE_TIME = "nextFireTime";

    protected final RedisJobStoreSchema redisSchema;

    protected final ObjectMapper mapper;

    protected final SchedulerSignaler signaler;

    protected final String schedulerInstanceId;

    protected final int lockTimeout;

    protected final int TRIGGER_LOCK_TIMEOUT = 10 * 60 * 1000;  // 10 minutes in milliseconds

    protected int misfireThreshold = 60000;

    /**
     * The value of the currently held Redis lock (if any)
     */
    protected UUID lockValue;

    public RedisStorage(RedisJobStoreSchema redisSchema, ObjectMapper mapper, SchedulerSignaler signaler, String schedulerInstanceId, int lockTimeout) {
        this.signaler = signaler;
        this.schedulerInstanceId = schedulerInstanceId;
        this.redisSchema = redisSchema;
        this.mapper = mapper;
        this.lockTimeout = lockTimeout;
    }

    /**
     * Attempt to acquire a lock
     * @return true if lock was successfully acquired; false otherwise
     */
    public boolean lock(Jedis jedis){
        UUID lockId = UUID.randomUUID();
        final String setResponse = jedis.set(redisSchema.lockKey(), lockId.toString(), "NX", "PX", lockTimeout);
        boolean lockAcquired = !Strings.isNullOrEmpty(setResponse) && setResponse.equals("OK");
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
    public void waitForLock(Jedis jedis){
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
    public boolean unlock(Jedis jedis){
        final String currentLock = jedis.get(redisSchema.lockKey());
        if(!Strings.isNullOrEmpty(currentLock) && UUID.fromString(currentLock).equals(lockValue)){
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
    public void clearAllSchedulingData(Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
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
    @SuppressWarnings("unchecked")
    public void storeJob(JobDetail jobDetail, boolean replaceExisting, Jedis jedis) throws ObjectAlreadyExistsException {
        final String jobHashKey = redisSchema.jobHashKey(jobDetail.getKey());
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobDetail.getKey());
        final String jobGroupSetKey = redisSchema.jobGroupSetKey(jobDetail.getKey());

        if(!replaceExisting && jedis.exists(jobHashKey)){
            throw new ObjectAlreadyExistsException(jobDetail);
        }

        Pipeline pipe = jedis.pipelined();
        pipe.hmset(jobHashKey, (Map<String, String>) mapper.convertValue(jobDetail, new TypeReference<HashMap<String, String>>() {}));
        if(jobDetail.getJobDataMap() != null && !jobDetail.getJobDataMap().isEmpty()){
            pipe.hmset(jobDataMapHashKey, getStringDataMap(jobDetail.getJobDataMap()));
        }

        pipe.sadd(redisSchema.jobsSet(), jobHashKey);
        pipe.sadd(redisSchema.jobGroupsSet(), jobGroupSetKey);
        pipe.sadd(jobGroupSetKey, jobHashKey);
        pipe.sync();
    }

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
    public JobDetail retrieveJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException, ClassNotFoundException{
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
    public boolean removeJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException {
        final String jobHashKey = redisSchema.jobHashKey(jobKey);
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobKey);
        final String jobGroupSetKey = redisSchema.jobGroupSetKey(jobKey);
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(jobKey);

        Pipeline pipe = jedis.pipelined();
        // remove the job and any associated data
        Response<Long> delJobHashKeyResponse = pipe.del(jobHashKey);
        pipe.del(jobDataMapHashKey);
        // remove the job from the set of all jobs
        pipe.srem(redisSchema.jobsSet(), jobHashKey);
        // remove the job from its group
        pipe.srem(jobGroupSetKey, jobHashKey);
        // retrieve the keys for all triggers associated with this job, then delete that set
        Response<Set<String>> jobTriggerSetResponse = pipe.smembers(jobTriggerSetKey);
        pipe.del(jobTriggerSetKey);
        Response<Long> jobGroupSetSizeResponse = pipe.scard(jobGroupSetKey);
        pipe.sync();
        if(jobGroupSetSizeResponse.get() == 0){
            // The group now contains no jobs. Remove it from the set of all job groups.
            jedis.srem(redisSchema.jobGroupsSet(), jobGroupSetKey);
        }

        // remove all triggers associated with this job
        pipe = jedis.pipelined();
        for (String triggerHashKey : jobTriggerSetResponse.get()) {
            // get this trigger's TriggerKey
            final TriggerKey triggerKey = redisSchema.triggerKey(triggerHashKey);
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(triggerKey);
            unsetTriggerState(triggerHashKey, jedis);
            // remove the trigger from the set of all triggers
            pipe.srem(redisSchema.triggersSet(), triggerHashKey);
            // remove the trigger's group from the set of all trigger groups
            pipe.srem(redisSchema.triggerGroupsSet(), triggerGroupSetKey);
            // remove this trigger from its group
            pipe.srem(triggerGroupSetKey, triggerHashKey);
            // delete the trigger
            pipe.del(triggerHashKey);
        }
        pipe.sync();

        return delJobHashKeyResponse.get() == 1;
    }

    /**
     * Store a trigger in redis
     * @param trigger the trigger to be stored
     * @param replaceExisting true if an existing trigger with the same identity should be replaced
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException
     * @throws ObjectAlreadyExistsException
     */
    public void storeTrigger(OperableTrigger trigger, boolean replaceExisting, Jedis jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
        final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(trigger.getKey());
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(trigger.getJobKey());

        if(!(trigger instanceof SimpleTrigger) && !(trigger instanceof CronTrigger)){
            throw new UnsupportedOperationException("Only SimpleTrigger and CronTrigger are supported.");
        }
        final boolean exists = jedis.exists(triggerHashKey);
        if(exists && !replaceExisting){
            throw new ObjectAlreadyExistsException(trigger);
        }

        Map<String, String> triggerMap = mapper.convertValue(trigger, new TypeReference<HashMap<String, String>>() {});
        triggerMap.put(TRIGGER_CLASS, trigger.getClass().getName());

        Pipeline pipe = jedis.pipelined();
        pipe.hmset(triggerHashKey, triggerMap);
        pipe.sadd(redisSchema.triggersSet(), triggerHashKey);
        pipe.sadd(redisSchema.triggerGroupsSet(), triggerGroupSetKey);
        pipe.sadd(triggerGroupSetKey, triggerHashKey);
        pipe.sadd(jobTriggerSetKey, triggerHashKey);
        if(trigger.getCalendarName() != null && !trigger.getCalendarName().isEmpty()){
            final String calendarTriggersSetKey = redisSchema.calendarTriggersSetKey(trigger.getCalendarName());
            pipe.sadd(calendarTriggersSetKey, triggerHashKey);
        }
        pipe.sync();

        if(exists){
            // We're overwriting a previously stored instance of this trigger, so clear any existing trigger state.
            unsetTriggerState(triggerHashKey, jedis);
        }

        pipe = jedis.pipelined();
        Response<Boolean> triggerPausedResponse = pipe.sismember(redisSchema.pausedTriggerGroupsSet(), triggerGroupSetKey);
        Response<Boolean> jobPausedResponse = pipe.sismember(redisSchema.pausedJobGroupsSet(), redisSchema.jobGroupSetKey(trigger.getJobKey()));
        pipe.sync();
        if(triggerPausedResponse.get() || jobPausedResponse.get()){
            final long nextFireTime = trigger.getNextFireTime() != null ? trigger.getNextFireTime().getTime() : -1;
            final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
            if(jedis.sismember(redisSchema.blockedJobsSet(), jobHashKey)){
                setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double) nextFireTime, triggerHashKey, jedis);
            }
            else{
                setTriggerState(RedisTriggerState.PAUSED, (double) nextFireTime, triggerHashKey, jedis);
            }
        }
        else if(trigger.getNextFireTime() != null){
            setTriggerState(RedisTriggerState.WAITING, (double) trigger.getNextFireTime().getTime(), triggerHashKey, jedis);
        }
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the given key.
     * If the associated job is non-durable and has no triggers after the given trigger is removed, the job will be
     * removed, as well.
     * @param triggerKey the key of the trigger to be removed
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger was found and removed
     */
    public boolean removeTrigger(TriggerKey triggerKey, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
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
    protected boolean removeTrigger(TriggerKey triggerKey, boolean removeNonDurableJob, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(triggerKey);

        if(!jedis.exists(triggerHashKey)){
            return false;
        }

        OperableTrigger trigger = retrieveTrigger(triggerKey, jedis);

        final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(trigger.getJobKey());

        Pipeline pipe = jedis.pipelined();
        // remove the trigger from the set of all triggers
        pipe.srem(redisSchema.triggersSet(), triggerHashKey);
        // remove the trigger from its trigger group set
        pipe.srem(triggerGroupSetKey, triggerHashKey);
        // remove the trigger from the associated job's trigger set
        pipe.srem(jobTriggerSetKey, triggerHashKey);
        pipe.sync();

        if(jedis.scard(triggerGroupSetKey) == 0){
            // The trigger group set is empty. Remove the trigger group from the set of trigger groups.
            jedis.srem(redisSchema.triggerGroupsSet(), triggerGroupSetKey);
        }

        if(removeNonDurableJob){
            pipe = jedis.pipelined();
            Response<Long> jobTriggerSetKeySizeResponse = pipe.scard(jobTriggerSetKey);
            Response<Boolean> jobExistsResponse = pipe.exists(jobHashKey);
            pipe.sync();
            if(jobTriggerSetKeySizeResponse.get() == 0 && jobExistsResponse.get()){
                JobDetail job = retrieveJob(trigger.getJobKey(), jedis);
                if(!job.isDurable()){
                    // Job is not durable and has no remaining triggers. Delete it.
                    removeJob(job.getKey(), jedis);
                    signaler.notifySchedulerListenersJobDeleted(job.getKey());
                }
            }
        }

        if(Strings.isNullOrEmpty(trigger.getCalendarName())){
            jedis.srem(redisSchema.calendarTriggersSetKey(trigger.getCalendarName()), triggerHashKey);
        }
        unsetTriggerState(triggerHashKey, jedis);
        jedis.del(triggerHashKey);
        return true;
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the
     * given key, and store the new given one - which must be associated
     * with the same job.
     * @param triggerKey the key of the trigger to be replaced
     * @param newTrigger the replacement trigger
     * @param jedis a thread-safe Redis connection
     * @return true if the target trigger was found, removed, and replaced
     */
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
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
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey, Jedis jedis) throws JobPersistenceException{
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
        return (OperableTrigger) mapper.convertValue(triggerMap, triggerClass);
    }

    /**
     * Retrieve triggers associated with the given job
     * @param jobKey the job for which to retrieve triggers
     * @param jedis a thread-safe Redis connection
     * @return a list of triggers associated with the given job
     */
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException {
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
    public boolean unsetTriggerState(final String triggerHashKey, Jedis jedis) throws JobPersistenceException {
        boolean removed = false;
        Pipeline pipe = jedis.pipelined();
        List<Response<Long>> responses = new ArrayList<>(RedisTriggerState.values().length);
        for (RedisTriggerState state : RedisTriggerState.values()) {
            responses.add(pipe.zrem(redisSchema.triggerStateKey(state), triggerHashKey));
        }
        pipe.sync();
        for (Response<Long> response : responses) {
            removed = response.get() == 1;
            if(removed){
                jedis.del(redisSchema.triggerLockKey(redisSchema.triggerKey(triggerHashKey)));
                break;
            }
        }
        return removed;
    }

    /**
     * Set a trigger state by adding the trigger to the relevant sorted set, using its next fire time as the score.
     * @param state the new state to be set
     * @param score the trigger's next fire time
     * @param triggerHashKey the trigger hash key
     * @param jedis a thread-safe Redis connection
     * @return true if set, false if the trigger was already a member of the given state's sorted set and its score was updated
     * @throws JobPersistenceException if the set operation fails
     */
    public boolean setTriggerState(final RedisTriggerState state, final double score, final String triggerHashKey, Jedis jedis) throws JobPersistenceException{
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
    public boolean checkExists(JobKey jobKey, Jedis jedis){
        return jedis.exists(redisSchema.jobHashKey(jobKey));
    }

    /**
     * Check if the trigger identified by the given key exists
     * @param triggerKey the key of the desired trigger
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger exists; false otherwise
     */
    public boolean checkExists(TriggerKey triggerKey, Jedis jedis){
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
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers, Jedis jedis) throws JobPersistenceException{
        final String calendarHashKey = redisSchema.calendarHashKey(name);
        if(!replaceExisting && jedis.exists(calendarHashKey)){
            throw new ObjectAlreadyExistsException(String.format("Calendar with key %s already exists.", calendarHashKey));
        }
        Map<String, String> calendarMap = new HashMap<>();
        calendarMap.put(CALENDAR_CLASS, calendar.getClass().getName());
        try {
            calendarMap.put(CALENDAR_JSON, mapper.writeValueAsString(calendar));
        } catch (JsonProcessingException e) {
            throw new JobPersistenceException("Unable to serialize calendar.", e);
        }

        Pipeline pipe = jedis.pipelined();
        pipe.hmset(calendarHashKey, calendarMap);
        pipe.sadd(redisSchema.calendarsSet(), calendarHashKey);
        pipe.sync();

        if(updateTriggers){
            final String calendarTriggersSetKey = redisSchema.calendarTriggersSetKey(name);
            Set<String> triggerHashKeys = jedis.smembers(calendarTriggersSetKey);
            for (String triggerHashKey : triggerHashKeys) {
                OperableTrigger trigger = retrieveTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
                long removed = jedis.zrem(redisSchema.triggerStateKey(RedisTriggerState.WAITING), triggerHashKey);
                trigger.updateWithNewCalendar(calendar, misfireThreshold);
                if(removed == 1){
                    setTriggerState(RedisTriggerState.WAITING, (double) trigger.getNextFireTime().getTime(), triggerHashKey, jedis);
                }
            }
        }
    }

    /**
     * Remove (delete) the <code>{@link org.quartz.Calendar}</code> with the given name.
     * @param calendarName the name of the calendar to be removed
     * @param jedis a thread-safe Redis connection
     * @return true if a calendar with the given name was found and removed
     */
    public boolean removeCalendar(String calendarName, Jedis jedis) throws JobPersistenceException {
        final String calendarTriggersSetKey = redisSchema.calendarTriggersSetKey(calendarName);

        if(jedis.scard(calendarTriggersSetKey) > 0){
            throw new JobPersistenceException(String.format("There are triggers pointing to calendar %s, so it cannot be removed.", calendarName));
        }
        final String calendarHashKey = redisSchema.calendarHashKey(calendarName);
        Pipeline pipe = jedis.pipelined();
        Response<Long> deleteResponse = pipe.del(calendarHashKey);
        pipe.srem(redisSchema.calendarsSet(), calendarHashKey);
        pipe.sync();

        return deleteResponse.get() == 1;
    }

    /**
     * Retrieve a calendar
     * @param name the name of the calendar to be retrieved
     * @param jedis a thread-safe Redis connection
     * @return the desired calendar if it exists; null otherwise
     */
    public Calendar retrieveCalendar(String name, Jedis jedis) throws JobPersistenceException{
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
    public int getNumberOfJobs(Jedis jedis){
        return jedis.scard(redisSchema.jobsSet()).intValue();
    }

    /**
     * Get the number of stored triggers
     * @param jedis a thread-safe Redis connection
     * @return the number of triggers currently persisted in the jobstore
     */
    public int getNumberOfTriggers(Jedis jedis){
        return jedis.scard(redisSchema.triggersSet()).intValue();
    }

    /**
     * Get the number of stored calendars
     * @param jedis a thread-safe Redis connection
     * @return the number of calendars currently persisted in the jobstore
     */
    public int getNumberOfCalendars(Jedis jedis){
        return jedis.scard(redisSchema.calendarsSet()).intValue();
    }

    /**
     * Get the keys of all of the <code>{@link org.quartz.Job}</code> s that have the given group name.
     * @param matcher the matcher with which to compare group names
     * @param jedis a thread-safe Redis connection
     * @return the set of all JobKeys which have the given group name
     */
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher, Jedis jedis){
        Set<JobKey> jobKeys = new HashSet<>();
        if(matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS){
            final String jobGroupSetKey = redisSchema.jobGroupSetKey(new JobKey("", matcher.getCompareToValue()));
            final Set<String> jobs = jedis.smembers(jobGroupSetKey);
            if(jobs != null){
                for (final String job : jobs) {
                    jobKeys.add(redisSchema.jobKey(job));
                }
            }
        }
        else{
            List<Response<Set<String>>> jobGroups = new ArrayList<>();
            Pipeline pipe = jedis.pipelined();
            for (final String jobGroupSetKey : jedis.smembers(redisSchema.jobGroupsSet())) {
                if(matcher.getCompareWithOperator().evaluate(redisSchema.jobGroup(jobGroupSetKey), matcher.getCompareToValue())){
                    jobGroups.add(pipe.smembers(jobGroupSetKey));
                }
            }
            pipe.sync();
            for (Response<Set<String>> jobGroup : jobGroups) {
                if(jobGroup.get() != null){
                    for (final String job : jobGroup.get()) {
                        jobKeys.add(redisSchema.jobKey(job));
                    }
                }
            }
        }
        return jobKeys;
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Trigger}</code> s that have the given group name.
     * @param matcher the matcher with which to compare group names
     * @param jedis a thread-safe Redis connection
     * @return the set of all TriggerKeys which have the given group name
     */
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher, Jedis jedis){
        Set<TriggerKey> triggerKeys = new HashSet<>();
        if(matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS){
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(new TriggerKey("", matcher.getCompareToValue()));
            final Set<String> triggers = jedis.smembers(triggerGroupSetKey);
            if(triggers != null){
                for (final String trigger : triggers) {
                    triggerKeys.add(redisSchema.triggerKey(trigger));
                }
            }
        }
        else{
            List<Response<Set<String>>> triggerGroups = new ArrayList<>();
            Pipeline pipe = jedis.pipelined();
            for (final String triggerGroupSetKey : jedis.smembers(redisSchema.triggerGroupsSet())) {
                if(matcher.getCompareWithOperator().evaluate(redisSchema.triggerGroup(triggerGroupSetKey), matcher.getCompareToValue())){
                    triggerGroups.add(pipe.smembers(triggerGroupSetKey));
                }
            }
            pipe.sync();
            for (Response<Set<String>> triggerGroup : triggerGroups) {
                if(triggerGroup.get() != null){
                    for (final String trigger : triggerGroup.get()) {
                        triggerKeys.add(redisSchema.triggerKey(trigger));
                    }
                }
            }
        }
        return triggerKeys;
    }

    /**
     * Get the names of all of the <code>{@link org.quartz.Job}</code> groups.
     * @param jedis a thread-safe Redis connection
     * @return the names of all of the job groups or an empty list if no job groups exist
     */
    public List<String> getJobGroupNames(Jedis jedis){
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
    public List<String> getTriggerGroupNames(Jedis jedis){
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
    public List<String> getCalendarNames(Jedis jedis){
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
    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey, Jedis jedis){
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Pipeline pipe = jedis.pipelined();
        Map<RedisTriggerState, Response<Double>> scores = new HashMap<>(RedisTriggerState.values().length);
        for (RedisTriggerState redisTriggerState : RedisTriggerState.values()) {
            scores.put(redisTriggerState, pipe.zscore(redisSchema.triggerStateKey(redisTriggerState), triggerHashKey));
        }
        pipe.sync();
        for (Map.Entry<RedisTriggerState, Response<Double>> entry : scores.entrySet()) {
            if(entry.getValue().get() != null){
                return entry.getKey().getTriggerState();
            }
        }
        return Trigger.TriggerState.NONE;
    }

    /**
     * Pause the trigger with the given key
     * @param triggerKey the key of the trigger to be paused
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException if the desired trigger does not exist
     */
    public void pauseTrigger(TriggerKey triggerKey, Jedis jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Pipeline pipe = jedis.pipelined();
        Response<Boolean> exists = pipe.exists(triggerHashKey);
        Response<Double> completedScore = pipe.zscore(redisSchema.triggerStateKey(RedisTriggerState.COMPLETED), triggerHashKey);
        Response<String> nextFireTimeResponse = pipe.hget(triggerHashKey, TRIGGER_NEXT_FIRE_TIME);
        Response<Double> blockedScore = pipe.zscore(redisSchema.triggerStateKey(RedisTriggerState.BLOCKED), triggerHashKey);
        pipe.sync();

        if(!exists.get()){
            return;
        }
        if(completedScore.get() != null){
            // doesn't make sense to pause a completed trigger
            return;
        }

        final long nextFireTime = nextFireTimeResponse.get() == null
                || nextFireTimeResponse.get().isEmpty() ? -1 : Long.parseLong(nextFireTimeResponse.get());
        if(blockedScore.get() != null){
            setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double) nextFireTime, triggerHashKey, jedis);
        }
        else{
            setTriggerState(RedisTriggerState.PAUSED, (double) nextFireTime, triggerHashKey, jedis);
        }
    }

    /**
     * Pause all of the <code>{@link org.quartz.Trigger}s</code> in the given group.
     * @param matcher matcher for the trigger groups to be paused
     * @param jedis a thread-safe Redis connection
     * @return a collection of names of trigger groups which were matched and paused
     * @throws JobPersistenceException
     */
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher, Jedis jedis) throws JobPersistenceException {
        Set<String> pausedTriggerGroups = new HashSet<>();
        if(matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS){
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(new TriggerKey("", matcher.getCompareToValue()));
            final long addResult = jedis.sadd(redisSchema.pausedTriggerGroupsSet(), triggerGroupSetKey);
            if(addResult > 0){
                for (final String trigger : jedis.smembers(triggerGroupSetKey)) {
                    pauseTrigger(redisSchema.triggerKey(trigger), jedis);
                }
                pausedTriggerGroups.add(redisSchema.triggerGroup(triggerGroupSetKey));
            }
        }
        else{
            Map<String, Response<Set<String>>> triggerGroups = new HashMap<>();
            Pipeline pipe = jedis.pipelined();
            for (final String triggerGroupSetKey : jedis.smembers(redisSchema.triggerGroupsSet())) {
                if(matcher.getCompareWithOperator().evaluate(redisSchema.triggerGroup(triggerGroupSetKey), matcher.getCompareToValue())){
                    triggerGroups.put(triggerGroupSetKey, pipe.smembers(triggerGroupSetKey));
                }
            }
            pipe.sync();
            for (final Map.Entry<String, Response<Set<String>>> entry : triggerGroups.entrySet()) {
                if(jedis.sadd(redisSchema.pausedJobGroupsSet(), entry.getKey()) > 0){
                    // This trigger group was not paused. Pause it now.
                    pausedTriggerGroups.add(redisSchema.triggerGroup(entry.getKey()));
                    for (final String triggerHashKey : entry.getValue().get()) {
                        pauseTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
                    }
                }
            }
        }
        return pausedTriggerGroups;
    }

    /**
     * Pause a job by pausing all of its triggers
     * @param jobKey the key of the job to be paused
     * @param jedis a thread-safe Redis connection
     */
    public void pauseJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException {
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
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher, Jedis jedis) throws JobPersistenceException {
        Set<String> pausedJobGroups = new HashSet<>();
        if(groupMatcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS){
            final String jobGroupSetKey = redisSchema.jobGroupSetKey(new JobKey("", groupMatcher.getCompareToValue()));
            if(jedis.sadd(redisSchema.pausedJobGroupsSet(), jobGroupSetKey) > 0){
                pausedJobGroups.add(redisSchema.jobGroup(jobGroupSetKey));
                for (String job : jedis.smembers(jobGroupSetKey)) {
                    pauseJob(redisSchema.jobKey(job), jedis);
                }
            }
        }
        else{
            Map<String, Response<Set<String>>> jobGroups = new HashMap<>();
            Pipeline pipe = jedis.pipelined();
            for (final String jobGroupSetKey : jedis.smembers(redisSchema.jobGroupsSet())) {
                if(groupMatcher.getCompareWithOperator().evaluate(redisSchema.jobGroup(jobGroupSetKey), groupMatcher.getCompareToValue())){
                    jobGroups.put(jobGroupSetKey, pipe.smembers(jobGroupSetKey));
                }
            }
            pipe.sync();
            for (final Map.Entry<String, Response<Set<String>>> entry : jobGroups.entrySet()) {
                if(jedis.sadd(redisSchema.pausedJobGroupsSet(), entry.getKey()) > 0){
                    // This job group was not already paused. Pause it now.
                    pausedJobGroups.add(redisSchema.jobGroup(entry.getKey()));
                    for (final String jobHashKey : entry.getValue().get()) {
                        pauseJob(redisSchema.jobKey(jobHashKey), jedis);
                    }
                }
            }
        }
        return pausedJobGroups;
    }

    /**
     * Resume (un-pause) a {@link org.quartz.Trigger}
     * @param triggerKey the key of the trigger to be resumed
     * @param jedis a thread-safe Redis connection
     */
    public void resumeTrigger(TriggerKey triggerKey, Jedis jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Pipeline pipe = jedis.pipelined();
        Response<Boolean> exists = pipe.sismember(redisSchema.triggersSet(), triggerHashKey);
        Response<Double> isPaused = pipe.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED), triggerHashKey);
        Response<Double> isPausedBlocked = pipe.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED_BLOCKED), triggerHashKey);
        pipe.sync();

        if(!exists.get()){
            // Trigger does not exist.  Nothing to do.
            return;
        }
        if(isPaused.get() == null && isPausedBlocked.get() == null){
            // Trigger is not paused. Nothing to do.
            return;
        }
        OperableTrigger trigger = retrieveTrigger(triggerKey, jedis);
        final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
        final Date nextFireTime = trigger.getNextFireTime();

        if(nextFireTime != null){
            if(jedis.sismember(redisSchema.blockedJobsSet(), jobHashKey)){
                setTriggerState(RedisTriggerState.BLOCKED, (double) nextFireTime.getTime(), triggerHashKey, jedis);
            }
            else{
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime.getTime(), triggerHashKey, jedis);
            }
        }
        applyMisfire(trigger, jedis);
    }

    /**
     * Determine whether or not the given trigger has misfired.
     * If so, notify the {@link org.quartz.spi.SchedulerSignaler} and update the trigger.
     * @param trigger the trigger to check for misfire
     * @param jedis a thread-safe Redis connection
     * @return false if the trigger has misfired; true otherwise
     * @throws JobPersistenceException
     */
    protected boolean applyMisfire(OperableTrigger trigger, Jedis jedis) throws JobPersistenceException {
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
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher, Jedis jedis) throws JobPersistenceException {
        Set<String> resumedTriggerGroups = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(new TriggerKey("", matcher.getCompareToValue()));
            Pipeline pipe = jedis.pipelined();
            pipe.srem(redisSchema.pausedJobGroupsSet(), triggerGroupSetKey);
            Response<Set<String>> triggerHashKeysResponse = pipe.smembers(triggerGroupSetKey);
            pipe.sync();
            for (String triggerHashKey : triggerHashKeysResponse.get()) {
                OperableTrigger trigger = retrieveTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
                resumeTrigger(trigger.getKey(), jedis);
                resumedTriggerGroups.add(trigger.getKey().getGroup());
            }
        }
        else {
            for (final String triggerGroupSetKey : jedis.smembers(redisSchema.triggerGroupsSet())) {
                if(matcher.getCompareWithOperator().evaluate(redisSchema.triggerGroup(triggerGroupSetKey), matcher.getCompareToValue())){
                    resumedTriggerGroups.addAll(resumeTriggers(GroupMatcher.triggerGroupEquals(redisSchema.triggerGroup(triggerGroupSetKey)), jedis));
                }
            }
        }
        return resumedTriggerGroups;
    }

    /**
     * Retrieve all currently paused trigger groups
     * @param jedis a thread-safe Redis connection
     * @return a set containing the names of all currently paused trigger groups
     */
    public Set<String> getPausedTriggerGroups(Jedis jedis){
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
    public void resumeJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException {
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
    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher, Jedis jedis) throws JobPersistenceException {
        Set<String> resumedJobGroups = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String jobGroupSetKey = redisSchema.jobGroupSetKey(new JobKey("", matcher.getCompareToValue()));
            Pipeline pipe = jedis.pipelined();
            Response<Long> unpauseResponse = pipe.srem(redisSchema.pausedJobGroupsSet(), jobGroupSetKey);
            Response<Set<String>> jobsResponse = pipe.smembers(jobGroupSetKey);
            pipe.sync();
            if(unpauseResponse.get() > 0){
                resumedJobGroups.add(redisSchema.jobGroup(jobGroupSetKey));
            }
            for (String job : jobsResponse.get()) {
                resumeJob(redisSchema.jobKey(job), jedis);
            }
        }
        else{
            for (final String jobGroupSetKey : jedis.smembers(redisSchema.jobGroupsSet())) {
                if(matcher.getCompareWithOperator().evaluate(redisSchema.jobGroup(jobGroupSetKey), matcher.getCompareToValue())){
                    resumedJobGroups.addAll(resumeJobs(GroupMatcher.jobGroupEquals(redisSchema.jobGroup(jobGroupSetKey)), jedis));
                }
            }
        }
        return resumedJobGroups;
    }

    /**
     * Pause all triggers - equivalent of calling <code>pauseTriggerGroup(group)</code> on every group.
     * @param jedis a thread-safe Redis connection
     */
    public void pauseAll(Jedis jedis) throws JobPersistenceException {
        for (String triggerGroupSet : jedis.smembers(redisSchema.triggerGroupsSet())) {
            pauseTriggers(GroupMatcher.triggerGroupEquals(redisSchema.triggerGroup(triggerGroupSet)), jedis);
        }
    }

    /**
     * Resume (un-pause) all triggers - equivalent of calling <code>resumeTriggerGroup(group)</code> on every group.
     * @param jedis a thread-safe Redis connection
     */
    public void resumeAll(Jedis jedis) throws JobPersistenceException {
        for (String triggerGroupSet : jedis.smembers(redisSchema.triggerGroupsSet())) {
            resumeTriggers(GroupMatcher.triggerGroupEquals(redisSchema.triggerGroup(triggerGroupSet)), jedis);
        }
    }

    /**
     * Release triggers from the given current state to the new state if its locking scheduler has not
     * registered as alive in the last 10 minutes
     * @param currentState the current state of the orphaned trigger
     * @param newState the new state of the orphaned trigger
     * @param jedis a thread-safe Redis connection
     */
    protected void releaseOrphanedTriggers(RedisTriggerState currentState, RedisTriggerState newState, Jedis jedis) throws JobPersistenceException {
        for (Tuple triggerTuple : jedis.zrangeWithScores(redisSchema.triggerStateKey(currentState), 0, -1)) {
            final String lockId = jedis.get(redisSchema.triggerLockKey(redisSchema.triggerKey(triggerTuple.getElement())));
            if(Strings.isNullOrEmpty(lockId)){
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
    protected void releaseTriggersCron(Jedis jedis) throws JobPersistenceException {
        if(System.currentTimeMillis() - getLastTriggersReleaseTime(jedis) > TRIGGER_LOCK_TIMEOUT){
            // it has been more than 10 minutes since we last released orphaned triggers
            releaseOrphanedTriggers(RedisTriggerState.ACQUIRED, RedisTriggerState.WAITING, jedis);
            releaseOrphanedTriggers(RedisTriggerState.BLOCKED, RedisTriggerState.WAITING, jedis);
            releaseOrphanedTriggers(RedisTriggerState.PAUSED_BLOCKED, RedisTriggerState.PAUSED, jedis);
            settLastTriggerReleaseTime(System.currentTimeMillis(), jedis);
        }
    }

    /**
     * Retrieve the last time (in milliseconds) that orphaned triggers were released
     * @param jedis a thread-safe Redis connection
     * @return a unix timestamp in milliseconds
     */
    protected long getLastTriggersReleaseTime(Jedis jedis){
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
    protected void settLastTriggerReleaseTime(long time, Jedis jedis){
        jedis.set(redisSchema.lastTriggerReleaseTime(), Long.toString(time));
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
    protected boolean lockTrigger(TriggerKey triggerKey, Jedis jedis){
        return jedis.set(redisSchema.triggerLockKey(triggerKey), schedulerInstanceId, "NX", "PX", TRIGGER_LOCK_TIMEOUT).equals("OK");
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
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
        releaseTriggersCron(jedis);
        List<OperableTrigger> acquiredTriggers = new ArrayList<>();
        boolean retry;
        do{
            retry = false;
            Set<String> acquiredJobHashKeysForNoConcurrentExec = new HashSet<>();
            for (Tuple triggerTuple : jedis.zrangeByScoreWithScores(redisSchema.triggerStateKey(RedisTriggerState.WAITING), 0, (double) (noLaterThan + timeWindow), 0, maxCount)) {
                OperableTrigger trigger = retrieveTrigger(redisSchema.triggerKey(triggerTuple.getElement()), jedis);
                if(applyMisfire(trigger, jedis)){
                    logger.debug("misfired trigger: " + triggerTuple.getElement());
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
                    if(acquiredJobHashKeysForNoConcurrentExec.contains(jobHashKey)){
                        // a trigger is already acquired for this job
                        continue;
                    }
                    else{
                        acquiredJobHashKeysForNoConcurrentExec.add(jobHashKey);
                    }
                }
                // acquire the trigger
                lockTrigger(trigger.getKey(), jedis);
                setTriggerState(RedisTriggerState.ACQUIRED, triggerTuple.getScore(), triggerTuple.getElement(), jedis);
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
    public void releaseAcquiredTrigger(OperableTrigger trigger, Jedis jedis) throws JobPersistenceException {
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
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
        List<TriggerFiredResult> results = new ArrayList<>();
        for (OperableTrigger trigger : triggers) {
            final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
            logger.debug(String.format("Trigger %s fired.", triggerHashKey));
            Pipeline pipe = jedis.pipelined();
            Response<Boolean> triggerExistsResponse = pipe.exists(triggerHashKey);
            Response<Double> triggerAcquiredResponse = pipe.zscore(redisSchema.triggerStateKey(RedisTriggerState.ACQUIRED), triggerHashKey);
            pipe.sync();
            if(!triggerExistsResponse.get() || triggerAcquiredResponse.get() == null){
                // the trigger does not exist or the trigger is not acquired
                if(!triggerExistsResponse.get()){
                    logger.debug(String.format("Trigger %s does not exist.", triggerHashKey));
                }
                else{
                    logger.debug(String.format("Trigger %s was not acquired.", triggerHashKey));
                }
                continue;
            }
            Calendar calendar = null;
            final String calendarName = trigger.getCalendarName();
            if(calendarName != null){
                calendar = retrieveCalendar(calendarName, jedis);
                if(calendar == null){
                    continue;
                }
            }

            final Date previousFireTime = trigger.getPreviousFireTime();
            trigger.triggered(calendar);

            JobDetail job = retrieveJob(trigger.getJobKey(), jedis);
            TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(job, trigger, calendar, false, new Date(), previousFireTime, previousFireTime, trigger.getNextFireTime());

            // handling jobs for which concurrent execution is disallowed
            if(isJobConcurrentExecutionDisallowed(job.getJobClass())){
                final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
                final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(job.getKey());
                for (String nonConcurrentTriggerHashKey : jedis.smembers(jobTriggerSetKey)) {
                    Double score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.WAITING), nonConcurrentTriggerHashKey);
                    if(score != null){
                        setTriggerState(RedisTriggerState.BLOCKED, score, nonConcurrentTriggerHashKey, jedis);
                    }
                    else{
                        score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED), nonConcurrentTriggerHashKey);
                        if(score != null){
                            setTriggerState(RedisTriggerState.PAUSED_BLOCKED, score, nonConcurrentTriggerHashKey, jedis);
                        }
                    }
                }
                pipe = jedis.pipelined();
                pipe.set(redisSchema.jobBlockedKey(job.getKey()), schedulerInstanceId);
                pipe.sadd(redisSchema.blockedJobsSet(), jobHashKey);
                pipe.sync();
            }

            // release the fired trigger
            if(trigger.getNextFireTime() != null){
                final long nextFireTime = trigger.getNextFireTime().getTime();
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, Long.toString(nextFireTime));
                logger.debug(String.format("Releasing trigger %s with next fire time %s. Setting state to WAITING.", triggerHashKey, nextFireTime));
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime, triggerHashKey, jedis);
            }
            else{
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, "");
                unsetTriggerState(triggerHashKey, jedis);
            }

            results.add(new TriggerFiredResult(triggerFiredBundle));
        }
        return results;
    }

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
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction triggerInstCode, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
        final String jobHashKey = redisSchema.jobHashKey(jobDetail.getKey());
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobDetail.getKey());
        final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
        logger.debug(String.format("Job %s completed.", jobHashKey));
        if(jedis.exists(jobHashKey)) {
            // job was not deleted during execution
            Pipeline pipe;
            if (isPersistJobDataAfterExecution(jobDetail.getJobClass())) {
                // update the job data map
                JobDataMap jobDataMap = jobDetail.getJobDataMap();
                pipe = jedis.pipelined();
                pipe.del(jobDataMapHashKey);
                if (jobDataMap != null && !jobDataMap.isEmpty()) {
                    pipe.hmset(jobDataMapHashKey, getStringDataMap(jobDataMap));
                }
                pipe.sync();
            }
            if (isJobConcurrentExecutionDisallowed(jobDetail.getJobClass())) {
                // unblock the job
                pipe = jedis.pipelined();
                pipe.srem(redisSchema.blockedJobsSet(), jobHashKey);
                pipe.del(redisSchema.jobBlockedKey(jobDetail.getKey()));
                pipe.sync();

                final String jobTriggersSetKey = redisSchema.jobTriggersSetKey(jobDetail.getKey());
                for (String nonConcurrentTriggerHashKey : jedis.smembers(jobTriggersSetKey)) {
                    Double score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.BLOCKED), nonConcurrentTriggerHashKey);
                    if (score != null) {
                        setTriggerState(RedisTriggerState.WAITING, score, nonConcurrentTriggerHashKey, jedis);
                    }
                    else {
                        score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED_BLOCKED), nonConcurrentTriggerHashKey);
                        if (score != null) {
                            setTriggerState(RedisTriggerState.PAUSED, score, nonConcurrentTriggerHashKey, jedis);
                        }
                    }
                }
                signaler.signalSchedulingChange(0L);
            }
        }
        else{
            // unblock the job, even if it has been deleted
            jedis.srem(redisSchema.blockedJobsSet(), jobHashKey);
        }

        if(jedis.exists(triggerHashKey)){
            // trigger was not deleted during job execution
            if(triggerInstCode == Trigger.CompletedExecutionInstruction.DELETE_TRIGGER){
                if(trigger.getNextFireTime() == null){
                    // double-check for possible reschedule within job execution, which would cancel the need to delete
                    if(Strings.isNullOrEmpty(jedis.hget(triggerHashKey, TRIGGER_NEXT_FIRE_TIME))){
                        removeTrigger(trigger.getKey(), jedis);
                    }
                }
                else{
                    removeTrigger(trigger.getKey(), jedis);
                    signaler.signalSchedulingChange(0L);
                }
            }
            else if(triggerInstCode == Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE){
                setTriggerState(RedisTriggerState.COMPLETED, (double) System.currentTimeMillis(), triggerHashKey, jedis);
                signaler.signalSchedulingChange(0L);
            }
            else if(triggerInstCode == Trigger.CompletedExecutionInstruction.SET_TRIGGER_ERROR){
                logger.debug(String.format("Trigger %s set to ERROR state.", triggerHashKey));
                final double score = trigger.getNextFireTime() != null ? (double) trigger.getNextFireTime().getTime() : 0;
                setTriggerState(RedisTriggerState.ERROR, score, triggerHashKey, jedis);
                signaler.signalSchedulingChange(0L);
            }
            else if(triggerInstCode == Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR){
                final String jobTriggersSetKey = redisSchema.jobTriggersSetKey(jobDetail.getKey());
                for (String errorTriggerHashKey : jedis.smembers(jobTriggersSetKey)) {
                    final String nextFireTime = jedis.hget(errorTriggerHashKey, TRIGGER_NEXT_FIRE_TIME);
                    final double score = Strings.isNullOrEmpty(nextFireTime) ? 0 : Double.parseDouble(nextFireTime);
                    setTriggerState(RedisTriggerState.ERROR, score, errorTriggerHashKey, jedis);
                }
                signaler.signalSchedulingChange(0L);
            }
            else if(triggerInstCode == Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE){
                final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(jobDetail.getKey());
                for (String completedTriggerHashKey : jedis.smembers(jobTriggerSetKey)) {
                    setTriggerState(RedisTriggerState.COMPLETED, (double) System.currentTimeMillis(), completedTriggerHashKey, jedis);
                }
                signaler.signalSchedulingChange(0L);
            }
        }
    }
}
