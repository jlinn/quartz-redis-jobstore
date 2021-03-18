package net.joelinn.quartz.jobstore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.quartz.Calendar;
import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.*;

/**
 * Joe Linn
 * 8/22/2015
 */
public class RedisStorage extends AbstractRedisStorage<Jedis> {
    private static final Logger logger = LoggerFactory.getLogger(RedisStorage.class);

    public RedisStorage(RedisJobStoreSchema redisSchema, ObjectMapper mapper, SchedulerSignaler signaler, String schedulerInstanceId, int lockTimeout) {
        super(redisSchema, mapper, signaler, schedulerInstanceId, lockTimeout);
    }


    /**
     * Remove the given job from Redis
     * @param jobKey the job to be removed
     * @param jedis a thread-safe Redis connection
     * @return true if the job was removed; false if it did not exist
     */
    @Override
    public boolean removeJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException {
        final String jobHashKey = redisSchema.jobHashKey(jobKey);
        final String jobBlockedKey = redisSchema.jobBlockedKey(jobKey);
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobKey);
        final String jobGroupSetKey = redisSchema.jobGroupSetKey(jobKey);
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(jobKey);

        Pipeline pipe = jedis.pipelined();
        // remove the job and any associated data
        Response<Long> delJobHashKeyResponse = pipe.del(jobHashKey);
        // remove the blocked job key
        pipe.del(jobBlockedKey);
        // remove the job's data map
        pipe.del(jobDataMapHashKey);
        // remove the job from the set of all jobs
        pipe.srem(redisSchema.jobsSet(), jobHashKey);
        // remove the job from the set of blocked jobs
        pipe.srem(redisSchema.blockedJobsSet(), jobHashKey);
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
     * Remove (delete) the <code>{@link org.quartz.Trigger}</code> with the given key.
     * @param triggerKey the key of the trigger to be removed
     * @param removeNonDurableJob if true, the job associated with the given trigger will be removed if it is non-durable
     *                            and has no other triggers
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger was found and removed
     */
    @Override
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

        if(isNullOrEmpty(trigger.getCalendarName())){
            jedis.srem(redisSchema.calendarTriggersSetKey(trigger.getCalendarName()), triggerHashKey);
        }
        unsetTriggerState(triggerHashKey, jedis);
        jedis.del(triggerHashKey);
        jedis.del(redisSchema.triggerDataMapHashKey(triggerKey));
        return true;
    }

    /**
     * Store a job in Redis
     * @param jobDetail the {@link org.quartz.JobDetail} object to be stored
     * @param replaceExisting if true, any existing job with the same group and name as the given job will be overwritten
     * @param jedis a thread-safe Redis connection
     * @throws org.quartz.ObjectAlreadyExistsException
     */
    @Override
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
        pipe.del(jobDataMapHashKey);
        if(jobDetail.getJobDataMap() != null && !jobDetail.getJobDataMap().isEmpty()){
            pipe.hmset(jobDataMapHashKey, getStringDataMap(jobDetail.getJobDataMap()));
        }

        pipe.sadd(redisSchema.jobsSet(), jobHashKey);
        pipe.sadd(redisSchema.jobGroupsSet(), jobGroupSetKey);
        pipe.sadd(jobGroupSetKey, jobHashKey);
        pipe.sync();
    }

    /**
     * Store a trigger in redis
     * @param trigger the trigger to be stored
     * @param replaceExisting true if an existing trigger with the same identity should be replaced
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException
     * @throws ObjectAlreadyExistsException
     */
    @Override
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
        if (trigger.getJobDataMap() != null && !trigger.getJobDataMap().isEmpty()) {
            final String triggerDataMapHashKey = redisSchema.triggerDataMapHashKey(trigger.getKey());
            pipe.hmset(triggerDataMapHashKey, getStringDataMap(trigger.getJobDataMap()));
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
        final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
        final long nextFireTime = trigger.getNextFireTime() != null ? trigger.getNextFireTime().getTime() : -1;
        if (triggerPausedResponse.get() || jobPausedResponse.get()){
            if (isBlockedJob(jobHashKey, jedis)) {
                setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double) nextFireTime, triggerHashKey, jedis);
            } else {
                setTriggerState(RedisTriggerState.PAUSED, (double) nextFireTime, triggerHashKey, jedis);
            }
        } else if(trigger.getNextFireTime() != null){
            if (isBlockedJob(jobHashKey, jedis)) {
                setTriggerState(RedisTriggerState.BLOCKED, nextFireTime, triggerHashKey, jedis);
            } else {
                setTriggerState(RedisTriggerState.WAITING, (double) trigger.getNextFireTime().getTime(), triggerHashKey, jedis);
            }
        }
    }

    /**
     * Unsets the state of the given trigger key by removing the trigger from all trigger state sets.
     * @param triggerHashKey the redis key of the desired trigger hash
     * @param jedis a thread-safe Redis connection
     * @return true if the trigger was removed, false if the trigger was stateless
     * @throws org.quartz.JobPersistenceException if the unset operation failed
     */
    @Override
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

    @Override
    public boolean unsetTriggerState(final String triggerHashKey,RedisTriggerState excludeState, Jedis jedis) throws JobPersistenceException {
        boolean removed = false;
        Pipeline pipe = jedis.pipelined();
        List<Response<Long>> responses = new ArrayList<>(RedisTriggerState.values().length);
        for (RedisTriggerState state : RedisTriggerState.values()) {
            if (state.name().equalsIgnoreCase(excludeState.name())){
                continue;
            }
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
     * Store a {@link org.quartz.Calendar}
     * @param name the name of the calendar
     * @param calendar the calendar object to be stored
     * @param replaceExisting if true, any existing calendar with the same name will be overwritten
     * @param updateTriggers if true, any existing triggers associated with the calendar will be updated
     * @param jedis a thread-safe Redis connection
     * @throws JobPersistenceException
     */
    @Override
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
    @Override
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
     * Get the keys of all of the <code>{@link org.quartz.Job}</code> s that have the given group name.
     * @param matcher the matcher with which to compare group names
     * @param jedis a thread-safe Redis connection
     * @return the set of all JobKeys which have the given group name
     */
    @Override
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
    @Override
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
     * Get the current state of the identified <code>{@link org.quartz.Trigger}</code>.
     * @param triggerKey the key of the desired trigger
     * @param jedis a thread-safe Redis connection
     * @return the state of the trigger
     */
    @Override
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
    @Override
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
    @Override
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
     * Pause all of the <code>{@link org.quartz.Job}s</code> in the given group - by pausing all of their
     * <code>Trigger</code>s.
     * @param groupMatcher the mather which will determine which job group should be paused
     * @param jedis a thread-safe Redis connection
     * @return a collection of names of job groups which have been paused
     * @throws JobPersistenceException
     */
    @Override
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
    @Override
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
            if(isBlockedJob(jobHashKey, jedis)){
                setTriggerState(RedisTriggerState.BLOCKED, (double) nextFireTime.getTime(), triggerHashKey, jedis);
            }
            else{
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime.getTime(), triggerHashKey, jedis);
            }
        }
        applyMisfire(trigger, jedis);
    }

    /**
     * Resume (un-pause) all of the <code>{@link org.quartz.Trigger}s</code> in the given group.
     * @param matcher matcher for the trigger groups to be resumed
     * @param jedis a thread-safe Redis connection
     * @return the names of trigger groups which were resumed
     */
    @Override
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
     * Resume (un-pause) all of the <code>{@link org.quartz.Job}s</code> in the given group.
     * @param matcher the matcher with which to compare job group names
     * @param jedis a thread-safe Redis connection
     * @return the set of job groups which were matched and resumed
     */
    @Override
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
    @Override
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

            // set the trigger state to WAITING
            final Date nextFireDate = trigger.getNextFireTime();
            long nextFireTime = 0;
            if (nextFireDate != null) {
                nextFireTime = nextFireDate.getTime();
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, Long.toString(nextFireTime));
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime, triggerHashKey, jedis);
            }

            JobDetail job = retrieveJob(trigger.getJobKey(), jedis);
            TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(job, trigger, calendar, false, new Date(), previousFireTime, previousFireTime, nextFireDate);

            // handling jobs for which concurrent execution is disallowed
            if (isJobConcurrentExecutionDisallowed(job.getJobClass())){
                if (logger.isTraceEnabled()) {
                    logger.trace("Firing trigger " + trigger.getKey() + " for job " + job.getKey() + " for which concurrent execution is disallowed. Adding job to blocked jobs set.");
                }
                final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
                final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(job.getKey());
                for (String nonConcurrentTriggerHashKey : jedis.smembers(jobTriggerSetKey)) {
                    Double score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.WAITING), nonConcurrentTriggerHashKey);
                    if(score != null){
                        if (logger.isTraceEnabled()) {
                            logger.trace("Setting state of trigger " + trigger.getKey() + " for non-concurrent job " + job.getKey() + " to BLOCKED.");
                        }
                        setTriggerState(RedisTriggerState.BLOCKED, score, nonConcurrentTriggerHashKey, jedis);
                        // setting trigger state removes trigger locks, so re-lock
                        lockTrigger(redisSchema.triggerKey(nonConcurrentTriggerHashKey), jedis);
                    }
                    else{
                        score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED), nonConcurrentTriggerHashKey);
                        if(score != null){
                            if (logger.isTraceEnabled()) {
                                logger.trace("Setting state of trigger " + trigger.getKey() + " for non-concurrent job " + job.getKey() + " to PAUSED_BLOCKED.");
                            }
                            setTriggerState(RedisTriggerState.PAUSED_BLOCKED, score, nonConcurrentTriggerHashKey, jedis);
                            // setting trigger state removes trigger locks, so re-lock
                            lockTrigger(redisSchema.triggerKey(nonConcurrentTriggerHashKey), jedis);
                        }
                    }
                }
                pipe = jedis.pipelined();
                pipe.set(redisSchema.jobBlockedKey(job.getKey()), schedulerInstanceId);
                pipe.sadd(redisSchema.blockedJobsSet(), jobHashKey);
                pipe.sync();
            } else if(nextFireDate != null){
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, Long.toString(nextFireTime));
                logger.debug(String.format("Releasing trigger %s with next fire time %s. Setting state to WAITING.", triggerHashKey, nextFireTime));
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime, triggerHashKey, jedis);
            } else {
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, "");
                unsetTriggerState(triggerHashKey, jedis);
            }
            jedis.hset(triggerHashKey, TRIGGER_PREVIOUS_FIRE_TIME, Long.toString(System.currentTimeMillis()));

            results.add(new TriggerFiredResult(triggerFiredBundle));
        }
        return results;
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
    @Override
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
                pipe.syncAndReturnAll();
            }
            if (isJobConcurrentExecutionDisallowed(jobDetail.getJobClass())) {
                // unblock the job
                pipe = jedis.pipelined();
                pipe.srem(redisSchema.blockedJobsSet(), jobHashKey);
                pipe.del(redisSchema.jobBlockedKey(jobDetail.getKey()));
                pipe.syncAndReturnAll();

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
                    if(isNullOrEmpty(jedis.hget(triggerHashKey, TRIGGER_NEXT_FIRE_TIME))){
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
                    final double score = isNullOrEmpty(nextFireTime) ? 0 : Double.parseDouble(nextFireTime);
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
