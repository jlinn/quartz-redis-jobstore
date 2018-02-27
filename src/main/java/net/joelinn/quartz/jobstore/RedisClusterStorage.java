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
import redis.clients.jedis.JedisCluster;

import java.util.*;

/**
 * Joe Linn
 * 8/22/2015
 */
public class RedisClusterStorage extends AbstractRedisStorage<JedisCluster> {
    private static final Logger logger = LoggerFactory.getLogger(RedisClusterStorage.class);

    public RedisClusterStorage(RedisJobStoreSchema redisSchema, ObjectMapper mapper, SchedulerSignaler signaler, String schedulerInstanceId, int lockTimeout) {
        super(redisSchema, mapper, signaler, schedulerInstanceId, lockTimeout);
    }

    /**
     * Store a job in Redis
     *
     * @param jobDetail       the {@link JobDetail} object to be stored
     * @param replaceExisting if true, any existing job with the same group and name as the given job will be overwritten
     * @param jedis           a thread-safe Redis connection
     * @throws ObjectAlreadyExistsException
     */
    @Override
    @SuppressWarnings("unchecked")
    public void storeJob(JobDetail jobDetail, boolean replaceExisting, JedisCluster jedis) throws ObjectAlreadyExistsException {
        final String jobHashKey = redisSchema.jobHashKey(jobDetail.getKey());
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobDetail.getKey());
        final String jobGroupSetKey = redisSchema.jobGroupSetKey(jobDetail.getKey());

        if (!replaceExisting && jedis.exists(jobHashKey)) {
            throw new ObjectAlreadyExistsException(jobDetail);
        }

        jedis.hmset(jobHashKey, (Map<String, String>) mapper.convertValue(jobDetail, new TypeReference<HashMap<String, String>>() {
        }));
        if (jobDetail.getJobDataMap() != null && !jobDetail.getJobDataMap().isEmpty()) {
            jedis.hmset(jobDataMapHashKey, getStringDataMap(jobDetail.getJobDataMap()));
        }

        jedis.sadd(redisSchema.jobsSet(), jobHashKey);
        jedis.sadd(redisSchema.jobGroupsSet(), jobGroupSetKey);
        jedis.sadd(jobGroupSetKey, jobHashKey);
    }

    /**
     * Remove the given job from Redis
     *
     * @param jobKey the job to be removed
     * @param jedis  a thread-safe Redis connection
     * @return true if the job was removed; false if it did not exist
     */
    @Override
    public boolean removeJob(JobKey jobKey, JedisCluster jedis) throws JobPersistenceException {
        final String jobHashKey = redisSchema.jobHashKey(jobKey);
        final String jobBlockedKey = redisSchema.jobBlockedKey(jobKey);
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobKey);
        final String jobGroupSetKey = redisSchema.jobGroupSetKey(jobKey);
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(jobKey);

        // remove the job and any associated data
        Long delJobHashKeyResponse = jedis.del(jobHashKey);
        // remove the blocked job key
        jedis.del(jobBlockedKey);
        // remove the job's data map
        jedis.del(jobDataMapHashKey);
        // remove the job from the set of all jobs
        jedis.srem(redisSchema.jobsSet(), jobHashKey);
        // remove the job from the set of blocked jobs
        jedis.srem(redisSchema.blockedJobsSet(), jobHashKey);
        // remove the job from its group
        jedis.srem(jobGroupSetKey, jobHashKey);
        // retrieve the keys for all triggers associated with this job, then delete that set
        Set<String> jobTriggerSetResponse = jedis.smembers(jobTriggerSetKey);
        jedis.del(jobTriggerSetKey);
        Long jobGroupSetSizeResponse = jedis.scard(jobGroupSetKey);
        if (jobGroupSetSizeResponse == 0) {
            // The group now contains no jobs. Remove it from the set of all job groups.
            jedis.srem(redisSchema.jobGroupsSet(), jobGroupSetKey);
        }

        // remove all triggers associated with this job
        for (String triggerHashKey : jobTriggerSetResponse) {
            // get this trigger's TriggerKey
            final TriggerKey triggerKey = redisSchema.triggerKey(triggerHashKey);
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(triggerKey);
            unsetTriggerState(triggerHashKey, jedis);
            // remove the trigger from the set of all triggers
            jedis.srem(redisSchema.triggersSet(), triggerHashKey);
            // remove the trigger's group from the set of all trigger groups
            jedis.srem(redisSchema.triggerGroupsSet(), triggerGroupSetKey);
            // remove this trigger from its group
            jedis.srem(triggerGroupSetKey, triggerHashKey);
            // delete the trigger
            jedis.del(triggerHashKey);
        }
        return delJobHashKeyResponse == 1;
    }

    /**
     * Store a trigger in redis
     *
     * @param trigger         the trigger to be stored
     * @param replaceExisting true if an existing trigger with the same identity should be replaced
     * @param jedis           a thread-safe Redis connection
     * @throws JobPersistenceException
     * @throws ObjectAlreadyExistsException
     */
    @Override
    public void storeTrigger(OperableTrigger trigger, boolean replaceExisting, JedisCluster jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
        final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(trigger.getKey());
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(trigger.getJobKey());

        if (!(trigger instanceof SimpleTrigger) && !(trigger instanceof CronTrigger)) {
            throw new UnsupportedOperationException("Only SimpleTrigger and CronTrigger are supported.");
        }
        final boolean exists = jedis.exists(triggerHashKey);
        if (exists && !replaceExisting) {
            throw new ObjectAlreadyExistsException(trigger);
        }

        Map<String, String> triggerMap = mapper.convertValue(trigger, new TypeReference<HashMap<String, String>>() {
        });
        triggerMap.put(TRIGGER_CLASS, trigger.getClass().getName());

        jedis.hmset(triggerHashKey, triggerMap);
        jedis.sadd(redisSchema.triggersSet(), triggerHashKey);
        jedis.sadd(redisSchema.triggerGroupsSet(), triggerGroupSetKey);
        jedis.sadd(triggerGroupSetKey, triggerHashKey);
        jedis.sadd(jobTriggerSetKey, triggerHashKey);
        if (trigger.getCalendarName() != null && !trigger.getCalendarName().isEmpty()) {
            final String calendarTriggersSetKey = redisSchema.calendarTriggersSetKey(trigger.getCalendarName());
            jedis.sadd(calendarTriggersSetKey, triggerHashKey);
        }
        if (trigger.getJobDataMap() != null && !trigger.getJobDataMap().isEmpty()) {
            final String triggerDataMapHashKey = redisSchema.triggerDataMapHashKey(trigger.getKey());
            jedis.hmset(triggerDataMapHashKey, getStringDataMap(trigger.getJobDataMap()));
        }

        if (exists) {
            // We're overwriting a previously stored instance of this trigger, so clear any existing trigger state.
            unsetTriggerState(triggerHashKey, jedis);
        }

        Boolean triggerPausedResponse = jedis.sismember(redisSchema.pausedTriggerGroupsSet(), triggerGroupSetKey);
        Boolean jobPausedResponse = jedis.sismember(redisSchema.pausedJobGroupsSet(), redisSchema.jobGroupSetKey(trigger.getJobKey()));

        if (triggerPausedResponse || jobPausedResponse) {
            final long nextFireTime = trigger.getNextFireTime() != null ? trigger.getNextFireTime().getTime() : -1;
            final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
            if (jedis.sismember(redisSchema.blockedJobsSet(), jobHashKey)) {
                setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double) nextFireTime, triggerHashKey, jedis);
            } else {
                setTriggerState(RedisTriggerState.PAUSED, (double) nextFireTime, triggerHashKey, jedis);
            }
        } else if (trigger.getNextFireTime() != null) {
            setTriggerState(RedisTriggerState.WAITING, (double) trigger.getNextFireTime().getTime(), triggerHashKey, jedis);
        }
    }

    /**
     * Remove (delete) the <code>{@link Trigger}</code> with the given key.
     *
     * @param triggerKey          the key of the trigger to be removed
     * @param removeNonDurableJob if true, the job associated with the given trigger will be removed if it is non-durable
     *                            and has no other triggers
     * @param jedis               a thread-safe Redis connection
     * @return true if the trigger was found and removed
     */
    @Override
    protected boolean removeTrigger(TriggerKey triggerKey, boolean removeNonDurableJob, JedisCluster jedis) throws JobPersistenceException, ClassNotFoundException {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(triggerKey);

        if (!jedis.exists(triggerHashKey)) {
            return false;
        }

        OperableTrigger trigger = retrieveTrigger(triggerKey, jedis);

        final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
        final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(trigger.getJobKey());

        // remove the trigger from the set of all triggers
        jedis.srem(redisSchema.triggersSet(), triggerHashKey);
        // remove the trigger from its trigger group set
        jedis.srem(triggerGroupSetKey, triggerHashKey);
        // remove the trigger from the associated job's trigger set
        jedis.srem(jobTriggerSetKey, triggerHashKey);

        if (jedis.scard(triggerGroupSetKey) == 0) {
            // The trigger group set is empty. Remove the trigger group from the set of trigger groups.
            jedis.srem(redisSchema.triggerGroupsSet(), triggerGroupSetKey);
        }

        if (removeNonDurableJob) {
            Long jobTriggerSetKeySizeResponse = jedis.scard(jobTriggerSetKey);
            Boolean jobExistsResponse = jedis.exists(jobHashKey);
            if (jobTriggerSetKeySizeResponse == 0 && jobExistsResponse) {
                JobDetail job = retrieveJob(trigger.getJobKey(), jedis);
                if (!job.isDurable()) {
                    // Job is not durable and has no remaining triggers. Delete it.
                    removeJob(job.getKey(), jedis);
                    signaler.notifySchedulerListenersJobDeleted(job.getKey());
                }
            }
        }

        if (isNullOrEmpty(trigger.getCalendarName())) {
            jedis.srem(redisSchema.calendarTriggersSetKey(trigger.getCalendarName()), triggerHashKey);
        }
        unsetTriggerState(triggerHashKey, jedis);
        jedis.del(triggerHashKey);
        return true;
    }

    /**
     * Unsets the state of the given trigger key by removing the trigger from all trigger state sets.
     *
     * @param triggerHashKey the redis key of the desired trigger hash
     * @param jedis          a thread-safe Redis connection
     * @return true if the trigger was removed, false if the trigger was stateless
     * @throws JobPersistenceException if the unset operation failed
     */
    @Override
    public boolean unsetTriggerState(String triggerHashKey, JedisCluster jedis) throws JobPersistenceException {
        boolean removed = false;
        List<Long> responses = new ArrayList<>(RedisTriggerState.values().length);
        for (RedisTriggerState state : RedisTriggerState.values()) {
            responses.add(jedis.zrem(redisSchema.triggerStateKey(state), triggerHashKey));
        }
        for (Long response : responses) {
            removed = response == 1;
            if (removed) {
                jedis.del(redisSchema.triggerLockKey(redisSchema.triggerKey(triggerHashKey)));
                break;
            }
        }
        return removed;
    }

    /**
     * Store a {@link Calendar}
     *
     * @param name            the name of the calendar
     * @param calendar        the calendar object to be stored
     * @param replaceExisting if true, any existing calendar with the same name will be overwritten
     * @param updateTriggers  if true, any existing triggers associated with the calendar will be updated
     * @param jedis           a thread-safe Redis connection
     * @throws JobPersistenceException
     */
    @Override
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers, JedisCluster jedis) throws JobPersistenceException {
        final String calendarHashKey = redisSchema.calendarHashKey(name);
        if (!replaceExisting && jedis.exists(calendarHashKey)) {
            throw new ObjectAlreadyExistsException(String.format("Calendar with key %s already exists.", calendarHashKey));
        }
        Map<String, String> calendarMap = new HashMap<>();
        calendarMap.put(CALENDAR_CLASS, calendar.getClass().getName());
        try {
            calendarMap.put(CALENDAR_JSON, mapper.writeValueAsString(calendar));
        } catch (JsonProcessingException e) {
            throw new JobPersistenceException("Unable to serialize calendar.", e);
        }

        jedis.hmset(calendarHashKey, calendarMap);
        jedis.sadd(redisSchema.calendarsSet(), calendarHashKey);

        if (updateTriggers) {
            final String calendarTriggersSetKey = redisSchema.calendarTriggersSetKey(name);
            Set<String> triggerHashKeys = jedis.smembers(calendarTriggersSetKey);
            for (String triggerHashKey : triggerHashKeys) {
                OperableTrigger trigger = retrieveTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
                long removed = jedis.zrem(redisSchema.triggerStateKey(RedisTriggerState.WAITING), triggerHashKey);
                trigger.updateWithNewCalendar(calendar, misfireThreshold);
                if (removed == 1) {
                    setTriggerState(RedisTriggerState.WAITING, (double) trigger.getNextFireTime().getTime(), triggerHashKey, jedis);
                }
            }
        }
    }

    /**
     * Remove (delete) the <code>{@link Calendar}</code> with the given name.
     *
     * @param calendarName the name of the calendar to be removed
     * @param jedis        a thread-safe Redis connection
     * @return true if a calendar with the given name was found and removed
     */
    @Override
    public boolean removeCalendar(String calendarName, JedisCluster jedis) throws JobPersistenceException {
        final String calendarTriggersSetKey = redisSchema.calendarTriggersSetKey(calendarName);

        if (jedis.scard(calendarTriggersSetKey) > 0) {
            throw new JobPersistenceException(String.format("There are triggers pointing to calendar %s, so it cannot be removed.", calendarName));
        }
        final String calendarHashKey = redisSchema.calendarHashKey(calendarName);
        Long deleteResponse = jedis.del(calendarHashKey);
        jedis.srem(redisSchema.calendarsSet(), calendarHashKey);

        return deleteResponse == 1;
    }

    /**
     * Get the keys of all of the <code>{@link Job}</code> s that have the given group name.
     *
     * @param matcher the matcher with which to compare group names
     * @param jedis   a thread-safe Redis connection
     * @return the set of all JobKeys which have the given group name
     */
    @Override
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher, JedisCluster jedis) {
        Set<JobKey> jobKeys = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String jobGroupSetKey = redisSchema.jobGroupSetKey(new JobKey("", matcher.getCompareToValue()));
            final Set<String> jobs = jedis.smembers(jobGroupSetKey);
            if (jobs != null) {
                for (final String job : jobs) {
                    jobKeys.add(redisSchema.jobKey(job));
                }
            }
        } else {
            List<Set<String>> jobGroups = new ArrayList<>();
            for (final String jobGroupSetKey : jedis.smembers(redisSchema.jobGroupsSet())) {
                if (matcher.getCompareWithOperator().evaluate(redisSchema.jobGroup(jobGroupSetKey), matcher.getCompareToValue())) {
                    jobGroups.add(jedis.smembers(jobGroupSetKey));
                }
            }
            for (Set<String> jobGroup : jobGroups) {
                if (jobGroup != null) {
                    for (final String job : jobGroup) {
                        jobKeys.add(redisSchema.jobKey(job));
                    }
                }
            }
        }
        return jobKeys;
    }

    /**
     * Get the names of all of the <code>{@link Trigger}</code> s that have the given group name.
     *
     * @param matcher the matcher with which to compare group names
     * @param jedis   a thread-safe Redis connection
     * @return the set of all TriggerKeys which have the given group name
     */
    @Override
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher, JedisCluster jedis) {
        Set<TriggerKey> triggerKeys = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(new TriggerKey("", matcher.getCompareToValue()));
            final Set<String> triggers = jedis.smembers(triggerGroupSetKey);
            if (triggers != null) {
                for (final String trigger : triggers) {
                    triggerKeys.add(redisSchema.triggerKey(trigger));
                }
            }
        } else {
            List<Set<String>> triggerGroups = new ArrayList<>();
            for (final String triggerGroupSetKey : jedis.smembers(redisSchema.triggerGroupsSet())) {
                if (matcher.getCompareWithOperator().evaluate(redisSchema.triggerGroup(triggerGroupSetKey), matcher.getCompareToValue())) {
                    triggerGroups.add(jedis.smembers(triggerGroupSetKey));
                }
            }
            for (Set<String> triggerGroup : triggerGroups) {
                if (triggerGroup != null) {
                    for (final String trigger : triggerGroup) {
                        triggerKeys.add(redisSchema.triggerKey(trigger));
                    }
                }
            }
        }
        return triggerKeys;
    }

    /**
     * Get the current state of the identified <code>{@link Trigger}</code>.
     *
     * @param triggerKey the key of the desired trigger
     * @param jedis      a thread-safe Redis connection
     * @return the state of the trigger
     */
    @Override
    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey, JedisCluster jedis) {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Map<RedisTriggerState, Double> scores = new HashMap<>(RedisTriggerState.values().length);
        for (RedisTriggerState redisTriggerState : RedisTriggerState.values()) {
            scores.put(redisTriggerState, jedis.zscore(redisSchema.triggerStateKey(redisTriggerState), triggerHashKey));
        }
        for (Map.Entry<RedisTriggerState, Double> entry : scores.entrySet()) {
            if (entry.getValue() != null) {
                return entry.getKey().getTriggerState();
            }
        }
        return Trigger.TriggerState.NONE;
    }

    /**
     * Pause the trigger with the given key
     *
     * @param triggerKey the key of the trigger to be paused
     * @param jedis      a thread-safe Redis connection
     * @throws JobPersistenceException if the desired trigger does not exist
     */
    @Override
    public void pauseTrigger(TriggerKey triggerKey, JedisCluster jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Boolean exists = jedis.exists(triggerHashKey);
        Double completedScore = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.COMPLETED), triggerHashKey);
        String nextFireTimeResponse = jedis.hget(triggerHashKey, TRIGGER_NEXT_FIRE_TIME);
        Double blockedScore = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.BLOCKED), triggerHashKey);

        if (!exists) {
            return;
        }
        if (completedScore != null) {
            // doesn't make sense to pause a completed trigger
            return;
        }

        final long nextFireTime = nextFireTimeResponse == null
                || nextFireTimeResponse.isEmpty() ? -1 : Long.parseLong(nextFireTimeResponse);
        if (blockedScore != null) {
            setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double) nextFireTime, triggerHashKey, jedis);
        } else {
            setTriggerState(RedisTriggerState.PAUSED, (double) nextFireTime, triggerHashKey, jedis);
        }
    }

    /**
     * Pause all of the <code>{@link Trigger}s</code> in the given group.
     *
     * @param matcher matcher for the trigger groups to be paused
     * @param jedis   a thread-safe Redis connection
     * @return a collection of names of trigger groups which were matched and paused
     * @throws JobPersistenceException
     */
    @Override
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher, JedisCluster jedis) throws JobPersistenceException {
        Set<String> pausedTriggerGroups = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(new TriggerKey("", matcher.getCompareToValue()));
            final long addResult = jedis.sadd(redisSchema.pausedTriggerGroupsSet(), triggerGroupSetKey);
            if (addResult > 0) {
                for (final String trigger : jedis.smembers(triggerGroupSetKey)) {
                    pauseTrigger(redisSchema.triggerKey(trigger), jedis);
                }
                pausedTriggerGroups.add(redisSchema.triggerGroup(triggerGroupSetKey));
            }
        } else {
            Map<String, Set<String>> triggerGroups = new HashMap<>();
            for (final String triggerGroupSetKey : jedis.smembers(redisSchema.triggerGroupsSet())) {
                if (matcher.getCompareWithOperator().evaluate(redisSchema.triggerGroup(triggerGroupSetKey), matcher.getCompareToValue())) {
                    triggerGroups.put(triggerGroupSetKey, jedis.smembers(triggerGroupSetKey));
                }
            }
            for (final Map.Entry<String, Set<String>> entry : triggerGroups.entrySet()) {
                if (jedis.sadd(redisSchema.pausedJobGroupsSet(), entry.getKey()) > 0) {
                    // This trigger group was not paused. Pause it now.
                    pausedTriggerGroups.add(redisSchema.triggerGroup(entry.getKey()));
                    for (final String triggerHashKey : entry.getValue()) {
                        pauseTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
                    }
                }
            }
        }
        return pausedTriggerGroups;
    }

    /**
     * Pause all of the <code>{@link Job}s</code> in the given group - by pausing all of their
     * <code>Trigger</code>s.
     *
     * @param groupMatcher the mather which will determine which job group should be paused
     * @param jedis        a thread-safe Redis connection
     * @return a collection of names of job groups which have been paused
     * @throws JobPersistenceException
     */
    @Override
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher, JedisCluster jedis) throws JobPersistenceException {
        Set<String> pausedJobGroups = new HashSet<>();
        if (groupMatcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String jobGroupSetKey = redisSchema.jobGroupSetKey(new JobKey("", groupMatcher.getCompareToValue()));
            if (jedis.sadd(redisSchema.pausedJobGroupsSet(), jobGroupSetKey) > 0) {
                pausedJobGroups.add(redisSchema.jobGroup(jobGroupSetKey));
                for (String job : jedis.smembers(jobGroupSetKey)) {
                    pauseJob(redisSchema.jobKey(job), jedis);
                }
            }
        } else {
            Map<String, Set<String>> jobGroups = new HashMap<>();
            for (final String jobGroupSetKey : jedis.smembers(redisSchema.jobGroupsSet())) {
                if (groupMatcher.getCompareWithOperator().evaluate(redisSchema.jobGroup(jobGroupSetKey), groupMatcher.getCompareToValue())) {
                    jobGroups.put(jobGroupSetKey, jedis.smembers(jobGroupSetKey));
                }
            }
            for (final Map.Entry<String, Set<String>> entry : jobGroups.entrySet()) {
                if (jedis.sadd(redisSchema.pausedJobGroupsSet(), entry.getKey()) > 0) {
                    // This job group was not already paused. Pause it now.
                    pausedJobGroups.add(redisSchema.jobGroup(entry.getKey()));
                    for (final String jobHashKey : entry.getValue()) {
                        pauseJob(redisSchema.jobKey(jobHashKey), jedis);
                    }
                }
            }
        }
        return pausedJobGroups;
    }

    /**
     * Resume (un-pause) a {@link Trigger}
     *
     * @param triggerKey the key of the trigger to be resumed
     * @param jedis      a thread-safe Redis connection
     */
    @Override
    public void resumeTrigger(TriggerKey triggerKey, JedisCluster jedis) throws JobPersistenceException {
        final String triggerHashKey = redisSchema.triggerHashKey(triggerKey);
        Boolean exists = jedis.sismember(redisSchema.triggersSet(), triggerHashKey);
        Double isPaused = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED), triggerHashKey);
        Double isPausedBlocked = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED_BLOCKED), triggerHashKey);

        if (!exists) {
            // Trigger does not exist.  Nothing to do.
            return;
        }
        if (isPaused == null && isPausedBlocked == null) {
            // Trigger is not paused. Nothing to do.
            return;
        }
        OperableTrigger trigger = retrieveTrigger(triggerKey, jedis);
        final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
        final Date nextFireTime = trigger.getNextFireTime();

        if (nextFireTime != null) {
            if (jedis.sismember(redisSchema.blockedJobsSet(), jobHashKey)) {
                setTriggerState(RedisTriggerState.BLOCKED, (double) nextFireTime.getTime(), triggerHashKey, jedis);
            } else {
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime.getTime(), triggerHashKey, jedis);
            }
        }
        applyMisfire(trigger, jedis);
    }

    /**
     * Resume (un-pause) all of the <code>{@link Trigger}s</code> in the given group.
     *
     * @param matcher matcher for the trigger groups to be resumed
     * @param jedis   a thread-safe Redis connection
     * @return the names of trigger groups which were resumed
     */
    @Override
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher, JedisCluster jedis) throws JobPersistenceException {
        Set<String> resumedTriggerGroups = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String triggerGroupSetKey = redisSchema.triggerGroupSetKey(new TriggerKey("", matcher.getCompareToValue()));
            jedis.srem(redisSchema.pausedJobGroupsSet(), triggerGroupSetKey);
            Set<String> triggerHashKeysResponse = jedis.smembers(triggerGroupSetKey);
            for (String triggerHashKey : triggerHashKeysResponse) {
                OperableTrigger trigger = retrieveTrigger(redisSchema.triggerKey(triggerHashKey), jedis);
                resumeTrigger(trigger.getKey(), jedis);
                resumedTriggerGroups.add(trigger.getKey().getGroup());
            }
        } else {
            for (final String triggerGroupSetKey : jedis.smembers(redisSchema.triggerGroupsSet())) {
                if (matcher.getCompareWithOperator().evaluate(redisSchema.triggerGroup(triggerGroupSetKey), matcher.getCompareToValue())) {
                    resumedTriggerGroups.addAll(resumeTriggers(GroupMatcher.triggerGroupEquals(redisSchema.triggerGroup(triggerGroupSetKey)), jedis));
                }
            }
        }
        return resumedTriggerGroups;
    }

    /**
     * Resume (un-pause) all of the <code>{@link Job}s</code> in the given group.
     *
     * @param matcher the matcher with which to compare job group names
     * @param jedis   a thread-safe Redis connection
     * @return the set of job groups which were matched and resumed
     */
    @Override
    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher, JedisCluster jedis) throws JobPersistenceException {
        Set<String> resumedJobGroups = new HashSet<>();
        if (matcher.getCompareWithOperator() == StringMatcher.StringOperatorName.EQUALS) {
            final String jobGroupSetKey = redisSchema.jobGroupSetKey(new JobKey("", matcher.getCompareToValue()));
            Long unpauseResponse = jedis.srem(redisSchema.pausedJobGroupsSet(), jobGroupSetKey);
            Set<String> jobsResponse = jedis.smembers(jobGroupSetKey);
            if (unpauseResponse > 0) {
                resumedJobGroups.add(redisSchema.jobGroup(jobGroupSetKey));
            }
            for (String job : jobsResponse) {
                resumeJob(redisSchema.jobKey(job), jedis);
            }
        } else {
            for (final String jobGroupSetKey : jedis.smembers(redisSchema.jobGroupsSet())) {
                if (matcher.getCompareWithOperator().evaluate(redisSchema.jobGroup(jobGroupSetKey), matcher.getCompareToValue())) {
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
     *
     * @param triggers a list of triggers
     * @param jedis    a thread-safe Redis connection
     * @return may return null if all the triggers or their calendars no longer exist, or
     * if the trigger was not successfully put into the 'executing'
     * state.  Preference is to return an empty list if none of the triggers
     * could be fired.
     */
    @Override
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers, JedisCluster jedis) throws JobPersistenceException, ClassNotFoundException {
        List<TriggerFiredResult> results = new ArrayList<>();
        for (OperableTrigger trigger : triggers) {
            final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
            logger.debug(String.format("Trigger %s fired.", triggerHashKey));
            Boolean triggerExistsResponse = jedis.exists(triggerHashKey);
            Double triggerAcquiredResponse = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.ACQUIRED), triggerHashKey);
            if (!triggerExistsResponse || triggerAcquiredResponse == null) {
                // the trigger does not exist or the trigger is not acquired
                if (!triggerExistsResponse) {
                    logger.debug(String.format("Trigger %s does not exist.", triggerHashKey));
                } else {
                    logger.debug(String.format("Trigger %s was not acquired.", triggerHashKey));
                }
                continue;
            }
            Calendar calendar = null;
            final String calendarName = trigger.getCalendarName();
            if (calendarName != null) {
                calendar = retrieveCalendar(calendarName, jedis);
                if (calendar == null) {
                    continue;
                }
            }

            final Date previousFireTime = trigger.getPreviousFireTime();
            trigger.triggered(calendar);

            JobDetail job = retrieveJob(trigger.getJobKey(), jedis);
            TriggerFiredBundle triggerFiredBundle = new TriggerFiredBundle(job, trigger, calendar, false, new Date(), previousFireTime, previousFireTime, trigger.getNextFireTime());

            // handling jobs for which concurrent execution is disallowed
            if (isJobConcurrentExecutionDisallowed(job.getJobClass())) {
                final String jobHashKey = redisSchema.jobHashKey(trigger.getJobKey());
                final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(job.getKey());
                for (String nonConcurrentTriggerHashKey : jedis.smembers(jobTriggerSetKey)) {
                    Double score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.WAITING), nonConcurrentTriggerHashKey);
                    if (score != null) {
                        setTriggerState(RedisTriggerState.BLOCKED, score, nonConcurrentTriggerHashKey, jedis);
                        // setting trigger state removes locks, so re-lock
                        lockTrigger(redisSchema.triggerKey(nonConcurrentTriggerHashKey), jedis);
                    } else {
                        score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED), nonConcurrentTriggerHashKey);
                        if (score != null) {
                            setTriggerState(RedisTriggerState.PAUSED_BLOCKED, score, nonConcurrentTriggerHashKey, jedis);
                            // setting trigger state removes locks, so re-lock
                            lockTrigger(redisSchema.triggerKey(nonConcurrentTriggerHashKey), jedis);
                        }
                    }
                }
                jedis.set(redisSchema.jobBlockedKey(job.getKey()), schedulerInstanceId);
                jedis.sadd(redisSchema.blockedJobsSet(), jobHashKey);
            }

            // release the fired trigger
            if (trigger.getNextFireTime() != null) {
                final long nextFireTime = trigger.getNextFireTime().getTime();
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, Long.toString(nextFireTime));
                logger.debug(String.format("Releasing trigger %s with next fire time %s. Setting state to WAITING.", triggerHashKey, nextFireTime));
                setTriggerState(RedisTriggerState.WAITING, (double) nextFireTime, triggerHashKey, jedis);
            } else {
                jedis.hset(triggerHashKey, TRIGGER_NEXT_FIRE_TIME, "");
                unsetTriggerState(triggerHashKey, jedis);
            }

            results.add(new TriggerFiredResult(triggerFiredBundle));
        }
        return results;
    }

    /**
     * Inform the <code>JobStore</code> that the scheduler has completed the
     * firing of the given <code>Trigger</code> (and the execution of its
     * associated <code>Job</code> completed, threw an exception, or was vetoed),
     * and that the <code>{@link JobDataMap}</code>
     * in the given <code>JobDetail</code> should be updated if the <code>Job</code>
     * is stateful.
     *
     * @param trigger         the trigger which was completed
     * @param jobDetail       the job which was completed
     * @param triggerInstCode the status of the completed job
     * @param jedis           a thread-safe Redis connection
     */
    @Override
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction triggerInstCode, JedisCluster jedis) throws JobPersistenceException, ClassNotFoundException {
        final String jobHashKey = redisSchema.jobHashKey(jobDetail.getKey());
        final String jobDataMapHashKey = redisSchema.jobDataMapHashKey(jobDetail.getKey());
        final String triggerHashKey = redisSchema.triggerHashKey(trigger.getKey());
        logger.debug(String.format("Job %s completed.", jobHashKey));
        if (jedis.exists(jobHashKey)) {
            // job was not deleted during execution
            if (isPersistJobDataAfterExecution(jobDetail.getJobClass())) {
                // update the job data map
                JobDataMap jobDataMap = jobDetail.getJobDataMap();
                jedis.del(jobDataMapHashKey);
                if (jobDataMap != null && !jobDataMap.isEmpty()) {
                    jedis.hmset(jobDataMapHashKey, getStringDataMap(jobDataMap));
                }
            }
            if (isJobConcurrentExecutionDisallowed(jobDetail.getJobClass())) {
                // unblock the job
                jedis.srem(redisSchema.blockedJobsSet(), jobHashKey);
                jedis.del(redisSchema.jobBlockedKey(jobDetail.getKey()));

                final String jobTriggersSetKey = redisSchema.jobTriggersSetKey(jobDetail.getKey());
                for (String nonConcurrentTriggerHashKey : jedis.smembers(jobTriggersSetKey)) {
                    Double score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.BLOCKED), nonConcurrentTriggerHashKey);
                    if (score != null) {
                        setTriggerState(RedisTriggerState.WAITING, score, nonConcurrentTriggerHashKey, jedis);
                    } else {
                        score = jedis.zscore(redisSchema.triggerStateKey(RedisTriggerState.PAUSED_BLOCKED), nonConcurrentTriggerHashKey);
                        if (score != null) {
                            setTriggerState(RedisTriggerState.PAUSED, score, nonConcurrentTriggerHashKey, jedis);
                        }
                    }
                }
                signaler.signalSchedulingChange(0L);
            }
        } else {
            // unblock the job, even if it has been deleted
            jedis.srem(redisSchema.blockedJobsSet(), jobHashKey);
        }

        if (jedis.exists(triggerHashKey)) {
            // trigger was not deleted during job execution
            if (triggerInstCode == Trigger.CompletedExecutionInstruction.DELETE_TRIGGER) {
                if (trigger.getNextFireTime() == null) {
                    // double-check for possible reschedule within job execution, which would cancel the need to delete
                    if (isNullOrEmpty(jedis.hget(triggerHashKey, TRIGGER_NEXT_FIRE_TIME))) {
                        removeTrigger(trigger.getKey(), jedis);
                    }
                } else {
                    removeTrigger(trigger.getKey(), jedis);
                    signaler.signalSchedulingChange(0L);
                }
            } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
                setTriggerState(RedisTriggerState.COMPLETED, (double) System.currentTimeMillis(), triggerHashKey, jedis);
                signaler.signalSchedulingChange(0L);
            } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
                logger.debug(String.format("Trigger %s set to ERROR state.", triggerHashKey));
                final double score = trigger.getNextFireTime() != null ? (double) trigger.getNextFireTime().getTime() : 0;
                setTriggerState(RedisTriggerState.ERROR, score, triggerHashKey, jedis);
                signaler.signalSchedulingChange(0L);
            } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
                final String jobTriggersSetKey = redisSchema.jobTriggersSetKey(jobDetail.getKey());
                for (String errorTriggerHashKey : jedis.smembers(jobTriggersSetKey)) {
                    final String nextFireTime = jedis.hget(errorTriggerHashKey, TRIGGER_NEXT_FIRE_TIME);
                    final double score = isNullOrEmpty(nextFireTime) ? 0 : Double.parseDouble(nextFireTime);
                    setTriggerState(RedisTriggerState.ERROR, score, errorTriggerHashKey, jedis);
                }
                signaler.signalSchedulingChange(0L);
            } else if (triggerInstCode == Trigger.CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
                final String jobTriggerSetKey = redisSchema.jobTriggersSetKey(jobDetail.getKey());
                for (String completedTriggerHashKey : jedis.smembers(jobTriggerSetKey)) {
                    setTriggerState(RedisTriggerState.COMPLETED, (double) System.currentTimeMillis(), completedTriggerHashKey, jedis);
                }
                signaler.signalSchedulingChange(0L);
            }
        }
    }
}
