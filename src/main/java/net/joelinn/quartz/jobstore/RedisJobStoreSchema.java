package net.joelinn.quartz.jobstore;

import org.quartz.JobKey;
import org.quartz.TriggerKey;

import java.util.Arrays;
import java.util.List;

/**
 * Joe Linn
 * 7/14/2014
 */
public class RedisJobStoreSchema {
    protected static final String DEFAULT_DELIMITER = ":";
    protected static final String JOBS_SET = "jobs";
    protected static final String JOB_GROUPS_SET = "job_groups";
    protected static final String LOCK = "lock";

    protected final String prefix;

    protected final String delimiter;

    public RedisJobStoreSchema(){
        this("");
    }

    /**
     * @param prefix the prefix to be prepended to all redis keys
     */
    public RedisJobStoreSchema(String prefix){
        this(prefix, DEFAULT_DELIMITER);
    }

    /**
     * @param prefix the prefix to be prepended to all redis keys
     * @param delimiter the delimiter to be used to separate key segments
     */
    public RedisJobStoreSchema(String prefix, String delimiter) {
        this.prefix = prefix;
        this.delimiter = delimiter;
    }

    /**
     *
     * @return the redis key used for locking
     */
    public String lockKey(){
        return  addPrefix(LOCK);
    }

    /**
     * @return the redis key for the set containing all job keys
     */
    public String jobsSet(){
        return addPrefix(JOBS_SET);
    }

    /**
     * @return the redis key for the set containing all job group keys
     */
    public String jobGroupsSet(){
        return addPrefix(JOB_GROUPS_SET);
    }

    /**
     *
     * @param jobKey
     * @return the redis key associated with the given {@link org.quartz.JobKey}
     */
    public String jobHashKey(final JobKey jobKey){
        return addPrefix("job" + delimiter + jobKey.getGroup() + delimiter + jobKey.getName());
    }

    /**
     *
     * @param jobKey
     * @return the redis key associated with the job data for the given {@link org.quartz.JobKey}
     */
    public String jobDataMapHashKey(final JobKey jobKey){
        return addPrefix("job_data_map" + delimiter + jobKey.getGroup() + delimiter + jobKey.getName());
    }

    /**
     *
     * @param jobKey
     * @return the key associated with the group set for the given {@link org.quartz.JobKey}
     */
    public String jobGroupSetKey(final JobKey jobKey){
        return addPrefix("job_group" + delimiter + jobKey.getGroup());
    }

    /**
     *
     * @param jobHashKey the hash key for a job
     * @return the {@link org.quartz.JobKey} object describing the job
     */
    public JobKey jobKey(final String jobHashKey){
        final List<String> hashParts = split(jobHashKey);
        return new JobKey(hashParts.get(2), hashParts.get(1));
    }

    /**
     *
     * @param jobGroupSetKey the redis key for a job group set
     * @return the name of the job group
     */
    public String jobGroup(final String jobGroupSetKey){
        return split(jobGroupSetKey).get(1);
    }

    /**
     * @param jobKey the job key for which to get a trigger set key
     * @return the key associated with the set of triggers for the given {@link org.quartz.JobKey}
     */
    public String jobTriggersSetKey(final JobKey jobKey){
        return addPrefix("job_triggers" + delimiter + jobKey.getGroup() + delimiter + jobKey.getName());
    }

    /**
     * @return the key associated with the set of blocked jobs
     */
    public String blockedJobsSet(){
        return addPrefix("blocked_jobs");
    }

    /**
     *
     * @param triggerKey a trigger key
     * @return the redis key associated with the given {@link org.quartz.TriggerKey}
     */
    public String triggerHashKey(final TriggerKey triggerKey){
        return addPrefix("trigger" + delimiter + triggerKey.getGroup() + delimiter + triggerKey.getName());
    }

    /**
     *
     * @param triggerHashKey the hash key for a trigger
     * @return the {@link org.quartz.TriggerKey} object describing the desired trigger
     */
    public TriggerKey triggerKey(final String triggerHashKey){
        final List<String> hashParts = split(triggerHashKey);
        return new TriggerKey(hashParts.get(2), hashParts.get(1));
    }

    /**
     *
     * @param triggerGroupSetKey the redis key for a trigger group set
     * @return the name of the trigger group represented by the given redis key
     */
    public String triggerGroup(final String triggerGroupSetKey){
        return split(triggerGroupSetKey).get(1);
    }

    /**
     * @param triggerKey a trigger key
     * @return the redis key associated with the group of the given {@link org.quartz.TriggerKey}
     */
    public String triggerGroupSetKey(final TriggerKey triggerKey){
        return addPrefix("trigger_group" + delimiter + triggerKey.getGroup());
    }

    /**
     * @return the key of the set containing all trigger keys
     */
    public String triggersSet(){
        return addPrefix("triggers");
    }

    /**
     * @return the key of the set containing all trigger group keys
     */
    public String triggerGroupsSet(){
        return addPrefix("trigger_groups");
    }

    /**
     * @return the key of the set containing paused trigger group keys
     */
    public String pausedTriggerGroupsSet(){
        return addPrefix("paused_trigger_groups");
    }

    /**
     * @param state a {@link net.joelinn.quartz.jobstore.RedisTriggerState}
     * @return the key of a set containing the keys of triggers which are in the given state
     */
    public String triggerStateKey(final RedisTriggerState state){
        return addPrefix(state.getKey());
    }

    /**
     *
     * @param triggerKey the key of the trigger for which to retrieve a lock key
     * @return the redis key for the lock state of the given trigger
     */
    public String triggerLockKey(final TriggerKey triggerKey){
        return addPrefix("trigger_lock" + delimiter + triggerKey.getGroup() + delimiter + triggerKey.getName());
    }

    /**
     *
     * @param jobKey the key of the job for which to retrieve a block key
     * @return the redis key for the blocked state of the given job
     */
    public String jobBlockedKey(final JobKey jobKey){
        return addPrefix("job_blocked" + delimiter + jobKey.getGroup() + delimiter + jobKey.getName());
    }

    /**
     * @return the key which holds the time at which triggers were last released
     */
    public String lastTriggerReleaseTime(){
        return addPrefix("last_triggers_release_time");
    }

    /**
     * @return the key of the set containing paused job groups
     */
    public String pausedJobGroupsSet(){
        return addPrefix("paused_job_groups");
    }

    /**
     * @param calendarName the name of the calendar for which to retrieve a key
     * @return the redis key for the set containing trigger keys for the given calendar name
     */
    public String calendarTriggersSetKey(final String calendarName){
        return addPrefix("calendar_triggers" + delimiter + calendarName);
    }

    /**
     * @param calendarName the name of the calendar for which to retrieve a key
     * @return the redis key for the calendar with the given name
     */
    public String calendarHashKey(final String calendarName){
        return addPrefix("calendar" + delimiter + calendarName);
    }

    /**
     *
     * @param calendarHashKey the redis key for a calendar
     * @return the name of the calendar represented by the given key
     */
    public String calendarName(final String calendarHashKey){
        return split(calendarHashKey).get(1);
    }

    /**
     * @return the key of the set containing all calendar keys
     */
    public String calendarsSet(){
        return addPrefix("calendars");
    }

    /**
     * Add the configured prefix string to the given key
     * @param key the key to which the prefix should be prepended
     * @return a prefixed key
     */
    protected String addPrefix(String key){
        return prefix + key;
    }

    /**
     * Split a string on the configured delimiter
     * @param string the string to split
     * @return a list comprised of the split parts of the given string
     */
    protected List<String> split(final String string){
        return Arrays.asList(string.split(delimiter));
    }
}
