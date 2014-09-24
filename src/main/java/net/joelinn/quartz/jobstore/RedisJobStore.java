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
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.*;

/**
 * Joe Linn
 * 7/12/2014
 */
public class RedisJobStore implements JobStore {
    private static final Logger logger = LoggerFactory.getLogger(RedisJobStore.class);

    protected static JedisPool jedisPool;

    /**
     * Redis lock timeout in milliseconds
     */
    protected int lockTimeout = 30000;

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
     * Redis key prefix
     */
    protected String keyPrefix = "";

    /**
     * Redis key delimiter
     */
    protected String keyDelimiter = ":";

    protected String instanceId;

    protected RedisStorage storage;

    /**
     * Called by the QuartzScheduler before the <code>JobStore</code> is
     * used, in order to give the it a chance to initialize.
     *
     * @param loadHelper class loader helper
     * @param signaler schedule signaler object
     */
    @Override
    public void initialize(ClassLoadHelper loadHelper, SchedulerSignaler signaler) throws SchedulerConfigException {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPool = new JedisPool(jedisPoolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null, database);

        final RedisJobStoreSchema redisSchema = new RedisJobStoreSchema(keyPrefix, keyDelimiter);

        ObjectMapper mapper = new ObjectMapper();
        mapper.addMixInAnnotations(CronTrigger.class, CronTriggerMixin.class);
        mapper.addMixInAnnotations(SimpleTrigger.class, TriggerMixin.class);
        mapper.addMixInAnnotations(JobDetail.class, JobDetailMixin.class);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);

        storage = new RedisStorage(redisSchema, mapper, signaler, instanceId, lockTimeout);
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
     * Whether or not the <code>JobStore</code> implementation is clustered.
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
    public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger) throws ObjectAlreadyExistsException, JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.storeJob(newJob, false, jedis);
            storage.storeTrigger(newTrigger, false, jedis);
        }
        catch (ObjectAlreadyExistsException e){
            logger.info("Job and / or trigger already exist in storage.", e);
            throw e;
        }
        catch (Exception e){
            logger.error("Could not store job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
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
    public void storeJob(JobDetail newJob, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.storeJob(newJob, replaceExisting, jedis);
        }
        catch (ObjectAlreadyExistsException e){
            logger.info("Job hash already exists");
            throw e;
        }
        catch (Exception e){
            logger.error("Could not store job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public void storeJobsAndTriggers(Map<JobDetail, Set<? extends Trigger>> triggersAndJobs, boolean replace) throws ObjectAlreadyExistsException, JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            for (Map.Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
                storage.storeJob(entry.getKey(), replace, jedis);
                for (Trigger trigger : entry.getValue()) {
                    storage.storeTrigger((OperableTrigger) trigger, replace, jedis);
                }
            }
        }
        catch (Exception e){
            logger.error("Could not store jobs and triggers.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.removeJob(jobKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not remove job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public boolean removeJobs(List<JobKey> jobKeys) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            boolean removed = jobKeys.size() > 0;
            for (JobKey jobKey : jobKeys) {
                removed = storage.removeJob(jobKey, jedis) && removed;
            }
            return removed;
        }
        catch (Exception e){
            logger.error("Could not remove jobs.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * Retrieve the <code>{@link org.quartz.JobDetail}</code> for the given
     * <code>{@link org.quartz.Job}</code>.
     *
     * @param jobKey the {@link org.quartz.JobKey} describing the desired job
     * @return The desired <code>Job</code>, or null if there is no match.
     */
    @Override
    public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        JobDetail jobDetail = null;
        try{
            storage.waitForLock(jedis);
            jobDetail = storage.retrieveJob(jobKey, jedis);
        }
        catch (Exception e) {
            logger.error("Could not retrieve job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return  jobDetail;
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
    public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting) throws ObjectAlreadyExistsException, JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.storeTrigger(newTrigger, replaceExisting, jedis);
        }
        catch (ObjectAlreadyExistsException e){
            logger.info("Attempted to store a trigger which already exists.");
            throw e;
        }
        catch (Exception e){
            logger.error("Could not store trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public boolean removeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        boolean removed = false;
        try {
            storage.waitForLock(jedis);
            removed = storage.removeTrigger(triggerKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not remove trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return removed;
    }

    @Override
    public boolean removeTriggers(List<TriggerKey> triggerKeys) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        boolean removed = triggerKeys.size() > 0;
        try{
            storage.waitForLock(jedis);
            for (TriggerKey triggerKey : triggerKeys) {
                removed = storage.removeTrigger(triggerKey, jedis) && removed;
            }
        }
        catch (Exception e){
            logger.error("Could not remove trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return removed;
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
    public boolean replaceTrigger(TriggerKey triggerKey, OperableTrigger newTrigger) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.replaceTrigger(triggerKey, newTrigger, jedis);
        }
        catch (Exception e){
            logger.error("Could not remove trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
     *
     * @param triggerKey the key of the desired trigger
     * @return The desired <code>Trigger</code>, or null if there is no
     * match.
     */
    @Override
    public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        OperableTrigger trigger = null;
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            trigger = storage.retrieveTrigger(triggerKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not retrieve trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return trigger;
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
    public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            final boolean exists = storage.checkExists(jobKey, jedis);
            jedisPool.returnResource(jedis);
            return exists;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not check if job exists.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
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
    public boolean checkExists(TriggerKey triggerKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            final boolean exists = storage.checkExists(triggerKey, jedis);
            jedisPool.returnResource(jedis);
            return exists;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not check if trigger exists.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    /**
     * Clear (delete!) all scheduling data - all {@link org.quartz.Job}s, {@link org.quartz.Trigger}s
     * {@link org.quartz.Calendar}s.
     *
     * @throws org.quartz.JobPersistenceException
     */
    @Override
    public void clearAllSchedulingData() throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try {
            storage.waitForLock(jedis);
            storage.clearAllSchedulingData(jedis);
        }
        catch (Exception e){
            logger.error("Could not clear scheduling data.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public void storeCalendar(String name, Calendar calendar, boolean replaceExisting, boolean updateTriggers) throws ObjectAlreadyExistsException, JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.storeCalendar(name, calendar, replaceExisting, updateTriggers, jedis);
        }
        catch (Exception e){
            logger.error("Could not store calendar.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public boolean removeCalendar(String calName) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try {
            storage.waitForLock(jedis);
            return storage.removeCalendar(calName, jedis);
        }
        catch (Exception e){
            logger.error("Could not remove calendar.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * Retrieve the given <code>{@link org.quartz.Trigger}</code>.
     *
     * @param calName The name of the <code>Calendar</code> to be retrieved.
     * @return The desired <code>Calendar</code>, or null if there is no
     * match.
     */
    @Override
    public Calendar retrieveCalendar(String calName) throws JobPersistenceException {
        Calendar calendar = null;
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            calendar = storage.retrieveCalendar(calName, jedis);
        }
        catch (Exception e){
            logger.error("Could not retrieve calendar.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return calendar;
    }

    /**
     * Get the number of <code>{@link org.quartz.Job}</code> s that are
     * stored in the <code>JobsStore</code>.
     */
    @Override
    public int getNumberOfJobs() throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            int numberOfJobs = storage.getNumberOfJobs(jedis);
            jedisPool.returnResource(jedis);
            return numberOfJobs;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not get number of jobs.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    /**
     * Get the number of <code>{@link org.quartz.Trigger}</code> s that are
     * stored in the <code>JobsStore</code>.
     */
    @Override
    public int getNumberOfTriggers() throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            int numberOfJobs = storage.getNumberOfTriggers(jedis);
            jedisPool.returnResource(jedis);
            return numberOfJobs;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not get number of jobs.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
    }

    /**
     * Get the number of <code>{@link org.quartz.Calendar}</code> s that are
     * stored in the <code>JobsStore</code>.
     */
    @Override
    public int getNumberOfCalendars() throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            int numberOfJobs = storage.getNumberOfCalendars(jedis);
            jedisPool.returnResource(jedis);
            return numberOfJobs;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not get number of jobs.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
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
    public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.lock(jedis);
            return storage.getJobKeys(matcher, jedis);
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not retrieve JobKeys.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            Set<TriggerKey> triggerKeys = storage.getTriggerKeys(matcher, jedis);
            jedisPool.returnResource(jedis);
            return triggerKeys;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not retrieve TriggerKeys.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
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
        Jedis jedis = jedisPool.getResource();
        try{
            List<String> jobGroupNames = storage.getJobGroupNames(jedis);
            jedisPool.returnResource(jedis);
            return jobGroupNames;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not retrieve job group names.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
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
        Jedis jedis = jedisPool.getResource();
        try{
            List<String> triggerGroupNames = storage.getTriggerGroupNames(jedis);
            jedisPool.returnResource(jedis);
            return triggerGroupNames;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not retrieve trigger group names.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
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
        Jedis jedis = jedisPool.getResource();
        try{
            List<String> calendarNames = storage.getCalendarNames(jedis);
            jedisPool.returnResource(jedis);
            return calendarNames;
        }
        catch (JedisConnectionException e){
            logger.error("Redis connection error.", e);
            jedisPool.returnBrokenResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        catch (Exception e){
            logger.error("Could not retrieve calendar names.", e);
            jedisPool.returnResource(jedis);
            throw new JobPersistenceException(e.getMessage(), e);
        }
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
    public List<OperableTrigger> getTriggersForJob(JobKey jobKey) throws JobPersistenceException {
        List<OperableTrigger> triggers = new ArrayList<>();
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            triggers = storage.getTriggersForJob(jobKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not retrieve triggers for job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return triggers;
    }

    /**
     * Get the current state of the identified <code>{@link org.quartz.Trigger}</code>.
     *
     * @param triggerKey the key of the trigger for which to retrieve state
     * @see org.quartz.Trigger.TriggerState
     */
    @Override
    public Trigger.TriggerState getTriggerState(TriggerKey triggerKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.getTriggerState(triggerKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not retrieve trigger state.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * Pause the <code>{@link org.quartz.Trigger}</code> with the given key.
     *
     * @param triggerKey the key for the trigger to be paused
     * @see #resumeTrigger(org.quartz.TriggerKey)
     */
    @Override
    public void pauseTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.pauseTrigger(triggerKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not pause trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.pauseTriggers(matcher, jedis);
        }
        catch (Exception e){
            logger.error("Could not pause triggers.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    /**
     * Pause the <code>{@link org.quartz.Job}</code> with the given name - by
     * pausing all of its current <code>Trigger</code>s.
     *
     * @param jobKey the key of the job to be paused
     * @see #resumeJob(org.quartz.JobKey)
     */
    @Override
    public void pauseJob(JobKey jobKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.pauseJob(jobKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not pause job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.pauseJobs(groupMatcher, jedis);
        }
        catch (Exception e){
            logger.error("Could not pause jobs.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public void resumeTrigger(TriggerKey triggerKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.resumeTrigger(triggerKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not resume trigger.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.resumeTriggers(matcher, jedis);
        }
        catch (Exception e){
            logger.error("Could not resume trigger group(s).", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

    @Override
    public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            return storage.getPausedTriggerGroups(jedis);
        }
        catch (Exception e){
            logger.error("Could not retrieve paused trigger groups.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            jedisPool.returnResource(jedis);
        }
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
    public void resumeJob(JobKey jobKey) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.resumeJob(jobKey, jedis);
        }
        catch (Exception e){
            logger.error("Could not resume job.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.resumeJobs(matcher, jedis);
        }
        catch (Exception e){
            logger.error("Could not resume jobs.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.pauseAll(jedis);
        }
        catch (Exception e){
            logger.error("Could not pause all triggers.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.resumeAll(jedis);
        }
        catch (Exception e){
            logger.error("Could not resume all triggers.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
    public List<OperableTrigger> acquireNextTriggers(long noLaterThan, int maxCount, long timeWindow) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            return storage.acquireNextTriggers(noLaterThan, maxCount, timeWindow, jedis);
        }
        catch (Exception e){
            logger.error("Could not acquire next triggers.", e);
            throw new JobPersistenceException(e.getMessage(), e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
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
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.releaseAcquiredTrigger(trigger, jedis);
        }
        catch (Exception e){
            logger.error("Could not release acquired trigger.", e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
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
    public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers) throws JobPersistenceException {
        Jedis jedis = jedisPool.getResource();
        List<TriggerFiredResult> result = new ArrayList<>();
        try {
            storage.waitForLock(jedis);
            result = storage.triggersFired(triggers, jedis);
        }
        catch (Exception e){
            logger.error("Could not set triggers as fired.", e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
        return result;
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
    public void triggeredJobComplete(OperableTrigger trigger, JobDetail jobDetail, Trigger.CompletedExecutionInstruction triggerInstCode) {
        Jedis jedis = jedisPool.getResource();
        try{
            storage.waitForLock(jedis);
            storage.triggeredJobComplete(trigger, jobDetail, triggerInstCode, jedis);
        }
        catch (Exception e){
            logger.error("Could not handle job completion.", e);
        }
        finally {
            storage.unlock(jedis);
            jedisPool.returnResource(jedis);
        }
    }

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

    public void setDatabase(short database) {
        this.database = database;
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
}
