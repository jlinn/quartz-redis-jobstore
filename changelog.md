# Changelog
### 2019-07-02
* Upgrade to Jedis 3.0.1

### 2019-06-26
* Delete job data map set from Redis prior to storing new job data when updating / overwriting a job. 
This will prevent keys which were removed from the job's data map prior to storage from being preserved.

### 2018-03-01
* Detect dead schedulers and unblock their blocked triggers
* Keep track of `previousFireTime` for triggers

### 2018-02-27
* Set fire instance id on retrieved triggers
* Fixed a bug where trigger locks would get incorrectly removed for non-concurrent jobs

### 2016-12-30
* Fix a bug when handling trigger firing for triggers with no next fire time

### 2016-12-04
* Fixed handling of jobs marked with `@DisallowConcurrentExecution`.

### 2016-10-30
* Fix serialization of HolidayCalendar

### 2016-10-23
* Add support for storing trigger-specific job data

### 2016-05-04
* Add support for Redis password

### 2016-03-17
* Allow Redis db to be set when using Sentinel

### 2016-03-02
* Fix a bug where acquired triggers were not being released.

### 2016-01-31
* Add support for Redis Sentinel

### 2015-08-19
* Add support for [Jedis cluster](https://github.com/xetorthio/jedis#jedis-cluster).
* Allow a pre-configured Pool<Jedis> or JedisCluster to be passed in to RedisJobStore.
* Update to Jackson v2.6.1.

### 2014-12-09
* Remove Guava dependency

### 2014-09-24
* Add the ability to specify a redis database.
* Fix setter methods for `keyPrefix` and `keyDelimiter` properties.
* Set default port to 6379.

### 2014-08-21
* Fix a bug where non-durable jobs with only one trigger would be deleted when replaceTrigger() was called with that trigger.

### 2014-07-25
* Handle `ObjectAlreadyExistsException` separately in RedisJobStore::storeJobAndTrigger()

### 2014-07-24
* Enable the use of all GroupMatchers (not just EQUALS)