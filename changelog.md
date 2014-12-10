# Changelog
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