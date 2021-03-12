package net.joelinn.quartz.jobstore.jedis;

import redis.clients.jedis.*;
import redis.clients.jedis.commands.JedisCommands;
import redis.clients.jedis.params.GeoRadiusParam;
import redis.clients.jedis.params.SetParams;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.params.ZIncrByParams;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unfortunately, {@link JedisCluster} does not implement the {@link JedisCommands} interface, even though the vast
 * majority of its method signatures line up.  This class works around that issue.  Hopefully future versions of Jedis
 * will render this unnecessary.
 * @author Joe Linn
 * 7/2/2019
 */
public class JedisClusterCommandsWrapper implements JedisCommands {
    private final JedisCluster cluster;

    public JedisClusterCommandsWrapper(JedisCluster cluster) {
        this.cluster = cluster;
    }


    @Override
    public String set(String s, String s1) {
        return cluster.set(s, s1);
    }

    @Override
    public String set(String s, String s1, SetParams setParams) {
        return cluster.set(s, s1, setParams);
    }

    @Override
    public String get(String s) {
        return cluster.get(s);
    }

    @Override
    public Boolean exists(String s) {
        return cluster.exists(s);
    }

    @Override
    public Long persist(String s) {
        return cluster.persist(s);
    }

    @Override
    public String type(String s) {
        return cluster.type(s);
    }

    @Override
    public byte[] dump(String s) {
        return cluster.dump(s);
    }

    @Override
    public String restore(String s, int i, byte[] bytes) {
        return cluster.restore(s, i, bytes);
    }

    @Override
    public String restoreReplace(String s, int i, byte[] bytes) {
        return null;
    }

    @Override
    public Long expire(String s, int i) {
        return cluster.expire(s, i);
    }

    @Override
    public Long pexpire(String s, long l) {
        return cluster.pexpire(s, l);
    }

    @Override
    public Long expireAt(String s, long l) {
        return cluster.expireAt(s, l);
    }

    @Override
    public Long pexpireAt(String s, long l) {
        return cluster.pexpireAt(s, l);
    }

    @Override
    public Long ttl(String s) {
        return cluster.ttl(s);
    }

    @Override
    public Long pttl(String s) {
        return cluster.pttl(s);
    }

    @Override
    public Long touch(String s) {
        return cluster.touch(s);
    }

    @Override
    public Boolean setbit(String s, long l, boolean b) {
        return cluster.setbit(s, l, b);
    }

    @Override
    public Boolean setbit(String s, long l, String s1) {
        return cluster.setbit(s, l, s1);
    }

    @Override
    public Boolean getbit(String s, long l) {
        return cluster.getbit(s, l);
    }

    @Override
    public Long setrange(String s, long l, String s1) {
        return cluster.setrange(s, l, s1);
    }

    @Override
    public String getrange(String s, long l, long l1) {
        return cluster.getrange(s, l, l1);
    }

    @Override
    public String getSet(String s, String s1) {
        return cluster.getSet(s, s1);
    }

    @Override
    public Long setnx(String s, String s1) {
        return cluster.setnx(s, s1);
    }

    @Override
    public String setex(String s, int i, String s1) {
        return cluster.setex(s, i, s1);
    }

    @Override
    public String psetex(String s, long l, String s1) {
        return cluster.psetex(s, l, s1);
    }

    @Override
    public Long decrBy(String s, long l) {
        return cluster.decrBy(s, l);
    }

    @Override
    public Long decr(String s) {
        return cluster.decr(s);
    }

    @Override
    public Long incrBy(String s, long l) {
        return cluster.incrBy(s, l);
    }

    @Override
    public Double incrByFloat(String s, double v) {
        return cluster.incrByFloat(s, v);
    }

    @Override
    public Long incr(String s) {
        return cluster.incr(s);
    }

    @Override
    public Long append(String s, String s1) {
        return cluster.append(s, s1);
    }

    @Override
    public String substr(String s, int i, int i1) {
        return cluster.substr(s, i, i1);
    }

    @Override
    public Long hset(String s, String s1, String s2) {
        return cluster.hset(s, s1, s2);
    }

    @Override
    public Long hset(String s, Map<String, String> map) {
        return cluster.hset(s, map);
    }

    @Override
    public String hget(String s, String s1) {
        return cluster.hget(s, s1);
    }

    @Override
    public Long hsetnx(String s, String s1, String s2) {
        return cluster.hsetnx(s, s1, s2);
    }

    @Override
    public String hmset(String s, Map<String, String> map) {
        return cluster.hmset(s, map);
    }

    @Override
    public List<String> hmget(String s, String... strings) {
        return cluster.hmget(s, strings);
    }

    @Override
    public Long hincrBy(String s, String s1, long l) {
        return cluster.hincrBy(s, s1, l);
    }

    @Override
    public Double hincrByFloat(String s, String s1, double v) {
        return cluster.hincrByFloat(s.getBytes(), s1.getBytes(), v);
    }

    @Override
    public Boolean hexists(String s, String s1) {
        return cluster.hexists(s, s1);
    }

    @Override
    public Long hdel(String s, String... strings) {
        return cluster.hdel(s, strings);
    }

    @Override
    public Long hlen(String s) {
        return cluster.hlen(s);
    }

    @Override
    public Set<String> hkeys(String s) {
        return cluster.hkeys(s);
    }

    @Override
    public List<String> hvals(String s) {
        return cluster.hvals(s);
    }

    @Override
    public Map<String, String> hgetAll(String s) {
        return cluster.hgetAll(s);
    }

    @Override
    public Long rpush(String s, String... strings) {
        return cluster.rpush(s, strings);
    }

    @Override
    public Long lpush(String s, String... strings) {
        return cluster.lpush(s, strings);
    }

    @Override
    public Long llen(String s) {
        return cluster.llen(s);
    }

    @Override
    public List<String> lrange(String s, long l, long l1) {
        return cluster.lrange(s, l, l1);
    }

    @Override
    public String ltrim(String s, long l, long l1) {
        return cluster.ltrim(s, l, l1);
    }

    @Override
    public String lindex(String s, long l) {
        return cluster.lindex(s, l);
    }

    @Override
    public String lset(String s, long l, String s1) {
        return cluster.lset(s, l, s1);
    }

    @Override
    public Long lrem(String s, long l, String s1) {
        return cluster.lrem(s, l, s1);
    }

    @Override
    public String lpop(String s) {
        return cluster.lpop(s);
    }

    @Override
    public String rpop(String s) {
        return cluster.rpop(s);
    }

    @Override
    public Long sadd(String s, String... strings) {
        return cluster.sadd(s, strings);
    }

    @Override
    public Set<String> smembers(String s) {
        return cluster.smembers(s);
    }

    @Override
    public Long srem(String s, String... strings) {
        return cluster.srem(s, strings);
    }

    @Override
    public String spop(String s) {
        return cluster.spop(s);
    }

    @Override
    public Set<String> spop(String s, long l) {
        return cluster.spop(s, l);
    }

    @Override
    public Long scard(String s) {
        return cluster.scard(s);
    }

    @Override
    public Boolean sismember(String s, String s1) {
        return cluster.sismember(s, s1);
    }

    @Override
    public String srandmember(String s) {
        return cluster.srandmember(s);
    }

    @Override
    public List<String> srandmember(String s, int i) {
        return cluster.srandmember(s, i);
    }

    @Override
    public Long strlen(String s) {
        return cluster.strlen(s);
    }

    @Override
    public Long zadd(String s, double v, String s1) {
        return cluster.zadd(s, v, s1);
    }

    @Override
    public Long zadd(String s, double v, String s1, ZAddParams zAddParams) {
        return cluster.zadd(s, v, s1, zAddParams);
    }

    @Override
    public Long zadd(String s, Map<String, Double> map) {
        return cluster.zadd(s, map);
    }

    @Override
    public Long zadd(String s, Map<String, Double> map, ZAddParams zAddParams) {
        return cluster.zadd(s, map, zAddParams);
    }

    @Override
    public Set<String> zrange(String s, long l, long l1) {
        return cluster.zrange(s, l, l1);
    }

    @Override
    public Long zrem(String s, String... strings) {
        return cluster.zrem(s, strings);
    }

    @Override
    public Double zincrby(String s, double v, String s1) {
        return cluster.zincrby(s, v, s1);
    }

    @Override
    public Double zincrby(String s, double v, String s1, ZIncrByParams zIncrByParams) {
        return cluster.zincrby(s, v, s1, zIncrByParams);
    }

    @Override
    public Long zrank(String s, String s1) {
        return cluster.zrank(s, s1);
    }

    @Override
    public Long zrevrank(String s, String s1) {
        return cluster.zrevrank(s, s1);
    }

    @Override
    public Set<String> zrevrange(String s, long l, long l1) {
        return cluster.zrevrange(s, l, l1);
    }

    @Override
    public Set<Tuple> zrangeWithScores(String s, long l, long l1) {
        return cluster.zrangeWithScores(s, l, l1);
    }

    @Override
    public Set<Tuple> zrevrangeWithScores(String s, long l, long l1) {
        return cluster.zrevrangeWithScores(s, l, l1);
    }

    @Override
    public Long zcard(String s) {
        return cluster.zcard(s);
    }

    @Override
    public Double zscore(String s, String s1) {
        return cluster.zscore(s, s1);
    }

    @Override
    public List<String> sort(String s) {
        return cluster.sort(s);
    }

    @Override
    public List<String> sort(String s, SortingParams sortingParams) {
        return cluster.sort(s, sortingParams);
    }

    @Override
    public Long zcount(String s, double v, double v1) {
        return cluster.zcount(s, v, v1);
    }

    @Override
    public Long zcount(String s, String s1, String s2) {
        return cluster.zcount(s, s1, s1);
    }

    @Override
    public Set<String> zrangeByScore(String s, double v, double v1) {
        return cluster.zrangeByScore(s, v, v1);
    }

    @Override
    public Set<String> zrangeByScore(String s, String s1, String s2) {
        return cluster.zrangeByScore(s, s1, s2);
    }

    @Override
    public Set<String> zrevrangeByScore(String s, double v, double v1) {
        return cluster.zrevrangeByScore(s, v, v1);
    }

    @Override
    public Set<String> zrangeByScore(String s, double v, double v1, int i, int i1) {
        return cluster.zrangeByScore(s, v, v1, i, i1);
    }

    @Override
    public Set<String> zrevrangeByScore(String s, String s1, String s2) {
        return cluster.zrevrangeByScore(s, s1, s2);
    }

    @Override
    public Set<String> zrangeByScore(String s, String s1, String s2, int i, int i1) {
        return cluster.zrangeByScore(s, s1, s2, i, i1);
    }

    @Override
    public Set<String> zrevrangeByScore(String s, double v, double v1, int i, int i1) {
        return cluster.zrevrangeByScore(s, v, v1, i, i1);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String s, double v, double v1) {
        return cluster.zrangeByScoreWithScores(s, v, v1);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String s, double v, double v1) {
        return cluster.zrevrangeByScoreWithScores(s, v, v1);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String s, double v, double v1, int i, int i1) {
        return cluster.zrangeByScoreWithScores(s, v, v1, i, i1);
    }

    @Override
    public Set<String> zrevrangeByScore(String s, String s1, String s2, int i, int i1) {
        return cluster.zrevrangeByScore(s, s1, s2, i, i1);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String s, String s1, String s2) {
        return cluster.zrangeByScoreWithScores(s, s1, s2);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String s, String s1, String s2) {
        return cluster.zrevrangeByScoreWithScores(s, s1, s2);
    }

    @Override
    public Set<Tuple> zrangeByScoreWithScores(String s, String s1, String s2, int i, int i1) {
        return cluster.zrangeByScoreWithScores(s, s1, s2, i , i1);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String s, double v, double v1, int i, int i1) {
        return cluster.zrevrangeByScoreWithScores(s, v, v1, i, i1);
    }

    @Override
    public Set<Tuple> zrevrangeByScoreWithScores(String s, String s1, String s2, int i, int i1) {
        return cluster.zrevrangeByScoreWithScores(s, s1, s2, i, i1);
    }

    @Override
    public Long zremrangeByRank(String s, long l, long l1) {
        return cluster.zremrangeByRank(s, l, l1);
    }

    @Override
    public Long zremrangeByScore(String s, double v, double v1) {
        return cluster.zremrangeByScore(s, v, v1);
    }

    @Override
    public Long zremrangeByScore(String s, String s1, String s2) {
        return cluster.zremrangeByScore(s, s1, s2);
    }

    @Override
    public Long zlexcount(String s, String s1, String s2) {
        return cluster.zlexcount(s, s1, s2);
    }

    @Override
    public Set<String> zrangeByLex(String s, String s1, String s2) {
        return cluster.zrangeByLex(s, s1, s2);
    }

    @Override
    public Set<String> zrangeByLex(String s, String s1, String s2, int i, int i1) {
        return cluster.zrangeByLex(s, s1, s2, i, i1);
    }

    @Override
    public Set<String> zrevrangeByLex(String s, String s1, String s2) {
        return cluster.zrevrangeByLex(s, s1, s2);
    }

    @Override
    public Set<String> zrevrangeByLex(String s, String s1, String s2, int i, int i1) {
        return cluster.zrevrangeByLex(s, s1, s2, i, i1);
    }

    @Override
    public Long zremrangeByLex(String s, String s1, String s2) {
        return cluster.zremrangeByLex(s, s1, s2);
    }

    @Override
    public Long linsert(String s, ListPosition listPosition, String s1, String s2) {
        return cluster.linsert(s, listPosition, s1, s2);
    }

    @Override
    public Long lpushx(String s, String... strings) {
        return cluster.lpushx(s, strings);
    }

    @Override
    public Long rpushx(String s, String... strings) {
        return cluster.rpushx(s, strings);
    }

    @Override
    public List<String> blpop(int i, String s) {
        return cluster.blpop(i, s);
    }

    @Override
    public List<String> brpop(int i, String s) {
        return cluster.brpop(i, s);
    }

    @Override
    public Long del(String s) {
        return cluster.del(s);
    }

    @Override
    public Long unlink(String s) {
        return cluster.unlink(s);
    }

    @Override
    public String echo(String s) {
        return cluster.echo(s);
    }

    @Override
    public Long move(String s, int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long bitcount(String s) {
        return cluster.bitcount(s);
    }

    @Override
    public Long bitcount(String s, long l, long l1) {
        return cluster.bitcount(s, l, l1);
    }

    @Override
    public Long bitpos(String s, boolean b) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long bitpos(String s, boolean b, BitPosParams bitPosParams) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String s, String s1) {
        return cluster.hscan(s, s1);
    }

    @Override
    public ScanResult<Map.Entry<String, String>> hscan(String s, String s1, ScanParams scanParams) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ScanResult<String> sscan(String s, String s1) {
        return cluster.sscan(s, s1);
    }

    @Override
    public ScanResult<Tuple> zscan(String s, String s1) {
        return cluster.zscan(s, s1);
    }

    @Override
    public ScanResult<Tuple> zscan(String s, String s1, ScanParams scanParams) {
        return cluster.zscan(s.getBytes(), s1.getBytes(), scanParams);
    }

    @Override
    public ScanResult<String> sscan(String s, String s1, ScanParams scanParams) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long pfadd(String s, String... strings) {
        return cluster.pfadd(s, strings);
    }

    @Override
    public long pfcount(String s) {
        return cluster.pfcount(s);
    }

    @Override
    public Long geoadd(String s, double v, double v1, String s1) {
        return cluster.geoadd(s, v, v1, s1);
    }

    @Override
    public Long geoadd(String s, Map<String, GeoCoordinate> map) {
        return cluster.geoadd(s, map);
    }

    @Override
    public Double geodist(String s, String s1, String s2) {
        return cluster.geodist(s, s1, s2);
    }

    @Override
    public Double geodist(String s, String s1, String s2, GeoUnit geoUnit) {
        return cluster.geodist(s, s1, s2, geoUnit);
    }

    @Override
    public List<String> geohash(String s, String... strings) {
        return cluster.geohash(s, strings);
    }

    @Override
    public List<GeoCoordinate> geopos(String s, String... strings) {
        return cluster.geopos(s, strings);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String s, double v, double v1, double v2, GeoUnit geoUnit) {
        return cluster.georadius(s, v, v1, v2, geoUnit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String s, double v, double v1, double v2, GeoUnit geoUnit) {
        return cluster.georadiusReadonly(s, v, v1, v2, geoUnit);
    }

    @Override
    public List<GeoRadiusResponse> georadius(String s, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return cluster.georadiusReadonly(s, v, v1, v2, geoUnit, geoRadiusParam);
    }

    @Override
    public List<GeoRadiusResponse> georadiusReadonly(String s, double v, double v1, double v2, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return cluster.georadiusReadonly(s, v, v1, v2, geoUnit, geoRadiusParam);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String s, String s1, double v, GeoUnit geoUnit) {
        return cluster.georadiusByMember(s, s1, v, geoUnit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String s, String s1, double v, GeoUnit geoUnit) {
        return cluster.georadiusByMemberReadonly(s, s1, v, geoUnit);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMember(String s, String s1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return cluster.georadiusByMember(s, s1, v, geoUnit, geoRadiusParam);
    }

    @Override
    public List<GeoRadiusResponse> georadiusByMemberReadonly(String s, String s1, double v, GeoUnit geoUnit, GeoRadiusParam geoRadiusParam) {
        return cluster.georadiusByMemberReadonly(s, s1, v, geoUnit, geoRadiusParam);
    }

    @Override
    public List<Long> bitfield(String s, String... strings) {
        return cluster.bitfield(s, strings);
    }

    @Override
    public Long hstrlen(String s, String s1) {
        return cluster.hstrlen(s, s1);
    }


    @Override
    public Tuple zpopmax(String key) {
        return cluster.zpopmax(key);
    }

    @Override
    public Set<Tuple> zpopmax(String key, int count) {
        return cluster.zpopmax(key, count);
    }

    @Override
    public Tuple zpopmin(String key) {
        return cluster.zpopmin(key);
    }

    @Override
    public Set<Tuple> zpopmin(String key, int count) {
        return cluster.zpopmin(key, count);
    }

    @Override
    public List<Long> bitfieldReadonly(String key, String... arguments) {
        return cluster.bitfieldReadonly(key, arguments);
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash) {
        return cluster.xadd(key, id, hash);
    }

    @Override
    public StreamEntryID xadd(String key, StreamEntryID id, Map<String, String> hash, long maxLen, boolean approximateLength) {
        return cluster.xadd(key, id, hash, maxLen, approximateLength);
    }

    @Override
    public Long xlen(String key) {
        return cluster.xlen(key);
    }

    @Override
    public List<StreamEntry> xrange(String key, StreamEntryID start, StreamEntryID end, int count) {
        return cluster.xrange(key, start, end, count);
    }

    @Override
    public List<StreamEntry> xrevrange(String key, StreamEntryID end, StreamEntryID start, int count) {
        return cluster.xrevrange(key, end, start, count);
    }

    @Override
    public long xack(String key, String group, StreamEntryID... ids) {
        return cluster.xack(key, group, ids);
    }

    @Override
    public String xgroupCreate(String key, String groupname, StreamEntryID id, boolean makeStream) {
        return cluster.xgroupCreate(key, groupname, id, makeStream);
    }

    @Override
    public String xgroupSetID(String key, String groupname, StreamEntryID id) {
        return cluster.xgroupSetID(key, groupname, id);
    }

    @Override
    public long xgroupDestroy(String key, String groupname) {
        return cluster.xgroupDestroy(key, groupname);
    }

    @Override
    public Long xgroupDelConsumer(String key, String groupname, String consumername) {
        return cluster.xgroupDelConsumer(key, groupname, consumername);
    }

    @Override
    public List<StreamPendingEntry> xpending(String key, String groupname, StreamEntryID start, StreamEntryID end, int count, String consumername) {
        return cluster.xpending(key, groupname, start, end, count, consumername);
    }

    @Override
    public long xdel(String key, StreamEntryID... ids) {
        return cluster.xdel(key, ids);
    }

    @Override
    public long xtrim(String key, long maxLen, boolean approximate) {
        return cluster.xtrim(key, maxLen, approximate);
    }

    @Override
    public List<StreamEntry> xclaim(String key, String group, String consumername, long minIdleTime, long newIdleTime, int retries, boolean force, StreamEntryID... ids) {
        return cluster.xclaim(key, group, consumername, minIdleTime, newIdleTime, retries, force, ids);
    }

    @Override
    public StreamInfo xinfoStream(String key) {
        throw new UnsupportedOperationException("xinfoStream not supported.");
    }

    @Override
    public List<StreamGroupInfo> xinfoGroup(String key) {
        throw new UnsupportedOperationException("xinfoGroup not supported.");
    }

    @Override
    public List<StreamConsumersInfo> xinfoConsumers(String key, String group) {
        throw new UnsupportedOperationException("xinfoConsumers not supported");
    }
}
