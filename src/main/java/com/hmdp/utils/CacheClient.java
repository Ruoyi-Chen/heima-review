package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @Author : Ruoyi Chen
 * @create 2022/12/15 22:47
 */
@Slf4j
@Component
public class CacheClient {

    @Autowired
    StringRedisTemplate redisTemplate;

    public void set(String key, Object value, Long time, TimeUnit unit) {
        redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        // 写入Redis
        redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R, ID> R queryWithPassThrough(String keyPrefex, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefex + id;
        // 1. 从redis里查询缓存
        String json = redisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        // 不存在 直接null
        if (!StringUtils.isEmpty(json)) {
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            // 返回错误信息
            return null;
        }

        // 4. 不存在，根据id查数据库
        R r = dbFallback.apply(id);
        // 5. 不存在，返回错误
        if (r == null) {
            // 空值写入redis
            redisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 6. 存在，写入redis
        this.set(key, JSONUtil.toJsonStr(r), time, unit);

        // 返回结果
        return r;
    }


    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    public <R, ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis里查询商铺缓存
        String shopJson = redisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        // 不存在 直接null
        if (StringUtils.isEmpty(shopJson)) {
            return null;
        }

        // 4. 命中 把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 如果未过期，直接返回店铺信息
            return r;
        }
        // 如果过期，需要缓存重建

        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock) {
            // 6.3 成功，开启独立线程，实现缓存重建


            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 获取锁成功，再次检测redis缓存是否存在，做DoubleCheck，如果存在则无需重建缓存
                    String ss = redisTemplate.opsForValue().get(key);
                    if (!StringUtils.isEmpty(ss)) {
                        // 3. 存在，直接返回
                        R rr = JSONUtil.toBean(shopJson, type);
                        return rr;
                    }
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                    return r1;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unlock(lockKey);
                }
            });
        }

        // 返回结果
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        redisTemplate.delete(key);
    }
}
