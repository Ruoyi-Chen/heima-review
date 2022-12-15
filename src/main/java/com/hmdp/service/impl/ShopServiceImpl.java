package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Autowired
    CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        Shop shop = cacheClient.queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_NULL_TTL, TimeUnit.SECONDS);

        // 互斥锁解决缓存击穿
//        Shop shop = queryWithMutex(id);

        // 逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpire(id);
//        Shop shop = cacheClient.queryWithLogicalExpire(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_NULL_TTL, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺信息不存在");
        }

        return Result.ok(shop);
    }

    public Shop queryWithMutex(long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis里查询商铺缓存
        String shopJson = redisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (!StringUtils.isEmpty(shopJson)) {
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // “/n/t”
        if (shopJson != null) {
            // 返回错误信息
            return null;
        }
        Shop shop = null;

        try {
            // 4. 实现缓存重建
            // 4.1 获取互斥锁
            String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
            boolean isLock = tryLock(lockKey);
            // 4.2 如果失败，休眠并重试
            if (!isLock) {
                Thread.sleep(50);
                queryWithMutex(id);
            }

            // 获取锁成功，再次检测redis缓存是否存在，做DoubleCheck，如果存在则无需重建缓存
            String sj = redisTemplate.opsForValue().get(key);
            if (!StringUtils.isEmpty(sj)) {
                // 3. 存在，直接返回
                Shop s = JSONUtil.toBean(shopJson, Shop.class);
                return s;
            }


            // 4. 不存在，根据id查数据库
            shop = getById(id);
            // 5. 不存在，返回错误
            if (shop == null) {
                // 空值写入redis
                redisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }

            // 6. 存在，写入redis
            redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7. 释放互斥锁
            unlock(key);
        }


        // 8. 返回
        return shop;
    }

    public Shop queryWithPassThrough(long id) {
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        // 1. 从redis里查询商铺缓存
        String shopJson = redisTemplate.opsForValue().get(key);

        // 2. 判断是否存在
        if (!StringUtils.isEmpty(shopJson)) {
            // 3. 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }
        // “/n/t”
        if (shopJson != null) {
            // 返回错误信息
            return null;
        }

        // 4. 不存在，根据id查数据库
        Shop shop = getById(id);
        // 5. 不存在，返回错误
        if (shop == null) {
            // 空值写入redis
            redisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        // 6. 存在，写入redis
        redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);



        // 返回结果
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    public Shop queryWithLogicalExpire(long id) {
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
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 如果未过期，直接返回店铺信息
            return shop;
        }
        // 如果过期，需要缓存重建

        // 6. 缓存重建
        // 6.1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2 判断是否获取锁成功
        if (isLock){
            // 6.3 成功，开启独立线程，实现缓存重建
            try {
                // 获取锁成功，再次检测redis缓存是否存在，做DoubleCheck，如果存在则无需重建缓存
                String sj = redisTemplate.opsForValue().get(key);
                if (!StringUtils.isEmpty(sj)) {
                    // 3. 存在，直接返回
                    Shop s = JSONUtil.toBean(shopJson, Shop.class);
                    return s;
                }

                CACHE_REBUILD_EXECUTOR.submit(() -> {
                   saveShop2Redis(id, 18000l);
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                unlock(lockKey);
            }
        }


        // 返回结果
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = redisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        redisTemplate.delete(key);
    }

    @Transactional
    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);

        // 2. 删除缓存
        redisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    private void saveShop2Redis(Long id, Long expireSeconds) {
        // 1. 查询店铺数据
        Shop shop = getById(id);

        // 2. 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // 3. 写入redis
        redisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
}
