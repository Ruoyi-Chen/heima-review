package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    StringRedisTemplate redisTemplate;

    @Override
    public Result queryTypeList() {
        String key = "shop:list";
        // 1. 先从缓存中找
//        List list = redisTemplate.opsForList().range(key, 0, -1);
        String json = redisTemplate.opsForValue().get(key);
        List<ShopType> list = JSONUtil.toList(json, ShopType.class);

        // 1.1 如果缓存中有， 直接返回
        if (!list.isEmpty() && list.size() > 0) {
            return Result.ok(list);
        }

        // 2. 如果缓存中没有， 去数据库查
        log.info("从数据库查询店铺list");
        List<ShopType> typeList = this.query().orderByAsc("sort").list();

        // 如果为空
        if (typeList.isEmpty() || typeList.size() == 0) {
            return Result.fail("没有数据");
        }

        // 将数据写入缓存
//        redisTemplate.opsForList().rightPushAll(key, typeList);
        redisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(typeList));

        // 返回数据
        return Result.ok(typeList);
    }


}
