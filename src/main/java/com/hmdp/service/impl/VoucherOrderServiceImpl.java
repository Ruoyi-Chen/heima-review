package com.hmdp.service.impl;

import com.baomidou.mybatisplus.core.toolkit.IdWorker;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    RedisIdWorker idWorker;
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    @Autowired
    RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 当前类初始化完毕就执行
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                // 1. 获取队列中的订单信息
                try {
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2. 创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常 {}", e);
                }
            }
    }

    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 1. 获取用户
        Long userId = voucherOrder.getUserId();

        // 2. 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);

        // 3. 获取锁
        boolean isLock = lock.tryLock();
        // 4. 判断锁是否获取成功
        if (!isLock) {
            // 获取锁失败 返回错误或者重试
            log.error("不允许重复下单");
            return;
        }
        // 获取和事务有关的代理对象，如果不用事务，就会出现事务失效（本类中调用本类事务方法）
        try{
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();

        // 1. 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.EMPTY_LIST,
                voucherId.toString(),
                userId.toString()
        );

        // 2. 判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            // 2.1 不为0， 没有购买资格
            return Result.fail(r == 1? "库存不足" : "不能重复下单");
        }
        // 2.2 为0， 有购买资格 把下单信息保存到阻塞队列
        long orderId = idWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);
        // 获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // TODO 保存到阻塞队列
        orderTasks.add(voucherOrder);


        // 3. 返回订单id
        return Result.ok(orderId);
    }



//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1. 查询优惠券信息
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 2. 判断秒杀是否开始
//        LocalDateTime now = LocalDateTime.now();
//        if (voucher.getBeginTime().isAfter(now)) {
//            return Result.fail("秒杀未开始");
//        }
//
//        // 3. 判断秒杀是否已结束
//        if (voucher.getEndTime().isBefore(now)) {
//            return Result.fail("秒杀已结束");
//        }
//
//        // 4. 判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足！");
//        }
//
//        // 5. 扣减库存
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")
//                .eq("voucher_id", voucherId)
////                .eq("stock", voucher.getStock()) // 乐观锁
//                .gt("stock", 0)
//                .update();
//        if (!success) {
//            return Result.fail("库存扣减失败！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
////        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//        RLock lock = redissonClient.getLock("order:" + userId);
//
//        boolean isLock = lock.tryLock();
//        if (!isLock) {
//            // 获取锁失败 返回错误或者重试
//            return Result.fail("不允许重复下单");
//        }
////        synchronized (userId.toString().intern()) {
//        // 获取和事务有关的代理对象，如果不用事务，就会出现事务失效（本类中调用本类事务方法）
//        try{
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            lock.unlock();
//        }
////        }
//
//    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 一人一单
        Long userId = UserHolder.getUser().getId();

        // 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 1) {
//            return Result.fail("一人一单");
            return;
        }

        // 5. 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId())
//                .eq("stock", voucher.getStock()) // 乐观锁
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存扣减失败！");
            return;
        }

        // 6. 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单id
        long orderId = idWorker.nextId("order");
//        voucherOrder.setId(orderId);
//        // 用户id
//        voucherOrder.setUserId(userId);
//        // 代金券id
//        voucherOrder.setVoucherId(voucherId);
        save(voucherOrder);
        // 返回订单id

    }
}
