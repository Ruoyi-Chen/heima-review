package com.hmdp;

import com.hmdp.service.impl.ShopServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@SpringBootTest
class HmDianPingApplicationTests {
    @Autowired
    CacheClient client;

    @Autowired
    ShopServiceImpl shopService;

    @Autowired
    RedisIdWorker redisIdWorker;

//    private ExecutorService es = Executors.newFixedThreadPool(500);
//
//    @Test
//    public void testIdWorker() throws InterruptedException {
//        CountDownLatch latch = new CountDownLatch(300);
//
//        Runnable task = () -> {
//            for (int i = 0; i < 100; i++) {
//                long order = redisIdWorker.nextId("order");
//                System.out.println("id = " + order);
//            }
//        };
//
//        long begin = System.currentTimeMillis();
//        for (int i = 0; i < 300; i++) {
//            es.submit(task);
//        }
//        latch.await();
//
//        long end = System.currentTimeMillis();
//        System.out.println("time = " + (end - begin));
//    }
}
