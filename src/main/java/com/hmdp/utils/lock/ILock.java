package com.hmdp.utils.lock;

/**
 * @Author : Ruoyi Chen
 * @create 2022/12/17 19:39
 */
public interface ILock {
    boolean tryLock(long timeoutSec);
    void unlock();
}
