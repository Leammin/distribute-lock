package com.leammin.distributelock.lock;

public interface DistributeLock {

    boolean tryLock();

    void unlock();
}
