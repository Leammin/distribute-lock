package com.leammin.distributelock.lock;

import com.leammin.distributelock.SpringContextHolder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

@SpringBootTest
class RedisDistributeLockTest {

    private void clear(String key) {
        RedisTemplate<String, Object> redisTemplate = SpringContextHolder.getBean("redisTemplate");
        redisTemplate.delete(key);
    }

    @Test
    void tryLock1() throws InterruptedException, ExecutionException {
        // 加锁测试
        String key = "tryLock1";
        int permits = 5;
        clear(key + permits + "1000");
        RedisDistributeLock lock = new RedisDistributeLock(key, permits, 1000);
        CountDownLatch countDownLatch = new CountDownLatch(permits);
        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<Boolean>> futures = new ArrayList<>(permits);
        for (int i = 0; i < permits; i++) {
            Future<Boolean> future = executorService.submit(() -> {
                boolean b = lock.tryLock();
                countDownLatch.countDown();
                return b;
            });
            futures.add(future);
        }
        countDownLatch.await();
        assert !lock.tryLock();
        for (Future<Boolean> future : futures) {
            assert future.get();
        }
    }

    @Test
    void tryLock2() throws InterruptedException {
        // 可重入测试
        int permits = 2;
        String key = "tryLock2";
        clear(key + permits + "1000");
        RedisDistributeLock lock = new RedisDistributeLock(key, permits, 1000);
        CountDownLatch countDownLatch = new CountDownLatch(1);
        new Thread(() -> {
            try {
                for (int i = 0; i < 10; i++) {
                    lock.tryLock();
                }
            }finally {
                countDownLatch.countDown();
            }
        }).start();
        countDownLatch.await();
        for (int i = 0; i < 10; i++) {
            assert lock.tryLock();
        }
    }

    @Test
    void tryLock3() throws InterruptedException {
        // 超时过期测试
        int permits = 1;
        String key = "tryLock3";
        clear(key + permits + "1000");
        RedisDistributeLock lock = new RedisDistributeLock(key, permits, 1000);
        Thread t = new Thread(lock::tryLock);
        t.start();
        t.join();
        // 锁仍未过期，加锁失败
        assert !lock.tryLock();
        Thread.sleep(500);
        assert !lock.tryLock();
        Thread.sleep(550);
        // 1s 后自动过期，加锁成功
        assert lock.tryLock();
    }

    @Test
    void tryLock4() throws InterruptedException {
        // 自动延期测试
        int permits = 1;
        String key = "tryLock4";
        clear(key + permits + "30000");
        RedisDistributeLock lock = new RedisDistributeLock(key, permits);
        CountDownLatch cdl1 = new CountDownLatch(1);
        CountDownLatch cdl2 = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            lock.tryLock();
            cdl1.countDown();
            try {
                cdl2.await();
                lock.unlock();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
        t.start();
        cdl1.await();
        // 自动延期，无论过去多久锁都不会过期
        for (int i = 0; i < 6; i++) {
            assert !lock.tryLock();
            Thread.sleep(10000);
        }
        assert !lock.tryLock();

        cdl2.countDown();
        t.join();
        // 解锁后，就能成功加锁
        assert lock.tryLock();
        Thread.sleep(30000);
        assert lock.tryLock();
        Thread.sleep(30000);

    }

    @Test
    void unLock1() throws InterruptedException {
        // 解锁测试
        int permits = 2;
        String key = "unLock1";
        clear(key + permits + "1000");
        RedisDistributeLock lock = new RedisDistributeLock(key, permits, 10000);
        CountDownLatch countDownLatch = new CountDownLatch(2);
        ReentrantLock reentrantLock = new ReentrantLock();
        Condition aCondition = reentrantLock.newCondition();
        Condition bCondition = reentrantLock.newCondition();
        Thread a = new Thread(() -> {

            try {
                lock.tryLock();
                countDownLatch.countDown();
                reentrantLock.lock();
                aCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
                reentrantLock.unlock();
            }
        });
        Thread b = new Thread(() -> {
            try {
                lock.tryLock();
                countDownLatch.countDown();
                reentrantLock.lock();
                bCondition.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
                reentrantLock.unlock();
            }
            lock.unlock();
        });
        a.start();
        b.start();
        countDownLatch.await();
        // a b 各获得一把锁，无剩余锁，加锁失败
        assert !lock.tryLock();
        try {
            reentrantLock.lock();
            // a 释放锁
            aCondition.signal();
        } finally {
            reentrantLock.unlock();
        }

        a.join();
        // a 释放后，剩余1 加锁成功
        assert lock.tryLock();
    }


}