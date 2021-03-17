package com.leammin.distributelock.lock;

import com.leammin.distributelock.SpringContextHolder;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.GenericToStringSerializer;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class RedisDistributeLock implements DistributeLock {
    private static final ThreadLocal<String> THREAD_ID = ThreadLocal.withInitial(() -> UUID.randomUUID().toString());

    private static final String LOCK_SCRIPT_STR = "" +
            "if redis.call('ZSCORE', KEYS[1], ARGV[3]) or redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) then " +
            "    redis.call('ZADD', KEYS[1], ARGV[2], ARGV[3]) " +
            "    redis.call('PEXPIRE', KEYS[1], ARGV[4]) " +
            "    return 1 " +
            "else " +
            "    return 0 " +
            "end ";
    private static final RedisScript<Boolean> LOCK_REDIS_SCRIPT = RedisScript.of(LOCK_SCRIPT_STR, Boolean.class);
    private static final GenericToStringSerializer<Boolean> BOOLEAN_REDIS_RESULT_SERIALIZER = new GenericToStringSerializer<>(Boolean.class);
    private static final GenericToStringSerializer<Object> TO_STRING_ARGV_SERIALIZER = new GenericToStringSerializer<>(Object.class);

    private static final long DEFAULT_TIMEOUT = 30 * 1000;
    private static final long DEFAULT_DELAY_TIME = DEFAULT_TIMEOUT / 3;
    private static final String RESET_TIME_SCRIPT_STR = "" +
            "if redis.call('ZSCORE', KEYS[1], ARGV[2]) then " +
            "    redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2]) " +
            "    redis.call('PEXPIRE', KEYS[1], ARGV[3]) " +
            "    return 1 " +
            "else " +
            "    return 0 " +
            "end ";
    private static final RedisScript<Boolean> RESET_TIME_REDIS_SCRIPT = RedisScript.of(RESET_TIME_SCRIPT_STR, Boolean.class);
    private final static ScheduledThreadPoolExecutor SCHEDULED_EXECUTOR_SERVICE = new ScheduledThreadPoolExecutor(1);
    private final static ConcurrentHashMap<String, ScheduledFuture<?>> TASKS = new ConcurrentHashMap<>();
    static {
        // 取消任务时，立即从队列中移除
        SCHEDULED_EXECUTOR_SERVICE.setRemoveOnCancelPolicy(true);
    }

    private final String key;
    private final int permits;
    private final long timeout;
    private final boolean autoDelay;

    private volatile static RedisTemplate<String, String> redisTemplate;

    public RedisDistributeLock(String key, int permits, long timeout) {
        // 三个参数相同才是同一把锁，否则如果key相同而permits或timeout不一样会互相影响出现异常。
        this.key = key + "_" + permits + "_" + timeout;
        this.permits = permits;
        // timeout 小于等于0，表示无限延长锁过期时间，系统意外中断时，30s后清除锁
        this.autoDelay = timeout <= 0;
        this.timeout = autoDelay ? DEFAULT_TIMEOUT : timeout;
    }

    public RedisDistributeLock(String key, int permits) {
        this(key, permits, 0);
    }

    public RedisDistributeLock(String key) {
        this(key, 1, 0);
    }


    @Override
    public boolean tryLock() {
        boolean success = acquire();
        if (success && autoDelay) {
            scheduleTask();
        }
        return success;
    }

    @Override
    public void unlock() {
        removeTask();
        release();
        THREAD_ID.remove();
    }

    private boolean acquire() {
        String threadId = THREAD_ID.get();
        long time = System.currentTimeMillis();
        RedisTemplate<String, String> redisTemplate = getRedisTemplate();
        // 移除超时线程id
        redisTemplate.boundZSetOps(key).removeRangeByScore(0, time - timeout);
        // 原子判断：如果 集合已存在此id（可重入） 或 集合数量小于 permits 则添加当前id并更新集合过期时间，否则加锁失败
        Boolean success = redisTemplate.execute(
                LOCK_REDIS_SCRIPT, TO_STRING_ARGV_SERIALIZER, BOOLEAN_REDIS_RESULT_SERIALIZER,
                Collections.singletonList(key),
                permits, time, threadId, timeout
        );
        return success != null && success;
    }

    private void scheduleTask() {
        // 添加定时任务：将此id的score更新为当前时间。避免此线程id过期
        // 默认过期时间为 30s，更新score频率为 10s 一次
        // 当系统意外中断时，30s后清除锁
        String threadId = THREAD_ID.get();
        String tasksKey = key + "_" + threadId;
        if (TASKS.get(tasksKey) == null) {
            ScheduledFuture<?> task = SCHEDULED_EXECUTOR_SERVICE.scheduleAtFixedRate(() -> {
                // 原子执行：如果集合中存在此id，则将此id的score更新为当前时间并更新集合过期时间。
                redisTemplate.execute(
                        RESET_TIME_REDIS_SCRIPT, TO_STRING_ARGV_SERIALIZER, BOOLEAN_REDIS_RESULT_SERIALIZER,
                        Collections.singletonList(key),
                        System.currentTimeMillis(), threadId, timeout
                );
            }, DEFAULT_DELAY_TIME, DEFAULT_DELAY_TIME, TimeUnit.MILLISECONDS);
            TASKS.put(tasksKey, task);
        }
    }

    private void removeTask() {
        String tasksKey = key + "_" + THREAD_ID.get();
        ScheduledFuture<?> future = TASKS.remove(tasksKey);
        if (future != null) {
            future.cancel(false);
        }
    }

    private void release() {
        getRedisTemplate().boundZSetOps(key).remove(THREAD_ID.get());
    }



    private static RedisTemplate<String, String> getRedisTemplate() {
        if (redisTemplate == null) {
            redisTemplate = SpringContextHolder.getBean("redisTemplate");
        }
        return redisTemplate;
    }
}
