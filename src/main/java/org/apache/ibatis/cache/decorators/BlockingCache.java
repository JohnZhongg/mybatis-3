/**
 *    Copyright 2009-2019 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.cache.decorators;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple blocking decorator
 *
 * 阻塞（防止缓存为空时）的 Cache 装饰类。
 *
 *
 * Simple and inefficient version of EhCache's BlockingCache decorator.
 * It sets a lock over a cache key when the element is not found in cache.
 * This way, other threads will wait until this element is filled instead of hitting the database.
 *
 * @author Eduardo Macarron
 *
 */
public class BlockingCache implements Cache {

    /**
     * 阻塞等待超时时间
     */
    private long timeout;
    /**
     * 委托的 Cache 对象
     */
    private final Cache delegate;
    /**
     * 缓存键与 ReentrantLock 对象的映射
     */
    private final ConcurrentHashMap<Object, ReentrantLock> locks;

    public BlockingCache(Cache delegate) {
        this.delegate = delegate;
        this.locks = new ConcurrentHashMap<>();
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public int getSize() {
        return delegate.getSize();
    }

    @Override
    public void putObject(Object key, Object value) {
        try {
            delegate.putObject(key, value);
        } finally {
            // 当释放锁
            releaseLock(key);
        }
    }

    @Override
    public Object getObject(Object key) {
        // 获得key对应的锁，只有这里会去取锁，即获取同一个key的线程会被阻塞（直接挂起，不会被cpu调度，直到锁释放）在这里，ReentrantLock是可重入的非自旋锁
        acquireLock(key);
        // 获得缓存值
        Object value = delegate.getObject(key);
        // 1、只有value不是null才释放锁，否则直接跳过，由putObject方法新增一个非null的value给这个key的时候才会释放，或者调用removeObject直接释放锁
        // 2、第一次为null时，线程不会被阻塞，而是返回null，让当前用户得到一次去查数据库并设置缓存的机会，后面所有的线程都会被阻塞
        // 3、这样就从根本上实现了防止缓存击穿，但是问题是所有的线程都被挂起了，需要由用户来实现应该何时putObject和removeObject的策略
        if (value != null) {
            releaseLock(key);
        }
        return value;
    }

    @Override
    public Object removeObject(Object key) {
        // despite of its name, this method is called only to release locks
        // 释放锁
        releaseLock(key);
        return null;
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public ReadWriteLock getReadWriteLock() {
        return null;
    }

    /**
     * 获得 ReentrantLock 对象。如果不存在，进行添加
     *
     * @param key 缓存键
     * @return ReentrantLock 对象
     */
    private ReentrantLock getLockForKey(Object key) {
        ReentrantLock lock = new ReentrantLock();
        // 如果该key之前没有锁，就将这个新建的锁设置进去并返回null；如果已经存在，则不做设置操作返回已经存在的锁
        ReentrantLock previous = locks.putIfAbsent(key, lock);
        return previous == null ? lock : previous;
    }

    private void acquireLock(Object key) {
        // 获得 ReentrantLock 对象。
        Lock lock = getLockForKey(key);
        // 获得锁，直到超时
        if (timeout > 0) {
            try {
                boolean acquired = lock.tryLock(timeout, TimeUnit.MILLISECONDS);
                if (!acquired) {
                    throw new CacheException("Couldn't get a lock in " + timeout + " for the key " + key + " at the cache " + delegate.getId());
                }
            } catch (InterruptedException e) {
                throw new CacheException("Got interrupted while trying to acquire lock for key " + key, e);
            }
        } else {
            // 无超时时间直到获取到锁为止
            lock.lock();
        }
    }

    private void releaseLock(Object key) {
        // 获得 ReentrantLock 对象
        ReentrantLock lock = locks.get(key);
        // 如果当前线程持有，进行释放
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
        }
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

}