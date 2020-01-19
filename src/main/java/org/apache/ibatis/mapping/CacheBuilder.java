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
package org.apache.ibatis.mapping;

import org.apache.ibatis.builder.InitializingObject;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.CacheException;
import org.apache.ibatis.cache.decorators.*;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * {@link Cache} 构造器
 *
 * @author Clinton Begin
 */
public class CacheBuilder {

    /**
     * 编号。
     *
     * 目前看下来，是命名空间
     */
    private final String id;
    /**
     * 负责存储的 Cache 实现类
     */
    private Class<? extends Cache> implementation;
    /**
     * Cache 装饰类集合
     *
     * 例如，负责过期的 Cache 实现类
     */
    private final List<Class<? extends Cache>> decorators;
    /**
     * 缓存容器大小
     */
    private Integer size;
    /**
     * 清空缓存的频率。0 代表不清空
     */
    private Long clearInterval;
    /**
     * 是否序列化
     */
    private boolean readWrite;
    /**
     * Properties 对象
     */
    private Properties properties;
    /**
     * 是否阻塞
     */
    private boolean blocking;

    /**
     * 传入{@link Cache}的唯一标识（或者叫做namespace），为{@link CacheBuilder#id}，并new一个{@link ArrayList}对象设置到{@link CacheBuilder#decorators}
     *
     * @param id
     */
    public CacheBuilder(String id) {
        this.id = id;
        this.decorators = new ArrayList<>();
    }

    public CacheBuilder implementation(Class<? extends Cache> implementation) {
        this.implementation = implementation;
        return this;
    }

    public CacheBuilder addDecorator(Class<? extends Cache> decorator) {
        if (decorator != null) {
            this.decorators.add(decorator);
        }
        return this;
    }

    public CacheBuilder size(Integer size) {
        this.size = size;
        return this;
    }

    public CacheBuilder clearInterval(Long clearInterval) {
        this.clearInterval = clearInterval;
        return this;
    }

    public CacheBuilder readWrite(boolean readWrite) {
        this.readWrite = readWrite;
        return this;
    }

    public CacheBuilder blocking(boolean blocking) {
        this.blocking = blocking;
        return this;
    }

    public CacheBuilder properties(Properties properties) {
        this.properties = properties;
        return this;
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link #setDefaultImplementations()}设置基础{@link Cache}类和包装{@link Cache}类默认值
     *     </li>
     *     <li>
     *         调用{@link #newBaseCacheInstance(Class, String)}获取基础{@link Cache}对象
     *     </li>
     *     <li>
     *         调用{@link #setCacheProperties(Cache)}设置属性给基础{@link Cache}类，并执行一些可能针对该properties的操作
     *     </li>
     *     <li>
     *         判断基础{@link Cache}是否为{@link PerpetualCache}.class：
     *         <ul>
     *             <li>
     *                 是则遍历所有{@link #decorators}，调用{@link #newCacheDecoratorInstance(Class, Cache)}对基础{@link Cache}进行包装并执行{@link #setCacheProperties(Cache)}。完成遍历后调用{@link #setStandardDecorators(Cache)}传入经过设置properties的该基础{@link Cache}进行标准包装，然后返回包装后的{@link Cache}
     *             </li>
     *             <li>
     *                 否则判断基础{@link Cache}是否是{@link LoggingCache}：如果是则直接返回该基础{@link Cache}；否则使用{@link LoggingCache}包装该基础{@link Cache}后返回
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @return
     */
    public Cache build() {
        setDefaultImplementations();
        Cache cache = newBaseCacheInstance(implementation, id);
        setCacheProperties(cache);
        // issue #352, do not apply decorators to custom caches
        if (PerpetualCache.class.equals(cache.getClass())) {
            for (Class<? extends Cache> decorator : decorators) {
                cache = newCacheDecoratorInstance(decorator, cache);
                setCacheProperties(cache);
            }
            cache = setStandardDecorators(cache);
        } else if (!LoggingCache.class.isAssignableFrom(cache.getClass())) {
            cache = new LoggingCache(cache);
        }
        return cache;
    }

    /**
     * 如果{@link #implementation}是null，设置为{@link PerpetualCache}.class；如果{@link #decorators}是空的，添加一个元素{@link LruCache}.class
     */
    private void setDefaultImplementations() {
        if (implementation == null) {
            implementation = PerpetualCache.class;
            if (decorators.isEmpty()) {
                decorators.add(LruCache.class);
            }
        }
    }

    /**
     * 设置标准的包装器：
     * <ol>
     *     <li>
     *         如果{@link #size}不为null并且使用{@link SystemMetaObject#forObject(Object)}包装{@code cache}之后调用{@link MetaObject#hasSetter(String)}查询该{@code cache}对象是否存在"size"属性，如果存在，则直接将size设置给它
     *     </li>
     *     <li>
     *         如果{@link #clearInterval}不为null，则使用{@link ScheduledCache}包装{@code cache}，并调用{@link ScheduledCache#setClearInterval(long)}设置{@link #clearInterval}
     *     </li>
     *     <li>
     *         如果{@link #readWrite}不为null，则使用{@link SerializedCache}包装{@code cache}
     *     </li>
     *     <li>
     *         使用{@link LoggingCache}包装{@code cache}
     *     </li>
     *     <li>
     *         使用{@link SynchronizedCache}包装{@code cache}
     *     </li>
     *     <li>
     *         如果{@link #blocking}是true，则使用{@link BlockingCache}包装{@code cache}
     *     </li>
     * </ol>
     * 返回{@code cache}
     *
     * @param cache
     * @return
     */
    private Cache setStandardDecorators(Cache cache) {
        try {
            MetaObject metaCache = SystemMetaObject.forObject(cache);
            if (size != null && metaCache.hasSetter("size")) {
                metaCache.setValue("size", size);
            }
            if (clearInterval != null) {
                cache = new ScheduledCache(cache);
                ((ScheduledCache) cache).setClearInterval(clearInterval);
            }
            if (readWrite) {
                cache = new SerializedCache(cache);
            }
            cache = new LoggingCache(cache);
            cache = new SynchronizedCache(cache);
            if (blocking) {
                cache = new BlockingCache(cache);
            }
            return cache;
        } catch (Exception e) {
            throw new CacheException("Error building standard cache decorators.  Cause: " + e, e);
        }
    }

    /**
     * <ol>
     *     <li>
     *          如果{@link #properties}不为null，则对其进行遍历，使用{@link SystemMetaObject#forObject(Object)}对{@code cache}封装成一个{@link MetaObject}对象，
     *          调用其{@link MetaObject#hasSetter(String)}和{@link MetaObject#getSetterType(String)}判断该{@link Cache}对象是否拥有该property以及该property类型
     *          是否为String、int、long、short、byte、float、boolean、double。如果拥有该property并且类型正确是则设置给该{@link Cache}对象；如果没有该property则什么都不做；
     *          如果该property类型不正确则抛出异常。
     *     </li>
     *     <li>
     *         然后检查{@code cache}是否实现了{@link InitializingObject}，如果是则对其调用其所实现的{@link InitializingObject#initialize()}方法
     *     </li>
     * </ol>
     *
     * @param cache
     */
    private void setCacheProperties(Cache cache) {
        if (properties != null) {
            MetaObject metaCache = SystemMetaObject.forObject(cache);
            for (Map.Entry<Object, Object> entry : properties.entrySet()) {
                String name = (String) entry.getKey();
                String value = (String) entry.getValue();
                if (metaCache.hasSetter(name)) {
                    Class<?> type = metaCache.getSetterType(name);
                    if (String.class == type) {
                        metaCache.setValue(name, value);
                    } else if (int.class == type
                            || Integer.class == type) {
                        metaCache.setValue(name, Integer.valueOf(value));
                    } else if (long.class == type
                            || Long.class == type) {
                        metaCache.setValue(name, Long.valueOf(value));
                    } else if (short.class == type
                            || Short.class == type) {
                        metaCache.setValue(name, Short.valueOf(value));
                    } else if (byte.class == type
                            || Byte.class == type) {
                        metaCache.setValue(name, Byte.valueOf(value));
                    } else if (float.class == type
                            || Float.class == type) {
                        metaCache.setValue(name, Float.valueOf(value));
                    } else if (boolean.class == type
                            || Boolean.class == type) {
                        metaCache.setValue(name, Boolean.valueOf(value));
                    } else if (double.class == type
                            || Double.class == type) {
                        metaCache.setValue(name, Double.valueOf(value));
                    } else {
                        throw new CacheException("Unsupported property type for cache: '" + name + "' of type " + type);
                    }
                }
            }
        }
        if (InitializingObject.class.isAssignableFrom(cache.getClass())) {
            try {
                ((InitializingObject) cache).initialize();
            } catch (Exception e) {
                throw new CacheException("Failed cache initialization for '" +
                        cache.getId() + "' on '" + cache.getClass().getName() + "'", e);
            }
        }
    }

    /**
     * 调用{@link #getBaseCacheConstructor(Class)}获取携带{@link String}类型参数的{@code cacheClass}的构造函数（基础{@link Cache}），并调用{@link Constructor#newInstance(Object...)}传入{@code id}构建一个{@link Cache}对象并返回
     *
     * @param cacheClass Cache 类
     * @param id 编号
     * @return Cache 对象
     */
    private Cache newBaseCacheInstance(Class<? extends Cache> cacheClass, String id) {
        Constructor<? extends Cache> cacheConstructor = getBaseCacheConstructor(cacheClass);
        try {
            return cacheConstructor.newInstance(id);
        } catch (Exception e) {
            throw new CacheException("Could not instantiate cache implementation (" + cacheClass + "). Cause: " + e, e);
        }
    }

    /**
     *  return cacheClass.getConstructor(String.class);
     *
     * @param cacheClass Cache 类
     * @return 构造方法
     */
    private Constructor<? extends Cache> getBaseCacheConstructor(Class<? extends Cache> cacheClass) {
        try {
            return cacheClass.getConstructor(String.class);
        } catch (Exception e) {
            throw new CacheException("Invalid base cache implementation (" + cacheClass + ").  " +
                    "Base cache implementations must have a constructor that takes a String id as a parameter.  Cause: " + e, e);
        }
    }

    /**
     * 调用{@link #getCacheDecoratorConstructor(Class)}获取携带{@link Cache}类型参数的{@code cacheClass}的构造函数（包装{@link Cache}），并调用{@link Constructor#newInstance(Object...)}传入{@code base}对{@code base}进行包装成一个包装{@link Cache}对象并返回
     *
     * @param cacheClass 包装的 Cache 类
     * @param base 被包装的 Cache 对象
     * @return 包装后的 Cache 对象
     */
    private Cache newCacheDecoratorInstance(Class<? extends Cache> cacheClass, Cache base) {
        Constructor<? extends Cache> cacheConstructor = getCacheDecoratorConstructor(cacheClass);
        try {
            return cacheConstructor.newInstance(base);
        } catch (Exception e) {
            throw new CacheException("Could not instantiate cache decorator (" + cacheClass + "). Cause: " + e, e);
        }
    }

    /**
     *  return cacheClass.getConstructor(Cache.class);
     *
     * @param cacheClass 指定类
     * @return 构造方法
     */
    private Constructor<? extends Cache> getCacheDecoratorConstructor(Class<? extends Cache> cacheClass) {
        try {
            return cacheClass.getConstructor(Cache.class);
        } catch (Exception e) {
            throw new CacheException("Invalid cache decorator (" + cacheClass + ").  " +
                    "Cache decorators must have a constructor that takes a Cache instance as a parameter.  Cause: " + e, e);
        }
    }

}