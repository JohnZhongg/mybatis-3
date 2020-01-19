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
package org.apache.ibatis.binding;

import org.apache.ibatis.builder.annotation.MapperAnnotationBuilder;
import org.apache.ibatis.io.ResolverUtil;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;

import java.util.*;

/**
 * Mapper 注册表
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Lasse Voss
 */
public class MapperRegistry {

    /**
     *  全局 {@link Configuration}对象
     */
    private final Configuration config;
    /**
     * KEY：Mapper 接口的class对象<br>
     * VALUE：对应的{@link MapperProxyFactory}对象
     */
    private final Map<Class<?>, MapperProxyFactory<?>> knownMappers = new HashMap<>();

    /**
     * {@code config}赋值到{@link #config}
     *
     * @param config 全局 {@link Configuration}对象
     */
    public MapperRegistry(Configuration config) {
        this.config = config;
    }

    @SuppressWarnings("unchecked")
    public <T> T getMapper(Class<T> type, SqlSession sqlSession) {
        // 获得 MapperProxyFactory 对象
        final MapperProxyFactory<T> mapperProxyFactory = (MapperProxyFactory<T>) knownMappers.get(type);
        // 不存在，则抛出 BindingException 异常
        if (mapperProxyFactory == null) {
            throw new BindingException("Type " + type + " is not known to the MapperRegistry.");
        }
        // 创建 Mapper Proxy 对象
        try {
            return mapperProxyFactory.newInstance(sqlSession);
        } catch (Exception e) {
            throw new BindingException("Error getting mapper instance. Cause: " + e, e);
        }
    }

    /**
     * 判断{@link #knownMappers}是否已经包含了{@code type}对应的mapper了
     *
     * @param type
     * @param <T>
     * @return
     */
    public <T> boolean hasMapper(Class<T> type) {
        return knownMappers.containsKey(type);
    }

    /**
     * <ul>
     *     调用{@code type}的{@link Class#isInterface()}，判断其是否是接口：
     *     <li>
     *         是：
     *         <ol>
     *             <li>
     *                 调用{@link #hasMapper(Class)}传入{@code type}，判断当前mapper的{@link Class}是否已经加载：
     *                 <ul>
     *                     <li>
     *                         是：直接抛出异常{@link BindingException}
     *                     </li>
     *                     <li>
     *                         否：什么也不做，继续往下走
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                  调用构造器{@link MapperProxyFactory#MapperProxyFactory(Class)}传入{@code type}构造一个{@link MapperProxyFactory}对象，然后调用{@link #knownMappers}的{@link HashMap#put(Object, Object)}传入{@code type}和构造好的{@link MapperProxyFactory}对象进行缓存
     *             </li>
     *             <li>
     *                 调用构造器{@link MapperAnnotationBuilder#MapperAnnotationBuilder(Configuration, Class)}传入{@link #config}和{@code type}构造一个{@link MapperAnnotationBuilder}对象，然后调用{@link MapperAnnotationBuilder#parse()}解析Mapper接口中的注解
     *             </li>
     *             <li>
     *                 在以上第2、3步的过程中抛出任何异常，本方法不会catch，继续往外抛，但是会执行以下 finally 操作：调用{@link #knownMappers}的{@link HashMap#remove(Object)}传入{@code type}移除当前Mapper和其{@link MapperProxyFactory}的键值对
     *                 <pre>
     *                     //代码
     *                     boolean loadCompleted = false;
     *                     try {
     *                         knownMappers.put(type, new MapperProxyFactory<>(type));
     *                         // It's important that the type is added before the parser is run
     *                         // otherwise the binding may automatically be attempted by the
     *                         // mapper parser. If the type is already known, it won't try.
     *                         MapperAnnotationBuilder parser = new MapperAnnotationBuilder(config, type);
     *                         parser.parse();
     *                         loadCompleted = true;
     *                     } finally {
     *                         if (!loadCompleted) {
     *                             knownMappers.remove(type);
     *                         }
     *                     }
     *                 </pre>
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         否：什么也不做，本方法结束
     *     </li>
     * </ul>
     *
     * @param type
     * @param <T>
     */
    public <T> void addMapper(Class<T> type) {
        if (type.isInterface()) {
            if (hasMapper(type)) {
                throw new BindingException("Type " + type + " is already known to the MapperRegistry.");
            }
            boolean loadCompleted = false;
            try {
                knownMappers.put(type, new MapperProxyFactory<>(type));
                // It's important that the type is added before the parser is run
                // otherwise the binding may automatically be attempted by the
                // mapper parser. If the type is already known, it won't try.
                MapperAnnotationBuilder parser = new MapperAnnotationBuilder(config, type);
                parser.parse();
                loadCompleted = true;
            } finally {
                if (!loadCompleted) {
                    knownMappers.remove(type);
                }
            }
        }
    }

    /**
     * 调用{@link Collections#unmodifiableList(List)}传入{@link #knownMappers}的{@link HashMap#keySet()}转化为不可修改的集合后返回
     *
     * @since 3.2.2
     */
    public Collection<Class<?>> getMappers() {
        return Collections.unmodifiableCollection(knownMappers.keySet());
    }

    /**
     * 扫描指定包，并将{@code superType}的子类，添加到 {@link #knownMappers} 中：
     * <ol>
     *     <li>new 一个{@link ResolverUtil}对象、调用构造器{@link ResolverUtil.IsA#IsA(Class)}传入{@code superType}构建一个测试器(是否指定类的子类)</li>
     *     <li>调用{@link ResolverUtil#find(ResolverUtil.Test, String)}传入前面new的{@link ResolverUtil.IsA}测试器和{@code packageName}进行类扫描</li>
     *     <li>调用{@link ResolverUtil#getClasses()} 获取扫描到的类集合{@link Set}对象（指定包{@code packageName}下{@code superType}的子类）</li>
     *     <li>循环迭代{@link Set}集合对象，对于每一个迭代的{@link Class}对象：调用{@link #addMapper(Class)}传入该对象进行mapper接口类的解析</li>
     * </ol>
     *
     * @param packageName 指定包名
     * @param superType 符合类的父类
     * @since 3.2.2
     */
    public void addMappers(String packageName, Class<?> superType) {
        ResolverUtil<Class<?>> resolverUtil = new ResolverUtil<>();
        resolverUtil.find(new ResolverUtil.IsA(superType), packageName);
        Set<Class<? extends Class<?>>> mapperSet = resolverUtil.getClasses();
        for (Class<?> mapperClass : mapperSet) {
            addMapper(mapperClass);
        }
    }

    /**
     * 调用{@link #addMappers(String, Class)}传入{@code packageName}和{@link Object}.class解析执行包名{@code packageName}下{@link Object}的子类（mapper接口类）
     *
     * @param packageName 指定包名
     * @since 3.2.2
     */
    public void addMappers(String packageName) {
        addMappers(packageName, Object.class);
    }

}