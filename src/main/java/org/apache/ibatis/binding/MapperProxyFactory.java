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

import org.apache.ibatis.session.SqlSession;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link MapperProxy} 工厂类（创建mapper的代理对象）
 *
 * @author Lasse Voss
 */
public class MapperProxyFactory<T> {

    /**
     * Mapper接口的{@link Class}对象
     */
    private final Class<T> mapperInterface;
    /**
     * 真实{@link Method}和{@link MapperMethod}的映射
     */
    private final Map<Method, MapperMethod> methodCache = new ConcurrentHashMap<>();

    /**
     * {@code mapperInterface}赋值到{@link #mapperInterface}
     *
     * @param mapperInterface Mapper接口的{@link Class}对象
     */
    public MapperProxyFactory(Class<T> mapperInterface) {
        this.mapperInterface = mapperInterface;
    }

    /**
     * @return {@link #mapperInterface}
     */
    public Class<T> getMapperInterface() {
        return mapperInterface;
    }

    /**
     * @return {@link #methodCache}
     */
    public Map<Method, MapperMethod> getMethodCache() {
        return methodCache;
    }

    /**
     * <ol>
     *     <li>
     *         接收一个mapper的{@link java.lang.reflect.InvocationHandler}（该类用于jdk提供的动态代理流程中，提供在拦截了要被代理的对象的方法之后要执行的步骤）实现类对象（{@link MapperProxy}）
     *     </li>
     *     <li>
     *         然后调用{@link Proxy#newProxyInstance(ClassLoader, Class[], InvocationHandler)}传入
     *         <ol>
     *             <li>
     *                 {@link #mapperInterface}的{@link Class#getClassLoader()}
     *             </li>
     *             <li>
     *                 new Class[]{{@link #mapperInterface}}  （用于jdk提供的动态代理流程中声明要被代理的接口，即声明代理对象要可以拥有或者代理哪些行为）
     *             </li>
     *             <li>
     *                 第一步接收到的{@link MapperProxy}对象
     *             </li>
     *         </ol>
     *         三个参数构建一个代理对象进行返回，对象类型为{@link #mapperInterface}的泛型
     *     </li>
     * </ol>
     *
     * @param mapperProxy mapper的{@link java.lang.reflect.InvocationHandler}实现类对象
     * @return
     */
    @SuppressWarnings("unchecked")
    protected T newInstance(MapperProxy<T> mapperProxy) {
        return (T) Proxy.newProxyInstance(mapperInterface.getClassLoader(), new Class[]{mapperInterface}, mapperProxy);
    }

    public T newInstance(SqlSession sqlSession) {
        final MapperProxy<T> mapperProxy = new MapperProxy<>(sqlSession, mapperInterface, methodCache);
        return newInstance(mapperProxy);
    }

}