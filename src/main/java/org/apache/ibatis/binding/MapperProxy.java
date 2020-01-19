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

import org.apache.ibatis.reflection.ExceptionUtil;
import org.apache.ibatis.session.SqlSession;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * Mapper代理对象
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class MapperProxy<T> implements InvocationHandler, Serializable {

    private static final long serialVersionUID = -6424540398559729838L;

    /**
     * {@link SqlSession} 对象
     */
    private final SqlSession sqlSession;
    /**
     * Mapper 接口
     */
    private final Class<T> mapperInterface;
    /**
     * 真实{@link Method}和{@link MapperMethod}的映射<br>
     *
     * 从 {@link MapperProxyFactory#methodCache} 传递过来
     */
    private final Map<Method, MapperMethod> methodCache;

    /**
     * {@code sqlSession}赋值到{@link #sqlSession}、{@code mapperInterface}赋值到{@link #mapperInterface}、{@code methodCache}赋值到{@link #methodCache}
     *
     * @param sqlSession
     * @param mapperInterface
     * @param methodCache
     */
    public MapperProxy(SqlSession sqlSession, Class<T> mapperInterface, Map<Method, MapperMethod> methodCache) {
        this.sqlSession = sqlSession;
        this.mapperInterface = mapperInterface;
        this.methodCache = methodCache;
    }

    /**
     * <ol>
     *     <li>
     *         判断 {@link Object}.class.equals（{@code method}的{@link Method#getDeclaringClass()}）（该方法是在{@link Object}）中声明的
     *         <ul>
     *             <li>
     *                 true：{@code method}的{@link Method#invoke(Object, Object...)}传入 当前对象的引用（不是{@code proxy}）和{@code args}两个参数进行方法调用
     *             </li>
     *             <li>
     *                 false：调用{@link #isDefaultMethod(Method)}传入{@code method}判断是否是默认方法：
     *                 <ul>
     *                     <li>
     *                         true：调用{@link #invokeDefaultMethod(Object, Method, Object[])}传入{@code proxy}、{@code method}、{@code args} 三个参数，进行默认方法的执行
     *                     </li>
     *                     <li>
     *                         false：什么也不做，进行往下走
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *         <u><b>（注：以上代码片段被try catch括住对{@link Throwable}进行捕获，捕获到异常之后调用{@link ExceptionUtil#unwrapThrowable(Throwable)}传入该异常对象进行异常的解析）</b></u>
     *     </li>
     *     <li>
     *         来到这里，说明当前方法既不是{@link Object}中的方法也不是默认方法，
     *     </li>
     * </ol>
     *
     * @param proxy 被拦截的对象
     * @param method 被拦截的当前方法
     * @param args 方法的入参列表
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        try {
            if (Object.class.equals(method.getDeclaringClass())) {
                return method.invoke(this, args);
            // 见 https://github.com/mybatis/mybatis-3/issues/709 ，支持 JDK8 default 方法
            } else if (isDefaultMethod(method)) {
                return invokeDefaultMethod(proxy, method, args);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.unwrapThrowable(t);
        }
        final MapperMethod mapperMethod = cachedMapperMethod(method);
        return mapperMethod.execute(sqlSession, args);
    }

    private MapperMethod cachedMapperMethod(Method method) {
        return methodCache.computeIfAbsent(method, k -> new MapperMethod(mapperInterface, method, sqlSession.getConfiguration()));
    }

    /**
     * 执行默认方法（通过方法句柄{@link java.lang.invoke.MethodHandle}进行"方法逻辑机器指令"的重用）：
     * <ol>
     *     <li>
     *         调用{@link MethodHandles.Lookup}.class的{@link Class#getDeclaredConstructor(Class[])}传入{@link Class}.class和int.class 获取{@link MethodHandles.Lookup}（方法查找器）的
     *         构造器{@link MethodHandles.Lookup#Lookup(Class, int)}的reflect的{@link Constructor}对象
     *     </li>
     *     <li>
     *         如果{@link Constructor#isAccessible()}为false：则调用{@link Constructor#setAccessible(boolean)}传入true设置为可访问；否则什么也不做
     *     </li>
     *     <li>
     *         调用{@code method}的{@link Method#getDeclaringClass()}获取{@code method}的声明类{@link Class}对象
     *     </li>
     *     <li>
     *         调用第一步得到的{@link Constructor}的{@link Constructor#newInstance(Object...)}传入前一步获取的{@link Class}对象和布尔运算"{@link MethodHandles.Lookup#PRIVATE} | {@link MethodHandles.Lookup#PROTECTED} | {@link MethodHandles.Lookup#PACKAGE} | {@link MethodHandles.Lookup#PUBLIC}"
     *         构建一个功能为从指定的class中查找描述符为"private"或者"protected"或者"package"或者"public"的{@link MethodHandles.Lookup}对象
     *     </li>
     *     <li>
     *         调用{@link MethodHandles.Lookup#unreflectSpecial(Method, Class)}传入{@code method}和第3步获取的方法声明类{@link Class}对象获取到该反射方法对象{@code method}的句柄{@link java.lang.invoke.MethodHandle}对象，然后调用
     *         {@link java.lang.invoke.MethodHandle#bindTo(Object)}传入{@code proxy}给该句柄指向的指令中涉及的"java object 域内的变量"进行赋值
     *     </li>
     *     <li>
     *         最后调用该方法句柄{@link java.lang.invoke.MethodHandle#invokeWithArguments(Object...)}传入{@code args}进行方法调用（给该句柄指向的指令中涉及的"方法入参域内的变量"进行赋值，然后执行指令），并return 方法执行得到的结果，方法结束
     *     </li>
     * </ol>
     *
     * @param proxy 被代理的对象
     * @param method 被代理的方法（reflection）
     * @param args 方法实参数组
     * @return 执行方法调用之后的返回对象
     * @throws Throwable
     */
    private Object invokeDefaultMethod(Object proxy, Method method, Object[] args)
            throws Throwable {
        final Constructor<MethodHandles.Lookup> constructor = MethodHandles.Lookup.class
                .getDeclaredConstructor(Class.class, int.class);
        if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
        }
        final Class<?> declaringClass = method.getDeclaringClass();
        return constructor
                .newInstance(declaringClass,
                        MethodHandles.Lookup.PRIVATE | MethodHandles.Lookup.PROTECTED
                                | MethodHandles.Lookup.PACKAGE | MethodHandles.Lookup.PUBLIC)
                .unreflectSpecial(method, declaringClass).bindTo(proxy).invokeWithArguments(args);
    }

    /**
     * Backport of java.lang.reflect.Method#isDefault()<br>
     * 判断一个方法是否默认方法：计算 {@code method}的{@link Method#getModifiers()} 与（{@link Modifier#ABSTRACT} 或 {@link Modifier#PUBLIC} 或 {@link Modifier#STATIC}）得到的值 == {@link Modifier#PUBLIC}  并且 {@code method}的{@link Method#getDeclaringClass()}的{@link Class#isInterface()}是true（方法的声明类是一个接口）
     */
    private boolean isDefaultMethod(Method method) {
        return (method.getModifiers()
                & (Modifier.ABSTRACT | Modifier.PUBLIC | Modifier.STATIC)) == Modifier.PUBLIC
                && method.getDeclaringClass().isInterface();
    }

}
