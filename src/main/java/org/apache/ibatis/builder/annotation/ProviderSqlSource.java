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
package org.apache.ibatis.builder.annotation;

import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.SqlSourceBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.session.Configuration;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 基于方法上的 @ProviderXXX 注解的 SqlSource 实现类
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class ProviderSqlSource implements SqlSource {

    /**
     *  全局{@link Configuration}对象
     */
    private final Configuration configuration;
    /**
     * {@link SqlSourceBuilder}
     */
    private final SqlSourceBuilder sqlSourceParser;
    /**
     * 提供sql构建逻辑的类（即@XXXProvider中的type属性值）
     */
    private final Class<?> providerType;
    /**
     * 提供sql构建逻辑的方法（即@XXXProvider中的method属性值）
     */
    private Method providerMethod;
    /**
     * 提供sql构建逻辑的方法（即@XXXProvider中的method属性值）的参数名称数组
     */
    private String[] providerMethodArgumentNames;
    /**
     * 提供sql构建逻辑的方法（即@XXXProvider中的method属性值）的参数类型数组
     */
    private Class<?>[] providerMethodParameterTypes;
    /**
     * 若 {@link #providerMethodParameterTypes} 参数有 {@link ProviderContext} 类型的，mybatis自动创建 {@link ProviderContext} 对象进行注入，不用用户赋值
     */
    private ProviderContext providerContext;
    /**
     * {@link #providerMethodParameterTypes} 参数中，{@link ProviderContext} 类型的参数，在数组中的索引
     */
    private Integer providerContextIndex;

    /**
     * @deprecated Please use the {@link #ProviderSqlSource(Configuration, Object, Class, Method)} instead of this.
     */
    @Deprecated
    public ProviderSqlSource(Configuration configuration, Object provider) {
        this(configuration, provider, null, null);
    }

    /**
     * <ol>
     *     <li>
     *         {@code configuration}赋值到{@link #configuration}
     *     </li>
     *     <li>
     *         调用构造器{@link SqlSourceBuilder#SqlSourceBuilder(Configuration)}传入{@code configuration}构建一个{@link SqlSourceBuilder}对象赋值到{@link #sqlSourceParser}
     *     </li>
     *     <li>
     *         调用{@code provider}.getClass.getMethod("type")获得该注解的"type()"方法{@link Method}，然后调用{@link Method#invoke(Object, Object...)}传入{@code provider}通过反射获取该注解中的"type"属性值{@link Class}对象然后赋值到{@link #providerType}
     *     </li>
     *     <li>
     *         调用{@code provider}.getClass.getMethod("method")获得该注解的"method()"方法{@link Method}，然后调用{@link Method#invoke(Object, Object...)}传入{@code provider}通过反射获取该注解中的"method"属性值{@link Class}对象然后赋值到{@link #providerMethod}
     *     </li>
     *     <li>
     *         调用第三步获得的"type"属性值{@link Class}的{@link Class#getMethods()}获取provider类的所有方法数组并遍历迭代该数组，对于每一个迭代的方法对象元素{@link Method}：
     *         <ul>
     *             判断：第四步获取的"method"属性值.equals(当前迭代的{@link Method#getName()}) 并且 {@link CharSequence}.class.isAssignableFrom(当前迭代的{@link Method#getReturnType()}) （方法名等于注解中声明的方法名且返回值是字符串）：
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         在将当前迭代的方法对象{@link Method}赋值到{@link #providerMethod}之前，检查{@link #providerMethod}是否已经不为null：
     *                         <ul>
     *                             <li>
     *                                 是：直接抛出异常{@link BuilderException}（不能进行方法重载）
     *                             </li>
     *                             <li>
     *                                 否：什么也不做，继续往下走
     *                             </li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         将当前迭代的方法对象赋值到{@link #providerMethod}
     *                     </li>
     *                     <li>
     *                         调用构造器{@link ParamNameResolver#ParamNameResolver(Configuration, Method)}传入{@code configuration}和当前迭代的方法对象{@link Method}构建该方法的方法参数名解析器{@link ParamNameResolver}然后调用{@link ParamNameResolver#getNames()}赋值到{@link #providerMethodArgumentNames}
     *                     </li>
     *                     <li>
     *                         调用当前迭代的方法对象{@link Method#getParameterTypes()}获取其参数类型数组赋值到{@link #providerMethodParameterTypes}
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false：什么也不做，进入循环下一迭代，直到循环结束
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         来到这里，检查{@link #providerMethod}是否还是null：如果还是直接抛出异常{@link BuilderException}（找不到该方法）；否则什么也不做，继续往下走
     *     </li>
     *     <li>
     *         遍历迭代{@link #providerMethodParameterTypes}数组，对于每一个迭代的参数类型对象{@link Class}：
     *         <ul>
     *             判断：当前迭代的参数类型对象{@link Class} == {@link ProviderContext}.class
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         在准备赋值到{@link #providerContext}之前，先检查它是不是已经不是null：
     *                         <ul>
     *                             <li>
     *                                 是：直接抛出异常{@link BuilderException}（冲突，不能定义两个{@link ProviderContext}类型的参数）
     *                             </li>
     *                             <li>
     *                                 否：什么也不做，继续往下走
     *                             </li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         调用构造器{@link ProviderContext#ProviderContext(Class, Method)}传入{@code mapperType}和{@code mapperMethod}构造一个{@link ProviderContext}对象并赋值到{@link #providerContext}
     *                     </li>
     *                     <li>
     *                         将当前迭代的索引赋值到{@link #providerContextIndex}
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false；什么也不做，进入循环下一迭代，直到循环结束，方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param configuration 全局{@link Configuration}对象
     * @param provider 当前注解对象（{@link org.apache.ibatis.annotations.SelectProvider}、{@link org.apache.ibatis.annotations.UpdateProvider}、{@link org.apache.ibatis.annotations.DeleteProvider}、{@link org.apache.ibatis.annotations.InsertProvider}、）
     * @param mapperType 定义了当前Provider注解的Mapper接口的{@link Class}对象
     * @param mapperMethod 定义了当前Provider注解的Mapper接口中的方法的{@link Method}对象
     * @since 3.4.5
     */
    public ProviderSqlSource(Configuration configuration, Object provider, Class<?> mapperType, Method mapperMethod) {
        String providerMethodName;
        try {
            this.configuration = configuration;
            this.sqlSourceParser = new SqlSourceBuilder(configuration);
            this.providerType = (Class<?>) provider.getClass().getMethod("type").invoke(provider);
            providerMethodName = (String) provider.getClass().getMethod("method").invoke(provider);
            for (Method m : this.providerType.getMethods()) {
                if (providerMethodName.equals(m.getName()) && CharSequence.class.isAssignableFrom(m.getReturnType())) {
                    if (providerMethod != null) {
                        throw new BuilderException("Error creating SqlSource for SqlProvider. Method '"
                                + providerMethodName + "' is found multiple in SqlProvider '" + this.providerType.getName()
                                + "'. Sql provider method can not overload.");
                    }
                    this.providerMethod = m;
                    this.providerMethodArgumentNames = new ParamNameResolver(configuration, m).getNames();
                    this.providerMethodParameterTypes = m.getParameterTypes();
                }
            }
        } catch (BuilderException e) {
            throw e;
        } catch (Exception e) {
            throw new BuilderException("Error creating SqlSource for SqlProvider.  Cause: " + e, e);
        }
        if (this.providerMethod == null) {
            throw new BuilderException("Error creating SqlSource for SqlProvider. Method '"
                    + providerMethodName + "' not found in SqlProvider '" + this.providerType.getName() + "'.");
        }
        for (int i = 0; i < this.providerMethodParameterTypes.length; i++) {
            Class<?> parameterType = this.providerMethodParameterTypes[i];
            if (parameterType == ProviderContext.class) {
                if (this.providerContext != null) {
                    throw new BuilderException("Error creating SqlSource for SqlProvider. ProviderContext found multiple in SqlProvider method ("
                            + this.providerType.getName() + "." + providerMethod.getName()
                            + "). ProviderContext can not define multiple in SqlProvider method argument.");
                }
                this.providerContext = new ProviderContext(mapperType, mapperMethod);
                this.providerContextIndex = i;
            }
        }
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link #createSqlSource(Object)}传入{@code parameterObject}构建一个{@link SqlSource}对象
     *     </li>
     *     <li>
     *         调用{@link SqlSource#getBoundSql(Object)}传入{@code parameterObject}获得结果并返回，方法结束
     *     </li>
     * </ol>
     *
     * @param parameterObject 参数对象
     * @return
     */
    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        SqlSource sqlSource = createSqlSource(parameterObject);
        return sqlSource.getBoundSql(parameterObject);
    }

    /**
     * <ol>
     *     <li>
     *         获取声明了当前provider注解的方法排除{@link ProviderContext}类型参数之后的参数个数"bindParameterCount"（int bindParameterCount = {@link #providerMethodParameterTypes}.length - ({@link #providerContext} == null ? 0 : 1);）
     *     </li>
     *     <li>
     *         判断{@link #providerMethodParameterTypes}.length == 0（方法没有参数）：
     *         <ul>
     *             <li>true：直接无参调用{@link #invokeProviderMethod(Object...)}获得sql字符串</li>
     *             <li>
     *                 false：判断排除了{@link ProviderContext}类型参数之后的参数个数 == 0（说明该方法有参数，但是只有1个{@link ProviderContext}类型的参数）：
     *                 <ul>
     *                     <li>true：调用{@link #invokeProviderMethod(Object...)}传入{@link #providerContext}获得sql字符串</li>
     *                     <li>
     *                         false：判断{ 排除了{@link ProviderContext}类型参数之后的参数个数 == 1 并且【{@code parameterObject} == null 或者 （如果{@link #providerContextIndex} == null或者 == 1：则取{@link #providerMethodParameterTypes}[0]；否则取{@link #providerMethodParameterTypes}[1]）.isAssignableFrom({@code parameterObject}.getClass())】}（该方法拥有1个非{@link ProviderContext}类型的1个其他类型的参数，然后校验本方法入参是否复符合该非{@link ProviderContext}类型的的参数类型）
     *                         <ul>
     *                             <li>true：调用{@link #extractProviderMethodArguments(Object)}传入{@code parameterObject}构建实参数组，然后调用{@link #invokeProviderMethod(Object...)}传入该实参数组获得sql字符串</li>
     *                             <li>
     *                                 false：判断{@code parameterObject} instanceof {@link Map}
     *                                 <ul>
     *                                     <li>true：将{@code parameterObject}强转为{@link Map}{@code <String, Object>}类型，然后调用{@link #extractProviderMethodArguments(Map, String[])}传入强转后的对象和{@link #providerMethodArgumentNames}构建方法实参数组，然后调用{@link #invokeProviderMethod(Object...)}传入该实参数组获得sql</li>
     *                                     <li>false：来到这里，即不满足以上所有判断条件，直接抛出异常{@link BuilderException}（异常内容说明参考方法内）</li>
     *                                 </ul>
     *                             </li>
     *                         </ul>
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link #replacePlaceholder(String)} 传入第二步获得sql字符串进行变量替换
     *     </li>
     *     <li>
     *         调用{@link #sqlSourceParser}的{@link SqlSourceBuilder#parse(String, Class, Map)}传入 前一步进行变量替换后的sql、（{@code parameterObject} == null ? {@link Object}.class : {@code parameterObject}.getClass()）、new 一个{@link HashMap}对象 三个参数，解析构建除一个{@link SqlSource}对象并返回，本方法结束
     *     </li>
     * </ol>
     *
     * @param parameterObject
     * @return
     */
    private SqlSource createSqlSource(Object parameterObject) {
        try {
            int bindParameterCount = providerMethodParameterTypes.length - (providerContext == null ? 0 : 1);
            String sql;
            if (providerMethodParameterTypes.length == 0) {
                sql = invokeProviderMethod();
            } else if (bindParameterCount == 0) {
                sql = invokeProviderMethod(providerContext);
            } else if (bindParameterCount == 1 &&
                    (parameterObject == null || providerMethodParameterTypes[(providerContextIndex == null || providerContextIndex == 1) ? 0 : 1].isAssignableFrom(parameterObject.getClass()))) {
                sql = invokeProviderMethod(extractProviderMethodArguments(parameterObject));
            } else if (parameterObject instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> params = (Map<String, Object>) parameterObject;
                sql = invokeProviderMethod(extractProviderMethodArguments(params, providerMethodArgumentNames));
            } else {
                throw new BuilderException("Error invoking SqlProvider method ("
                        + providerType.getName() + "." + providerMethod.getName()
                        + "). Cannot invoke a method that holds "
                        + (bindParameterCount == 1 ? "named argument(@Param)" : "multiple arguments")
                        + " using a specifying parameterObject. In this case, please specify a 'java.util.Map' object.");
            }
            Class<?> parameterType = parameterObject == null ? Object.class : parameterObject.getClass();
            return sqlSourceParser.parse(replacePlaceholder(sql), parameterType, new HashMap<>());
        } catch (BuilderException e) {
            throw e;
        } catch (Exception e) {
            throw new BuilderException("Error invoking SqlProvider method ("
                    + providerType.getName() + "." + providerMethod.getName()
                    + ").  Cause: " + e, e);
        }
    }

    /**
     * 判断：{@link #providerContext} != null：
     * <ul>
     *     <li>
     *         true：
     *         <ol>
     *             <li>
     *                 new 一个长度为2的{@link Object}数组
     *             </li>
     *             <li>
     *                 如果{@link #providerContextIndex}是0则将{@code parameterObject}赋值到刚new的{@link Object}数组索引为1的元素槽；不是0则赋值到索引为0的元素槽。然后将{@link #providerContext}赋值到索引为{@link #providerContextIndex}的元素槽，return 该数组，方法结束
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         false：new 一个{@link Object}数组并只初始化1个元素{@code parameterObject}，然后return 该数组，方法结束
     *     </li>
     * </ul>
     *
     * @param parameterObject
     * @return
     */
    private Object[] extractProviderMethodArguments(Object parameterObject) {
        if (providerContext != null) {
            Object[] args = new Object[2];
            args[providerContextIndex == 0 ? 1 : 0] = parameterObject;
            args[providerContextIndex] = providerContext;
            return args;
        } else {
            return new Object[]{parameterObject};
        }
    }

    /**
     * <ol>
     *     <li>new 一个{@code argumentNames}长度的{@link Object}类型数组</li>
     *     <li>
     *         遍历迭代{@code argumentNames}，对于每一个遍历迭代的{@link String}对象：判断{@link #providerContextIndex} != null 并且 {@link #providerContextIndex} == 当前迭代的索引
     *         <ul>
     *             <li>true：{@link #providerContext}赋值到新new 的{@link Object}数组的当前迭代索引指向的位置</li>
     *             <li>false：调用{@code params}的{@link Map#get(Object)}传入当前迭代的字符串元素获取对应的value，然后将该value赋值到新new 的{@link Object}数组的当前迭代索引指向的位置</li>
     *         </ul>
     *     </li>
     *     <li>返回第一步new的{@link Object}对象，方法结束</li>
     * </ol>
     *
     * @param params
     * @param argumentNames
     * @return
     */
    private Object[] extractProviderMethodArguments(Map<String, Object> params, String[] argumentNames) {
        Object[] args = new Object[argumentNames.length];
        for (int i = 0; i < args.length; i++) {
            if (providerContextIndex != null && providerContextIndex == i) {
                args[i] = providerContext;
            } else {
                args[i] = params.get(argumentNames[i]);
            }
        }
        return args;
    }

    /**
     * 执行sql Provider类的sql Provider方法：
     * <ol>
     *     <li>
     *         调用{@link Modifier#isStatic(int)}传入{@link #providerMethod}的{@link Method#getModifiers()}检查当前方法是否为静态方法：
     *         <ul>
     *             <li>
     *                 是：调用{@link #providerMethod}的{@link Method#invoke(Object, Object...)}传入null和{@code args}直接进行方法调用（不用构建声明了该方法的类型{@link #providerType}的实例对象）
     *             </li>
     *             <li>
     *                 否：调用{@link #providerType}的{@link Class#newInstance()}构建声明了该方法的类型的实例对象，然后调用{@link #providerMethod}的{@link Method#invoke(Object, Object...)}传入前面得到的{@link #providerType}类型的实例对象和{@code args}直接进行方法调用
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         将第一步通过反射方法调用返回的结果{@link Object}对象强转为{@link CharSequence}类型，然后判断该对象是否为null：为null则返回null；否则返回该对象.toString()
     *     </li>
     * </ol>
     *
     * @param args 执行sql Provider类的sql Provider方法的实参列表
     * @return 方法执行后得到的sql
     * @throws Exception
     */
    private String invokeProviderMethod(Object... args) throws Exception {
        Object targetObject = null;
        if (!Modifier.isStatic(providerMethod.getModifiers())) {
            targetObject = providerType.newInstance();
        }
        CharSequence sql = (CharSequence) providerMethod.invoke(targetObject, args);
        return sql != null ? sql.toString() : null;
    }

    /**
     * 调用{@link PropertyParser#parse(String, Properties)}传入{@code sql}和{@link #configuration}的{@link Configuration#getVariables()}对sql的"${}"Token进行变量替换
     *
     * @param sql
     * @return
     */
    private String replacePlaceholder(String sql) {
        return PropertyParser.parse(sql, configuration.getVariables());
    }

}