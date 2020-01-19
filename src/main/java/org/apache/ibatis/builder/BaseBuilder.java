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
package org.apache.ibatis.builder;

import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.ResultSetType;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 基础构造器抽象类，为子类提供通用的工具类
 *
 * @author Clinton Begin
 */
public abstract class BaseBuilder {

    /**
     * MyBatis Configuration 对象
     */
    protected final Configuration configuration;
    protected final TypeAliasRegistry typeAliasRegistry;
    protected final TypeHandlerRegistry typeHandlerRegistry;

    /**
     * 本类的唯一构造函数
     * <ul>
     *     <li>
     *         设置内部{@code configuration}到内部变量{@link #configuration}
     *     </li>
     *     <li>
     *         设置{@link Configuration#getTypeAliasRegistry()}到内部变量{@link #typeAliasRegistry}
     *     </li>
     *     <li>
     *         设置{@link Configuration#getTypeHandlerRegistry()}到内部变量{@link #typeHandlerRegistry}
     *     </li>
     * </ul>
     *
     * @param configuration
     */
    public BaseBuilder(Configuration configuration) {
        this.configuration = configuration;
        this.typeAliasRegistry = this.configuration.getTypeAliasRegistry();
        this.typeHandlerRegistry = this.configuration.getTypeHandlerRegistry();
    }

    public Configuration getConfiguration() {
            return configuration;
    }

    /**
     * 传入{@code regex}编译为正则表达式对象{@link Pattern}，如果{@code regex}是null，则使用{@code defaultValue}进行构建
     *
     * @param regex 指定表达式
     * @param defaultValue 默认表达式
     * @return 正则表达式
     */
    @SuppressWarnings("SameParameterValue")
    protected Pattern parseExpression(String regex, String defaultValue) {
        return Pattern.compile(regex == null ? defaultValue : regex);
    }

    /**
     * 直接调用{@link Boolean#valueOf(String)}将{@code value}字符串转成{@link Boolean}，如果{@code value}是null，则直接返回{@code defaultValue}
     *
     * @param value
     * @param defaultValue
     * @return
     */
    protected Boolean booleanValueOf(String value, Boolean defaultValue) {
        return value == null ? defaultValue : Boolean.valueOf(value);
    }

    /**
     * 直接调用{@link Integer#valueOf(String)}将{@code value}字符串转成{@link Integer}，如果{@code value}是null，则直接返回{@code defaultValue}
     *
     * @param value
     * @param defaultValue
     * @return
     */
    @SuppressWarnings("SameParameterValue")
    protected Integer integerValueOf(String value, Integer defaultValue) {
        return value == null ? defaultValue : Integer.valueOf(value);
    }

    /**
     * 将传入的{@code value}使用","进行切割成多个字符串数组并构成成一个{@link HashSet}，如果{@code value}是null，则使用{@code defaultValue}进行以上操作并返回
     *
     * @param value
     * @param defaultValue
     * @return
     */
    @SuppressWarnings("SameParameterValue")
    protected Set<String> stringSetValueOf(String value, String defaultValue) {
        value = (value == null ? defaultValue : value);
        return new HashSet<>(Arrays.asList(value.split(",")));
    }

    /**
     * 传入{@code alias}为{@link JdbcType}枚举的名称，如果{@code alias}则返回null，否则调用{@link JdbcType#valueOf(String)}获取相应的{@link JdbcType}枚举值对象
     *
     * @param alias
     * @return
     */
    protected JdbcType resolveJdbcType(String alias) {
        if (alias == null) {
            return null;
        }
        try {
            return JdbcType.valueOf(alias);
        } catch (IllegalArgumentException e) {
            throw new BuilderException("Error resolving JdbcType. Cause: " + e, e);
        }
    }

    /**
     * 传入{@code alias}为{@link ResultSetType}枚举的名称，如果{@code alias}为null则返回null，否则调用{@link ResultSetType#valueOf(String)}获取相应的{@link ResultSetType}枚举值对象
     *
     * @param alias
     * @return
     */
    protected ResultSetType resolveResultSetType(String alias) {
        if (alias == null) {
            return null;
        }
        try {
            return ResultSetType.valueOf(alias);
        } catch (IllegalArgumentException e) {
            throw new BuilderException("Error resolving ResultSetType. Cause: " + e, e);
        }
    }

    /**
     * 传入{@code alias}为{@link ParameterMode}枚举的名称，如果{@code alias}则返回null，否则调用{@link ParameterMode#valueOf(String)}获取相应的{@link ParameterMode}枚举值对象
     *
     * @param alias
     * @return
     */
    protected ParameterMode resolveParameterMode(String alias) {
        if (alias == null) {
            return null;
        }
        try {
            return ParameterMode.valueOf(alias);
        } catch (IllegalArgumentException e) {
            throw new BuilderException("Error resolving ParameterMode. Cause: " + e, e);
        }
    }

    /**
     * 传入类的别名{@code alias}，调用{@link BaseBuilder#resolveClass(String)}获取对应的Clazz对象，如果为null则返回null，否则直接调用{@link Class#newInstance()}构建对应的Object返回
     *
     * @param alias
     * @return
     */
    protected Object createInstance(String alias) {
        // 获得对应的类型
        Class<?> clazz = resolveClass(alias);
        if (clazz == null) {
            return null;
        }
        try {
            // 创建对象
            return resolveClass(alias).newInstance(); // 这里重复获得了一次
        } catch (Exception e) {
            throw new BuilderException("Error creating instance. Cause: " + e, e);
        }
    }

    /**
     * 如果{@code alias}是null，则返回null，否则直接调用{@link #resolveAlias(String)}从{@link BaseBuilder#typeAliasRegistry}中获取别名对应的Class对象
     *
     * @param alias
     * @param <T>
     * @return
     */
    protected <T> Class<? extends T> resolveClass(String alias) {
        if (alias == null) {
            return null;
        }
        try {
            return resolveAlias(alias);
        } catch (Exception e) {
            throw new BuilderException("Error resolving class. Cause: " + e, e);
        }
    }

    /**
     * <ol>
     *     <li>
     *         如果{@code typeHandlerAlias}是null，则返回null
     *     </li>
     *     <li>
     *         否则调用{@link BaseBuilder#resolveClass(String)}获取TypeHandler的Class对象，如果该Class对象为null或者不是{@link TypeHandler}的子类则抛出异常
     *     </li>
     *     <li>
     *         调用{@link BaseBuilder#resolveTypeHandler(Class, Class)}将{@code javaType}和{@link TypeHandler}的Class对象传入获取其Object
     *     </li>
     * </ol>
     *
     * @param javaType
     * @param typeHandlerAlias
     * @return
     */
    protected TypeHandler<?> resolveTypeHandler(Class<?> javaType, String typeHandlerAlias) {
        if (typeHandlerAlias == null) {
            return null;
        }
        Class<?> type = resolveClass(typeHandlerAlias);
        if (type != null && !TypeHandler.class.isAssignableFrom(type)) {
            throw new BuilderException("Type " + type.getName() + " is not a valid TypeHandler because it does not implement TypeHandler interface");
        }
        @SuppressWarnings("unchecked") // already verified it is a TypeHandler
        Class<? extends TypeHandler<?>> typeHandlerType = (Class<? extends TypeHandler<?>>) type;
        return resolveTypeHandler(javaType, typeHandlerType);
    }

    /**
     * <ol>
     *     <li>
     *         如果{@code typeHandlerType}是null，则返回null。
     *     </li>
     *     <li>
     *         否则使用{@link BaseBuilder#typeHandlerRegistry}调用{@link TypeHandlerRegistry#getMappingTypeHandler(Class)}获取{@link TypeHandler}对象
     *     </li>
     *     <li>
     *         如果获取不到则使用{@link BaseBuilder#typeHandlerRegistry}调用{@link TypeHandlerRegistry#getInstance(Class, Class)}将{@code javaType}作为{@code typeHandlerType}可能的携带{@link Class}类型的有参构造创建一个实例返回
     *     </li>
     * </ol>
     *
     * @param javaType
     * @param typeHandlerType
     * @return
     */
    protected TypeHandler<?> resolveTypeHandler(Class<?> javaType, Class<? extends TypeHandler<?>> typeHandlerType) {
        if (typeHandlerType == null) {
            return null;
        }
        // javaType ignored for injected handlers see issue #746 for full detail
        TypeHandler<?> handler = typeHandlerRegistry.getMappingTypeHandler(typeHandlerType);
        if (handler == null) {
            // not in registry, create a new one
            handler = typeHandlerRegistry.getInstance(javaType, typeHandlerType);
        }
        return handler;
    }

    /**
     * 传入{@code alias}为{@link #typeAliasRegistry}中的类的别名，调用{@link TypeAliasRegistry#resolveAlias(String)}获取别名对应的Class对象
     *
     * @param alias
     * @param <T>
     * @return
     */
    protected <T> Class<? extends T> resolveAlias(String alias) {
        return typeAliasRegistry.resolveAlias(alias);
    }
}
