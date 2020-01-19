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
package org.apache.ibatis.scripting.defaults;

import org.apache.ibatis.builder.SqlSourceBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.scripting.xmltags.DynamicContext;
import org.apache.ibatis.scripting.xmltags.DynamicSqlSource;
import org.apache.ibatis.scripting.xmltags.SqlNode;
import org.apache.ibatis.session.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * Static SqlSource. It is faster than {@link DynamicSqlSource} because mappings are 
 * calculated during startup.
 *
 * 原始的 SqlSource 实现类
 *
 * @since 3.2.0
 * @author Eduardo Macarron
 */
public class RawSqlSource implements SqlSource {

    /**
     * 内部的{@link SqlSource} 对象
     */
    private final SqlSource sqlSource;

    /**
     * <ol>
     *     <li>
     *         调用{@link #getSql(Configuration, SqlNode)}传入{@code configuration}和{@code rootSqlNode}得到拼接后（解析应用了{@link SqlNode}）的最终静态sql
     *     </li>
     *     <li>
     *         调用另一个构造器{@link RawSqlSource#RawSqlSource(Configuration, String, Class)} 传入 {@code configuration}、第一步得到的sql字符串、{@code parameterType}构建{@link RawSqlSource}对象
     *     </li>
     * </ol>
     *
     * @param configuration 全局{@link Configuration}对象
     * @param rootSqlNode 当前定义sql的标签（{@code <select/>}、{@code <update/>}、{@code <insert/>}、{@code <delete/>}、{@code <selectKey/>}）解析之后得到的一个{@link SqlNode}对象（{@link MixedSqlNode}）
     * @param parameterType {@link org.apache.ibatis.scripting.xmltags.XMLScriptBuilder#parameterType}
     */
    public RawSqlSource(Configuration configuration, SqlNode rootSqlNode, Class<?> parameterType) {
        this(configuration, getSql(configuration, rootSqlNode), parameterType);
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link SqlSourceBuilder#SqlSourceBuilder(Configuration)}传入{@code configuration}构建一个{@link SqlSourceBuilder}对象
     *     </li>
     *     <li>
     *         调用{@link SqlSourceBuilder#parse(String, Class, Map)}传入{@code sql}、{@code parameterType}是null则是{@link Object}.class否则就是它自己、new 一个{@link HashMap}对象  三个参数解析构建一个{@link SqlSource}对象（{@link org.apache.ibatis.builder.StaticSqlSource}）并赋值到{@link #sqlSource}
     *     </li>
     * </ol>
     *
     * @param configuration  全局{@link Configuration}对象
     * @param sql
     * @param parameterType  {@link org.apache.ibatis.scripting.xmltags.XMLScriptBuilder#parameterType}
     */
    public RawSqlSource(Configuration configuration, String sql, Class<?> parameterType) {
        SqlSourceBuilder sqlSourceParser = new SqlSourceBuilder(configuration);
        Class<?> clazz = parameterType == null ? Object.class : parameterType;
        sqlSource = sqlSourceParser.parse(sql, clazz, new HashMap<>());
    }

    /**
     * <ol>
     *     <li>
     *         调用构造器{@link DynamicContext#DynamicContext(Configuration, Object)}传入{@code configuration}和null构建一个{@link DynamicContext}对象
     *     </li>
     *     <li>
     *         调用{@code rootSqlNode}的{@link SqlNode#apply(DynamicContext)}传入第一步构建的{@link DynamicContext}对象
     *     </li>
     *     <li>
     *         return 第一步构建第二步处理之后的的{@link DynamicContext}对象的{@link DynamicContext#getSql()}，本方法结束
     *     </li>
     * </ol>
     *
     * @param configuration 全局{@link Configuration}对象
     * @param rootSqlNode 当前定义sql的标签（{@code <select/>}、{@code <update/>}、{@code <insert/>}、{@code <delete/>}、{@code <selectKey/>}）解析之后得到的一个{@link SqlNode}对象（{@link MixedSqlNode}）
     * @return
     */
    private static String getSql(Configuration configuration, SqlNode rootSqlNode) {
        DynamicContext context = new DynamicContext(configuration, null);
        rootSqlNode.apply(context);
        return context.getSql();
    }

    /**
     * @param parameterObject 参数对象
     * @return 调用{@link #sqlSource}的{@link SqlSource#getBoundSql(Object)}传入{@code parameterObject}
     */
    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        return sqlSource.getBoundSql(parameterObject);
    }

}