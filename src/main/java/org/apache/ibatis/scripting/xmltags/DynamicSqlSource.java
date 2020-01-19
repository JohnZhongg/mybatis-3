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
package org.apache.ibatis.scripting.xmltags;

import org.apache.ibatis.builder.SqlSourceBuilder;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

import java.util.Map;

/**
 * 动态的 {@link SqlSource} 实现类
 *
 * @author Clinton Begin
 */
public class DynamicSqlSource implements SqlSource {

    /**
     * 全局的{@link Configuration}对象
     */
    private final Configuration configuration;
    /**
     * 当前定义sql的标签（{@code <select/>}、{@code <update/>}、{@code <insert/>}、{@code <delete/>}、{@code <selectKey/>}）解析之后得到的一个{@link SqlNode}对象（{@link MixedSqlNode}）
     */
    private final SqlNode rootSqlNode;

    /**
     * {@code configuration}赋值到{@link #configuration}、{@code rootSqlNode}赋值到{@link #rootSqlNode}
     *
     * @param configuration 全局{@link Configuration}对象
     * @param rootSqlNode 当前定义sql的标签（{@code <select/>}、{@code <update/>}、{@code <insert/>}、{@code <delete/>}、{@code <selectKey/>}）解析之后得到的一个{@link SqlNode}对象（{@link MixedSqlNode}）
     */
    public DynamicSqlSource(Configuration configuration, SqlNode rootSqlNode) {
        this.configuration = configuration;
        this.rootSqlNode = rootSqlNode;
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link DynamicContext#DynamicContext(Configuration, Object)}传入{@link #configuration}和{@code parameterObject}构建一个sql上下文{@link DynamicContext}对象
     *     </li>
     *     <li>
     *         调用{@link #rootSqlNode}的{@link SqlNode#apply(DynamicContext)}（{@link MixedSqlNode#apply(DynamicContext)}）传入上面构建的好的sql上下文对象进行{@link SqlNode}解析成sql
     *     </li>
     *     <li>
     *         调用{@link SqlSourceBuilder#SqlSourceBuilder(Configuration)}传入{@link #configuration}构建{@link SqlSourceBuilder}对象
     *     </li>
     *     <li>
     *         调用上面构建的{@link SqlSourceBuilder#parse(String, Class, Map)}传入 "经过第二步处理后的sql上下文{@link DynamicContext#getSql()}"、"如果{@code parameterObject}是null则是{@link Object}.class否则是{@code parameterObject}.getClass()"、"sql上下文{@link DynamicContext#getBindings()}" 三个参数解析构建成一个{@link SqlSource}对象（{@link org.apache.ibatis.builder.StaticSqlSource}）
     *     </li>
     *     <li>
     *         调用{@link org.apache.ibatis.builder.StaticSqlSource#getBoundSql(Object)}传入{@code parameterObject}得到一个{@link BoundSql}对象
     *     </li>
     *     <li>
     *         循环遍历第一步new的然后经过第二步处理之后的{@link DynamicContext}对象的{@link DynamicContext#getBindings()}的{@link DynamicContext.ContextMap#entrySet()}，对于每一个迭代的{@link java.util.Map.Entry}对象：<br>
     *
     *                 调用{@link Map.Entry#getKey()}和{@link Map.Entry#getValue()}分别获取key和value，然后调用前一步得到的{@link BoundSql}对象的{@link BoundSql#setAdditionalParameter(String, Object)}传入key和value
     *
     *     </li>
     *     <li>
     *         return 设置了{@link BoundSql#additionalParameters}之后的{@link BoundSql}对象，本方法结束
     *     </li>
     * </ol>
     *
     * @param parameterObject 承载了参数值的对象
     * @return
     */
    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        DynamicContext context = new DynamicContext(configuration, parameterObject);
        rootSqlNode.apply(context);
        SqlSourceBuilder sqlSourceParser = new SqlSourceBuilder(configuration);
        Class<?> parameterType = parameterObject == null ? Object.class : parameterObject.getClass();
        SqlSource sqlSource = sqlSourceParser.parse(context.getSql(), parameterType, context.getBindings());
        BoundSql boundSql = sqlSource.getBoundSql(parameterObject);
        for (Map.Entry<String, Object> entry : context.getBindings().entrySet()) {
            boundSql.setAdditionalParameter(entry.getKey(), entry.getValue());
        }
        return boundSql;
    }

}