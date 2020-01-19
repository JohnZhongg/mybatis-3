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

import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.session.Configuration;

import java.util.List;

/**
 * 静态的 {@link SqlSource} 实现类
 *
 * @author Clinton Begin
 */
public class StaticSqlSource implements SqlSource {

    /**
     * 静态的 SQL（最终sql："${}"动态替换成了对应的字符串、"#{}"替换成了?）
     */
    private final String sql;
    /**
     * {@link ParameterMapping} 对象集合
     */
    private final List<ParameterMapping> parameterMappings;
    /**
     * 全局的{@link Configuration}对象
     */
    private final Configuration configuration;

    /**
     * 调用另一个构造器{@link StaticSqlSource#StaticSqlSource(Configuration, String, List)}传入{@code configuration}、{@code sql}、null构建一个{@link StaticSqlSource}对象
     *
     * @param configuration
     * @param sql
     */
    public StaticSqlSource(Configuration configuration, String sql) {
        this(configuration, sql, null);
    }

    /**
     * {@code sql}赋值到{@link #sql}、{@code parameterMappings}赋值到{@link #parameterMappings}、{@code configuration}赋值到{@link #configuration}
     *
     * @param configuration
     * @param sql
     * @param parameterMappings
     */
    public StaticSqlSource(Configuration configuration, String sql, List<ParameterMapping> parameterMappings) {
        this.sql = sql;
        this.parameterMappings = parameterMappings;
        this.configuration = configuration;
    }

    /**
     * 调用{@link BoundSql#BoundSql(Configuration, String, List, Object)}传入{@link #configuration}、{@link #sql}、{@link #parameterMappings}、{@code parameterObject} 四个参数实例化一个{@link BoundSql}对象并返回
     *
     * @param parameterObject 参数对象
     * @return
     */
    @Override
    public BoundSql getBoundSql(Object parameterObject) {
        return new BoundSql(configuration, sql, parameterMappings, parameterObject);
    }

}