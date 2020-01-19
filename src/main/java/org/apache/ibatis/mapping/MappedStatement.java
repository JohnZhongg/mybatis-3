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

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 映射的语句，每个 {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 对应一个 {@link MappedStatement} 对象
 *
 * 另外，比较特殊的是，{@code <selectKey />} 解析后，也会对应一个 {@link MappedStatement} 对象
 *
 * @author Clinton Begin
 */
public final class MappedStatement {

    /**
     * 资源引用的地址
     */
    private String resource;
    /**
     * Configuration 对象
     */
    private Configuration configuration;
    /**
     * 编号
     */
    private String id;
    /**
     * 这是尝试影响驱动程序每次批量返回的结果行数和这个设置值相等。默认值为 unset（依赖驱动）。
     */
    private Integer fetchSize;
    /**
     * 这个设置是在抛出异常之前，驱动程序等待数据库返回请求结果的秒数。默认值为 unset（依赖驱动）。
     */
    private Integer timeout;
    /**
     * 语句类型
     */
    private StatementType statementType;
    /**
     * 结果集类型
     */
    private ResultSetType resultSetType;
    /**
     * SqlSource 对象
     */
    private SqlSource sqlSource;
    /**
     * Cache 对象
     */
    private Cache cache;
    /**
     * ParameterMap 对象
     */
    private ParameterMap parameterMap;
    /**
     * ResultMap 集合
     */
    private List<ResultMap> resultMaps;
    /**
     * 将其设置为 true，任何时候只要语句被调用，都会导致本地缓存和二级缓存都会被清空，默认值：false。
     */
    private boolean flushCacheRequired;
    /**
     * 是否使用缓存
     */
    private boolean useCache;
    /**
     * 这个设置仅针对嵌套结果 select 语句适用：如果为 true，就是假设包含了嵌套结果集或是分组了，这样的话当返回一个主结果行的时候，就不会发生有对前面结果集的引用的情况。这就使得在获取嵌套的结果集的时候不至于导致内存不够用。默认值：false。
     */
    private boolean resultOrdered;
    /**
     * SQL 语句类型
     */
    private SqlCommandType sqlCommandType;
    /**
     * {@link KeyGenerator} 对象
     */
    private KeyGenerator keyGenerator;
    /**
     * （仅对 insert 和 update 有用）唯一标记一个属性，MyBatis 会通过 getGeneratedKeys 的返回值或者通过 insert 语句的 selectKey 子元素设置它的键值，默认：unset。如果希望得到多个生成的列，也可以是逗号分隔的属性名称列表。
     */
    private String[] keyProperties;
    /**
     * （仅对 insert 和 update 有用）通过生成的键值设置表中的列名，这个设置仅在某些数据库（像 PostgreSQL）是必须的，当主键列不是表中的第一列的时候需要设置。如果希望得到多个生成的列，也可以是逗号分隔的属性名称列表。
     */
    private String[] keyColumns;
    /**
     * 是否有内嵌的 ResultMap
     */
    private boolean hasNestedResultMaps;
    /**
     * 数据库标识
     */
    private String databaseId;
    /**
     * Log 对象
     */
    private Log statementLog;
    /**
     * LanguageDriver 对象
     */
    private LanguageDriver lang;
    /**
     * 这个设置仅对多结果集的情况适用，它将列出语句执行后返回的结果集并每个结果集给一个名称，名称是逗号分隔的。
     */
    private String[] resultSets;

    MappedStatement() {
        // constructor disabled
    }

    public static class Builder {

        /**
         * 每个{@link Builder}内部的一个{@link MappedStatement}对象
         */
        private MappedStatement mappedStatement = new MappedStatement();

        /**
         * <ol>
         *     <li>
         *         <ul>
         *             <li>
         *                 {@code configuration}赋值到{@link #mappedStatement}的{@link MappedStatement#configuration}
         *             </li>
         *             <li>
         *                 {@code id}赋值到{@link #mappedStatement}的{@link MappedStatement#id}
         *             </li>
         *             <li>
         *                 {@code sqlSource}赋值到{@link #mappedStatement}的{@link MappedStatement#sqlSource}
         *             </li>
         *             <li>
         *                 {@link StatementType#PREPARED}赋值到{@link #mappedStatement}的{@link MappedStatement#statementType}
         *             </li>
         *             <li>
         *                 {@link ResultSetType#DEFAULT}赋值到{@link #mappedStatement}的{@link MappedStatement#resultSetType}
         *             </li>
         *             <li>
         *                 调用{@link ParameterMap.Builder#Builder(Configuration, String, Class, List)}传入{@code configuration}、"defaultParameterMap"、null、new 一个{@link ArrayList}对象 四个参数构造一个{@link ParameterMap.Builder}对象然后调用{@link ParameterMap.Builder#build()}构建一个{@link ParameterMap}对象赋值到{@link #mappedStatement}的{@link MappedStatement#parameterMap}
         *             </li>
         *             <li>
         *                 new 一个{@link ArrayList}对象赋值到{@link #mappedStatement}的{@link MappedStatement#resultMaps}
         *             </li>
         *             <li>
         *                 {@code sqlCommandType}赋值到{@link #mappedStatement}的{@link MappedStatement#sqlCommandType}
         *             </li>
         *             <li>
         *                 判断：{@code configuration}的{@link Configuration#isUseGeneratedKeys()} 并且 {@link SqlCommandType#INSERT}.equals({@code sqlCommandType})：
         *                 <ul>
         *                     <li>
         *                         true：{@link Jdbc3KeyGenerator#INSTANCE}赋值到{@link #mappedStatement}的{@link MappedStatement#keyGenerator}
         *                     </li>
         *                     <li>
         *                         false：{@link NoKeyGenerator#INSTANCE}赋值到{@link #mappedStatement}的{@link MappedStatement#keyGenerator}
         *                     </li>
         *                 </ul>
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         判断{@code configuration}的{@link Configuration#getLogPrefix()} != null
         *         <ul>
         *             <li>
         *                 true：调用{@link Configuration#getLogPrefix()}拼接上{@code id}作为{@link Log}的id，然后调用{@link LogFactory#getLog(String)}传入拼接后的id构建一个{@link Log}对象赋值到{@link #mappedStatement}的{@link MappedStatement#statementLog}
         *             </li>
         *             <li>
         *                 false：直接将{@code id}作为{@link Log}的id，然后调用{@link LogFactory#getLog(String)}传入该id构建一个{@link Log}对象赋值到{@link #mappedStatement}的{@link MappedStatement#statementLog}
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         调用{@code configuration}的{@link Configuration#getDefaultScriptingLanguageInstance()}获取默认的{@link LanguageDriver}对象赋值到{@link #mappedStatement}的{@link MappedStatement#lang}
         *     </li>
         * </ol>
         *
         * @param configuration 全局{@link Configuration}对象
         * @param id 当前标签的id
         * @param sqlSource
         * @param sqlCommandType
         */
        public Builder(Configuration configuration, String id, SqlSource sqlSource, SqlCommandType sqlCommandType) {
            mappedStatement.configuration = configuration;
            mappedStatement.id = id;
            mappedStatement.sqlSource = sqlSource;
            mappedStatement.statementType = StatementType.PREPARED;
            mappedStatement.resultSetType = ResultSetType.DEFAULT;
            mappedStatement.parameterMap = new ParameterMap.Builder(configuration, "defaultParameterMap", null, new ArrayList<>()).build();
            mappedStatement.resultMaps = new ArrayList<>();
            mappedStatement.sqlCommandType = sqlCommandType;
            mappedStatement.keyGenerator = configuration.isUseGeneratedKeys() && SqlCommandType.INSERT.equals(sqlCommandType) ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
            String logId = id;
            if (configuration.getLogPrefix() != null) {
                logId = configuration.getLogPrefix() + id;
            }
            mappedStatement.statementLog = LogFactory.getLog(logId);
            mappedStatement.lang = configuration.getDefaultScriptingLanguageInstance();
        }

        /**
         * <ol>
         *     <li>
         *         assert {@link #mappedStatement}的{@link MappedStatement#configuration} != null
         *     </li>
         *     <li>
         *         assert {@link #mappedStatement}的{@link MappedStatement#id} != null
         *     </li>
         *     <li>
         *         assert {@link #mappedStatement}的{@link MappedStatement#sqlSource} != null
         *     </li>
         *     <li>
         *         assert {@link #mappedStatement}的{@link MappedStatement#lang} != null
         *     </li>
         *     <li>
         *         调用{@link Collections#unmodifiableList(List)}传入{@link #mappedStatement}的{@link MappedStatement#resultMaps}得到一个不可变更的集合覆盖到{@link #mappedStatement}的{@link MappedStatement#resultMaps}
         *     </li>
         *     <li>
         *         return {@link #mappedStatement}
         *     </li>
         * </ol>
         *
         * @return
         */
        public MappedStatement build() {
            assert mappedStatement.configuration != null;
            assert mappedStatement.id != null;
            assert mappedStatement.sqlSource != null;
            assert mappedStatement.lang != null;
            mappedStatement.resultMaps = Collections.unmodifiableList(mappedStatement.resultMaps);
            return mappedStatement;
        }

        /*********************************************************************************************  Getter  ***********************************************************************************************/

        /**
         * @return {@link #mappedStatement}的{@link MappedStatement#id}
         */
        public String id() {
            return mappedStatement.id;
        }

        /*********************************************************************************************  Setter  ***********************************************************************************************/


        /**
         * {@code resource}赋值到{@link #mappedStatement}的{@link MappedStatement#resource}，然后return this
         *
         * @param resource
         * @return
         */
        public Builder resource(String resource) {
            mappedStatement.resource = resource;
            return this;
        }



        /**
         * {@code parameterMap}赋值到{@link #mappedStatement}的{@link MappedStatement#parameterMap}，然后return this
         *
         * @param parameterMap
         * @return
         */
        public Builder parameterMap(ParameterMap parameterMap) {
            mappedStatement.parameterMap = parameterMap;
            return this;
        }

        /**
         * <ol>
         *     <li>
         *         {@code resultMaps}赋值到{@link #mappedStatement}的{@link MappedStatement#resultMaps}
         *     </li>
         *     <li>
         *         遍历迭代{@code resultMaps}集合，对于每一个迭代的对象：赋值 "{@link #mappedStatement}的{@link MappedStatement#hasNestedResultMaps} || 当前迭代的{@link ResultMap}对象的{@link ResultMap#hasNestedResultMaps()}" 到{@link #mappedStatement}的{@link MappedStatement#hasNestedResultMaps}（只要集合中有一个{@link ResultMap}对象具有嵌套的{@link ResultMap}或者本身{@link MappedStatement#hasNestedResultMaps}就是true，{@link MappedStatement#hasNestedResultMaps}就会是true）
         *     </li>
         *     <li>
         *         return this
         *     </li>
         * </ol>
         *
         * @param resultMaps
         * @return
         */
        public Builder resultMaps(List<ResultMap> resultMaps) {
            mappedStatement.resultMaps = resultMaps;
            for (ResultMap resultMap : resultMaps) {
                mappedStatement.hasNestedResultMaps = mappedStatement.hasNestedResultMaps || resultMap.hasNestedResultMaps();
            }
            return this;
        }

        /**
         * {@code fetchSize}赋值到{@link #mappedStatement}的{@link MappedStatement#fetchSize}，然后return this
         *
         * @param fetchSize
         * @return
         */
        public Builder fetchSize(Integer fetchSize) {
            mappedStatement.fetchSize = fetchSize;
            return this;
        }

        /**
         * {@code timeout}赋值到{@link #mappedStatement}的{@link MappedStatement#timeout}，然后return this
         *
         * @param timeout
         * @return
         */
        public Builder timeout(Integer timeout) {
            mappedStatement.timeout = timeout;
            return this;
        }

        /**
         * {@code statementType}赋值到{@link #mappedStatement}的{@link MappedStatement#statementType}，然后return this
         *
         * @param statementType
         * @return
         */
        public Builder statementType(StatementType statementType) {
            mappedStatement.statementType = statementType;
            return this;
        }

        /**
         * 判断：{@code resultSetType} == null
         * <ul>
         *     <li>
         *         true：赋值{@link ResultSetType#DEFAULT}到{@link #mappedStatement}的{@link MappedStatement#resultSetType}
         *     </li>
         *     <li>
         *         false：{@code resultSetType}赋值到{@link #mappedStatement}的{@link MappedStatement#resultSetType}
         *     </li>
         * </ul>
         * 然后 return this
         *
         * @param resultSetType
         * @return
         */
        public Builder resultSetType(ResultSetType resultSetType) {
            mappedStatement.resultSetType = resultSetType == null ? ResultSetType.DEFAULT : resultSetType;
            return this;
        }

        /**
         * {@code cache}赋值到{@link #mappedStatement}的{@link MappedStatement#cache}，然后return this
         *
         * @param cache
         * @return
         */
        public Builder cache(Cache cache) {
            mappedStatement.cache = cache;
            return this;
        }

        /**
         * {@code flushCacheRequired}赋值到{@link #mappedStatement}的{@link MappedStatement#flushCacheRequired}，然后return this
         *
         * @param flushCacheRequired
         * @return
         */
        public Builder flushCacheRequired(boolean flushCacheRequired) {
            mappedStatement.flushCacheRequired = flushCacheRequired;
            return this;
        }

        /**
         * {@code useCache}赋值到{@link #mappedStatement}的{@link MappedStatement#useCache}，然后return this
         *
         * @param useCache
         * @return
         */
        public Builder useCache(boolean useCache) {
            mappedStatement.useCache = useCache;
            return this;
        }

        /**
         * {@code resultOrdered}赋值到{@link #mappedStatement}的{@link MappedStatement#resultOrdered}，然后return this
         *
         * @param resultOrdered
         * @return
         */
        public Builder resultOrdered(boolean resultOrdered) {
            mappedStatement.resultOrdered = resultOrdered;
            return this;
        }

        /**
         * {@code keyGenerator}赋值到{@link #mappedStatement}的{@link MappedStatement#keyGenerator}，然后return this
         *
         * @param keyGenerator
         * @return
         */
        public Builder keyGenerator(KeyGenerator keyGenerator) {
            mappedStatement.keyGenerator = keyGenerator;
            return this;
        }

        /**
         * 调用{@link #delimitedStringToArray(String)}传入{@code keyProperty}得到分割之后的数组赋值到{@link #mappedStatement}的{@link MappedStatement#keyProperties}，然后return this
         *
         * @param keyProperty
         * @return
         */
        public Builder keyProperty(String keyProperty) {
            mappedStatement.keyProperties = delimitedStringToArray(keyProperty);
            return this;
        }

        /**
         * 调用{@link #delimitedStringToArray(String)}传入{@code keyColumn}得到分割之后的数组赋值到{@link #mappedStatement}的{@link MappedStatement#keyProperties}，然后return this
         *
         * @param keyColumn
         * @return
         */
        public Builder keyColumn(String keyColumn) {
            mappedStatement.keyColumns = delimitedStringToArray(keyColumn);
            return this;
        }

        /**
         * {@code databaseId}赋值到{@link #mappedStatement}的{@link MappedStatement#databaseId}，然后return this
         *
         * @param databaseId
         * @return
         */
        public Builder databaseId(String databaseId) {
            mappedStatement.databaseId = databaseId;
            return this;
        }

        /**
         * {@code driver}赋值到{@link #mappedStatement}的{@link MappedStatement#lang}，然后return this
         *
         * @param driver
         * @return
         */
        public Builder lang(LanguageDriver driver) {
            mappedStatement.lang = driver;
            return this;
        }

        /**
         * 调用{@link #delimitedStringToArray(String)}传入{@code resultSet}得到分割之后的数组赋值到{@link #mappedStatement}的{@link MappedStatement#resultSets}，然后return this
         *
         * @param resultSet
         * @return
         */
        public Builder resultSets(String resultSet) {
            mappedStatement.resultSets = delimitedStringToArray(resultSet);
            return this;
        }

        /** @deprecated Use {@link #resultSets} */
        @Deprecated
        public Builder resulSets(String resultSet) {
            mappedStatement.resultSets = delimitedStringToArray(resultSet);
            return this;
        }


    }

    /**
     * 判断：{@code in} == null 或者 {@code in}.trim.length() == 0
     * <ul>
     *     <li>
     *         true：返回null
     *     </li>
     *     <li>
     *         false：返回{@code in}.split(",")
     *     </li>
     * </ul>
     *
     * @param in
     * @return
     */
    private static String[] delimitedStringToArray(String in) {
        if (in == null || in.trim().length() == 0) {
            return null;
        } else {
            return in.split(",");
        }
    }

    /**************************************************************  Getter  ******************************************************************/

    /**
     * @return {@link #keyGenerator}
     */
    public KeyGenerator getKeyGenerator() {
        return keyGenerator;
    }

    /**
     * @return {@link #sqlCommandType}
     */
    public SqlCommandType getSqlCommandType() {
        return sqlCommandType;
    }

    /**
     * @return {@link #resource}
     */
    public String getResource() {
        return resource;
    }

    /**
     * @return {@link #configuration}
     */
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * @return {@link #id}
     */
    public String getId() {
        return id;
    }

    /**
     * @return {@link #hasNestedResultMaps}
     */
    public boolean hasNestedResultMaps() {
        return hasNestedResultMaps;
    }

    /**
     * @return {@link #fetchSize}
     */
    public Integer getFetchSize() {
        return fetchSize;
    }

    /**
     * @return {@link #timeout}
     */
    public Integer getTimeout() {
        return timeout;
    }

    /**
     * @return {@link #statementType}
     */
    public StatementType getStatementType() {
        return statementType;
    }

    /**
     * @return {@link #resultSetType}
     */
    public ResultSetType getResultSetType() {
        return resultSetType;
    }

    /**
     * @return {@link #sqlSource}
     */
    public SqlSource getSqlSource() {
        return sqlSource;
    }

    /**
     * @return {@link #parameterMap}
     */
    public ParameterMap getParameterMap() {
        return parameterMap;
    }

    /**
     * @return {@link #resultMaps}
     */
    public List<ResultMap> getResultMaps() {
        return resultMaps;
    }

    /**
     * @return {@link #cache}
     */
    public Cache getCache() {
        return cache;
    }

    /**
     * @return {@link #flushCacheRequired}
     */
    public boolean isFlushCacheRequired() {
        return flushCacheRequired;
    }

    /**
     * @return {@link #useCache}
     */
    public boolean isUseCache() {
        return useCache;
    }

    /**
     * @return {@link #resultOrdered}
     */
    public boolean isResultOrdered() {
        return resultOrdered;
    }

    /**
     * @return {@link #databaseId}
     */
    public String getDatabaseId() {
        return databaseId;
    }

    /**
     * @return {@link #keyProperties}
     */
    public String[] getKeyProperties() {
        return keyProperties;
    }

    /**
     * @return {@link #keyColumns}
     */
    public String[] getKeyColumns() {
        return keyColumns;
    }

    /**
     * @return {@link #statementLog}
     */
    public Log getStatementLog() {
        return statementLog;
    }

    /**
     * @return {@link #lang}
     */
    public LanguageDriver getLang() {
        return lang;
    }

    /**
     * @return {@link #resultSets}
     */
    public String[] getResultSets() {
        return resultSets;
    }

    /** @deprecated Use {@link #getResultSets()} */
    @Deprecated
    public String[] getResulSets() {
        return resultSets;
    }

    // 获得 BoundSql 对象
    public BoundSql getBoundSql(Object parameterObject) {
        // 获得 BoundSql 对象
        BoundSql boundSql = sqlSource.getBoundSql(parameterObject);
        // 忽略，因为 <parameterMap /> 已经废弃，参见 http://www.mybatis.org/mybatis-3/zh/sqlmap-xml.html 文档
        List<ParameterMapping> parameterMappings = boundSql.getParameterMappings();
        if (parameterMappings == null || parameterMappings.isEmpty()) {
            boundSql = new BoundSql(configuration, boundSql.getSql(), parameterMap.getParameterMappings(), parameterObject);
        }

        // check for nested result maps in parameter mappings (issue #30)
        // 判断传入的参数中，是否有内嵌的结果 ResultMap 。如果有，则修改 hasNestedResultMaps 为 true
        // 存储过程相关，暂时无视
        for (ParameterMapping pm : boundSql.getParameterMappings()) {
            String rmId = pm.getResultMapId();
            if (rmId != null) {
                ResultMap rm = configuration.getResultMap(rmId);
                if (rm != null) {
                    hasNestedResultMaps |= rm.hasNestedResultMaps();
                }
            }
        }

        return boundSql;
    }



}
