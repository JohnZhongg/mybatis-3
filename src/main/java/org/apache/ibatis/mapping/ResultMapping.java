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

import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * {@link ResultMap} 中的每一条结果字段的映射
 *
 * @author Clinton Begin
 */
public class ResultMapping {

    /**
     * MyBatis Configuration 对象
     */
    private Configuration configuration;
    /**
     * 映射关系承载类的属性名称或者其构造器的参数名称
     */
    private String property;
    /**
     * 数据库的字段名
     */
    private String column;
    /**
     * 映射关系承载类的属性类型获取其构造器的参数类型
     */
    private Class<?> javaType;
    /**
     * 数据库的字段的类型
     */
    private JdbcType jdbcType;
    /**
     * 处理映射的 TypeHandler 对象
     */
    private TypeHandler<?> typeHandler;
    /**
     * 内嵌的 ResultMap 编号
     */
    private String nestedResultMapId;
    /**
     * 内嵌的查询语句编号
     */
    private String nestedQueryId;
    /**
     * 非空字段集合
     */
    private Set<String> notNullColumns;
    /**
     * 当连接多表时，你将不得不使用列别名来避免ResultSet中的重复列名。指定columnPrefix允许你映射列名到一个外部的结果集中。
     */
    private String columnPrefix;
    /**
     * ResultFlag 集合
     */
    private List<ResultFlag> flags;
    /**
     * 组合字段解析后的 ResultMapping 集合
     *
     * {@link org.apache.ibatis.builder.MapperBuilderAssistant#parseCompositeColumnName(String)}
     */
    private List<ResultMapping> composites;
    /**
     * 标识这个将会从哪里加载的复杂类型数据的结果集合的名称
     */
    private String resultSet; // 存储过程相关，忽略
    /**
     * 标识出包含 foreign keys 的列的名称。这个 foreign keys的值将会和父类型中指定的列属性的值相匹配
     */
    private String foreignColumn;
    /**
     * 是否懒加载
     */
    private boolean lazy;

    ResultMapping() {
    }

    /**
     * 构造器
     */
    public static class Builder {

        private ResultMapping resultMapping = new ResultMapping();

        /**
         * 调用{@link #Builder(Configuration, String)}传入{@code configuration}和{@code property}，并将入参{@code column}和{@code typehandler}分别设置给成员变量{@link #resultMapping}的属性{@link ResultMapping#column}和{@link ResultMapping#typeHandler}
         *
         * @param configuration
         * @param property
         * @param column
         * @param typeHandler
         */
        public Builder(Configuration configuration, String property, String column, TypeHandler<?> typeHandler) {
            this(configuration, property);
            resultMapping.column = column;
            resultMapping.typeHandler = typeHandler;
        }

        /**
         * 调用{@link #Builder(Configuration, String)}传入{@code configuration}和{@code property}，并将入参{@code column}和{@code javaType}分别设置给成员变量{@link #resultMapping}的属性{@link ResultMapping#column}和{@link ResultMapping#javaType}
         *
         * @param configuration
         * @param property
         * @param column
         * @param javaType
         */
        public Builder(Configuration configuration, String property, String column, Class<?> javaType) {
            this(configuration, property);
            resultMapping.column = column;
            resultMapping.javaType = javaType;
        }

        /**
         * 设置成员变量{@link #resultMapping}的属性：
         * <ol>
         *     <li>
         *         将入参{@code configuration}和{@code property}分别设置到{@link ResultMapping#configuration}、{@link ResultMapping#property}
         *     </li>
         *     <li>
         *         new一个{@link ArrayList}设置到{@link ResultMapping#flags}、new一个{@link ArrayList}设置到{@link ResultMapping#composites}、获取{@link Configuration#isLazyLoadingEnabled()}设置到{@link ResultMapping#lazy}
         *     </li>
         * </ol>
         *
         * @param configuration
         * @param property
         */
        public Builder(Configuration configuration, String property) {
            resultMapping.configuration = configuration;
            resultMapping.property = property;
            resultMapping.flags = new ArrayList<>();
            resultMapping.composites = new ArrayList<>();
            resultMapping.lazy = configuration.isLazyLoadingEnabled();
        }

        public Builder javaType(Class<?> javaType) {
            resultMapping.javaType = javaType;
            return this;
        }

        public Builder jdbcType(JdbcType jdbcType) {
            resultMapping.jdbcType = jdbcType;
            return this;
        }

        public Builder nestedResultMapId(String nestedResultMapId) {
            resultMapping.nestedResultMapId = nestedResultMapId;
            return this;
        }

        public Builder nestedQueryId(String nestedQueryId) {
            resultMapping.nestedQueryId = nestedQueryId;
            return this;
        }

        public Builder resultSet(String resultSet) {
            resultMapping.resultSet = resultSet;
            return this;
        }

        public Builder foreignColumn(String foreignColumn) {
            resultMapping.foreignColumn = foreignColumn;
            return this;
        }

        public Builder notNullColumns(Set<String> notNullColumns) {
            resultMapping.notNullColumns = notNullColumns;
            return this;
        }

        public Builder columnPrefix(String columnPrefix) {
            resultMapping.columnPrefix = columnPrefix;
            return this;
        }

        public Builder flags(List<ResultFlag> flags) {
            resultMapping.flags = flags;
            return this;
        }

        public Builder typeHandler(TypeHandler<?> typeHandler) {
            resultMapping.typeHandler = typeHandler;
            return this;
        }

        public Builder composites(List<ResultMapping> composites) {
            resultMapping.composites = composites;
            return this;
        }

        public Builder lazy(boolean lazy) {
            resultMapping.lazy = lazy;
            return this;
        }

        /**
         * 构建一个{@link ResultMapping}对象：
         * <ol>
         *     <li>
         *         分别将成员变量{@link #resultMapping}的{@link ResultMapping#flags}和{@link ResultMapping#composites}通过{@link Collections#unmodifiableList(List)}转化成不可修改集合
         *     </li>
         *     <li>
         *         调用{@link #resolveTypeHandler()}尝试解析成员变量{@link #resultMapping}的{@link ResultMapping#typeHandler}
         *     </li>
         *     <li>
         *         调用{@link #validate()}对成员变量{@link #resultMapping}进行相关必要的校验
         *     </li>
         *     <li>
         *         校验如果通过证明该{@link #resultMapping}是合格的，将其作为方法结果进行返回
         *     </li>
         * </ol>
         *
         * @return
         */
        public ResultMapping build() {
            resultMapping.flags = Collections.unmodifiableList(resultMapping.flags);
            resultMapping.composites = Collections.unmodifiableList(resultMapping.composites);
            resolveTypeHandler();
            validate();
            return resultMapping;
        }

        /**
         * 校验：
         * <ol>
         *     <li>
         *         成员变量{@link #resultMapping}的{@link ResultMapping#nestedQueryId}和{@link ResultMapping#nestedResultMapId}不能同时不为null，否则抛出异常{@link IllegalStateException}
         *     </li>
         *     <li>
         *         成员变量{@link #resultMapping}的{@link ResultMapping#nestedQueryId}和{@link ResultMapping#nestedResultMapId}和{@link ResultMapping#typeHandler}不能同时为null，否则抛出异常{@link IllegalStateException}
         *     </li>
         *     <li>
         *         成员变量{@link #resultMapping}的{@link ResultMapping#nestedResultMapId}和{@link ResultMapping#column}和{@link ResultMapping#composites}不能为null和empty，否则抛出异常{@link IllegalStateException}
         *     </li>
         *     <li>
         *         如果成员变量{@link #resultMapping}的{@link ResultMapping#resultSet}不为null，则使用逗号","对{@link ResultMapping#column}和{@link ResultMapping#foreignColumn}进行切割分别得到两个数组，如果他们的长度不相等，则抛出异常{@link IllegalStateException}
         *     </li>
         * </ol>
         *
         */
        private void validate() {
            // Issue #697: cannot define both nestedQueryId and nestedResultMapId
            if (resultMapping.nestedQueryId != null && resultMapping.nestedResultMapId != null) {
                throw new IllegalStateException("Cannot define both nestedQueryId and nestedResultMapId in property " + resultMapping.property);
            }
            // Issue #5: there should be no mappings without typehandler
            if (resultMapping.nestedQueryId == null && resultMapping.nestedResultMapId == null && resultMapping.typeHandler == null) {
                throw new IllegalStateException("No typehandler found for property " + resultMapping.property);
            }
            // Issue #4 and GH #39: column is optional only in nested resultmaps but not in the rest
            if (resultMapping.nestedResultMapId == null && resultMapping.column == null && resultMapping.composites.isEmpty()) {
                throw new IllegalStateException("Mapping is missing column attribute for property " + resultMapping.property);
            }
            if (resultMapping.getResultSet() != null) {
                int numColumns = 0;
                if (resultMapping.column != null) {
                    numColumns = resultMapping.column.split(",").length;
                }
                int numForeignColumns = 0;
                if (resultMapping.foreignColumn != null) {
                    numForeignColumns = resultMapping.foreignColumn.split(",").length;
                }
                if (numColumns != numForeignColumns) {
                    throw new IllegalStateException("There should be the same number of columns and foreignColumns in property " + resultMapping.property);
                }
            }
        }

        /**
         * 当成员变量{@link #resultMapping}的{@link ResultMapping#typeHandler}为null的时候尝试去解析它：<br>
         *
         * 如果成员变量{@link #resultMapping}的{@link ResultMapping#typeHandler}为null且其{@link ResultMapping#javaType}不为null，通过它的成员变量{@link ResultMapping#configuration}获取{@link Configuration}对象然后通过{@link Configuration#getTypeHandlerRegistry()}获取{@link TypeHandlerRegistry}对象，然后调用
         * {@link TypeHandlerRegistry#getTypeHandler(Class, JdbcType)}分别传入{@link ResultMapping#javaType}和{@link ResultMapping#jdbcType}获取{@link TypeHandler}对象并设置到
         * 它的成员变量{@link ResultMapping#typeHandler}
         */
        private void resolveTypeHandler() {
            if (resultMapping.typeHandler == null && resultMapping.javaType != null) {
                Configuration configuration = resultMapping.configuration;
                TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
                resultMapping.typeHandler = typeHandlerRegistry.getTypeHandler(resultMapping.javaType, resultMapping.jdbcType);
            }
        }

        public Builder column(String column) {
            resultMapping.column = column;
            return this;
        }
    }

    public String getProperty() {
        return property;
    }

    public String getColumn() {
        return column;
    }

    public Class<?> getJavaType() {
        return javaType;
    }

    public JdbcType getJdbcType() {
        return jdbcType;
    }

    public TypeHandler<?> getTypeHandler() {
        return typeHandler;
    }

    public String getNestedResultMapId() {
        return nestedResultMapId;
    }

    public String getNestedQueryId() {
        return nestedQueryId;
    }

    public Set<String> getNotNullColumns() {
        return notNullColumns;
    }

    public String getColumnPrefix() {
        return columnPrefix;
    }

    public List<ResultFlag> getFlags() {
        return flags;
    }

    public List<ResultMapping> getComposites() {
        return composites;
    }

    /**
     * @return {@link #composites} != null && !{@link #composites}.isEmpty()
     */
    public boolean isCompositeResult() {
        return this.composites != null && !this.composites.isEmpty();
    }

    public String getResultSet() {
        return this.resultSet;
    }

    public String getForeignColumn() {
        return foreignColumn;
    }

    public void setForeignColumn(String foreignColumn) {
        this.foreignColumn = foreignColumn;
    }

    public boolean isLazy() {
        return lazy;
    }

    public void setLazy(boolean lazy) {
        this.lazy = lazy;
    }

    /**
     * 覆盖逻辑：两个{@link ResultMapping}对象的{@link #property}都不为null则"equals"
     *
     * <ol>
     *     <li>
     *         如果当前{@link ResultMapping}对象 == 传入参数{@code o}，返回true，否则下一步
     *     </li>
     *     <li>
     *         如果传入参数{@code o}为null 或者 当前对象的{@link Class}对象 != 传入{@code o}.getClass()，返回false，否则下一步
     *     </li>
     *     <li>
     *         如果当前对象成员变量{@link #property}为null 或者 !{@link #property}.equals({@code o}.property)，返回false，否则下一步
     *     </li>
     *     <li>
     *         上面都不符合，则返回true
     *     </li>
     * </ol>
     *
     * @param o
     * @return
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ResultMapping that = (ResultMapping) o;

        if (property == null || !property.equals(that.property)) {
            return false;
        }

        return true;
    }

    /**
     * 覆盖逻辑：
     * <ol>
     *     <li>
     *         如果成员变量{@link #property}不为null，则返回{@link #property}.hashCode()，否则下一步
     *     </li>
     *     <li>
     *         如果成员变量{@link #column}不为null，则返回{@link #column}.hashCode()，否则下一步
     *     </li>
     *     <li>
     *         上面都不满足，返回 0
     *     </li>
     * </ol>
     *
     * @return
     */
    @Override
    public int hashCode() {
        if (property != null) {
            return property.hashCode();
        } else if (column != null) {
            return column.hashCode();
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ResultMapping{");
        //sb.append("configuration=").append(configuration); // configuration doesn't have a useful .toString()
        sb.append("property='").append(property).append('\'');
        sb.append(", column='").append(column).append('\'');
        sb.append(", javaType=").append(javaType);
        sb.append(", jdbcType=").append(jdbcType);
        //sb.append(", typeHandler=").append(typeHandler); // typeHandler also doesn't have a useful .toString()
        sb.append(", nestedResultMapId='").append(nestedResultMapId).append('\'');
        sb.append(", nestedQueryId='").append(nestedQueryId).append('\'');
        sb.append(", notNullColumns=").append(notNullColumns);
        sb.append(", columnPrefix='").append(columnPrefix).append('\'');
        sb.append(", flags=").append(flags);
        sb.append(", composites=").append(composites);
        sb.append(", resultSet='").append(resultSet).append('\'');
        sb.append(", foreignColumn='").append(foreignColumn).append('\'');
        sb.append(", lazy=").append(lazy);
        sb.append('}');
        return sb.toString();
    }

}
