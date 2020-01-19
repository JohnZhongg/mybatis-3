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

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.property.PropertyTokenizer;
import org.apache.ibatis.session.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An actual SQL String got from an {@link SqlSource} after having processed any dynamic content.
 * The SQL may have SQL placeholders "?" and an list (ordered) of an parameter mappings
 * with the additional information for each parameter (at least the property name of the input object to read
 * the value from).
 * <p>
 * Can also have additional parameters that are created by the dynamic language (for loops, bind...).
 *
 *
 * @author Clinton Begin
 */
public class BoundSql {

    /**
     * SQL 语句（最终sql："${}"动态替换成了对应的字符串、"#{}"替换成了?）
     */
    private final String sql;
    /**
     * {@link ParameterMapping} 对象集合
     */
    private final List<ParameterMapping> parameterMappings;
    /**
     * 参数对象
     */
    private final Object parameterObject;
    /**
     * 附加的参数集合
     */
    private final Map<String, Object> additionalParameters;
    /**
     * {@link #additionalParameters} 的 {@link MetaObject} 对象
     */
    private final MetaObject metaParameters;

    /**
     * {@code sql}赋值到{@link #sql}、{@code parameterMappings}赋值到{@link #parameterMappings}、{@code parameterObject}赋值到{@link #parameterObject}、new 一个{@link HashMap}对象赋值到{@link #additionalParameters}、调用{@code configuration}的{@link Configuration#newMetaObject(Object)}传入{@link #additionalParameters}构建其对应的{@link MetaObject}对象赋值到{@link #metaParameters}
     *
     * @param configuration 全局的{@link Configuration}对象
     * @param sql SQL 语句（最终sql："${}"动态替换成了对应的字符串、"#{}"替换成了?）
     * @param parameterMappings {@link ParameterMapping} 对象集合
     * @param parameterObject 参数对象
     */
    public BoundSql(Configuration configuration, String sql, List<ParameterMapping> parameterMappings, Object parameterObject) {
        this.sql = sql;
        this.parameterMappings = parameterMappings;
        this.parameterObject = parameterObject;
        this.additionalParameters = new HashMap<>();
        this.metaParameters = configuration.newMetaObject(additionalParameters);
    }

    /**
     * @return {@link #sql}
     */
    public String getSql() {
        return sql;
    }

    /**
     * @return {@link #parameterMappings}
     */
    public List<ParameterMapping> getParameterMappings() {
        return parameterMappings;
    }

    /**
     * @return {@link #parameterObject}
     */
    public Object getParameterObject() {
        return parameterObject;
    }

    /**
     * 调用构造器{@link PropertyTokenizer#PropertyTokenizer(String)}传入{@code name}得到一个{@link PropertyTokenizer}实例，然后调用{@link PropertyTokenizer#getName()}，将得到的结果字符串传入到
     * {@link #additionalParameters}的{@link HashMap#containsKey(Object)}中并return该方法调用的结果，本方法结束
     *
     * @param name 属性表达式 TODO
     * @return
     */
    public boolean hasAdditionalParameter(String name) {
        String paramName = new PropertyTokenizer(name).getName();
        return additionalParameters.containsKey(paramName);
    }

    /**
     * 调用{@link #metaParameters}的{@link MetaObject#setValue(String, Object)}传入{@code name}和{@code value}设置对象到属性表达式对应的属性中
     *
     * @param name 属性表达式
     * @param value 要设置的对象
     */
    public void setAdditionalParameter(String name, Object value) {
        metaParameters.setValue(name, value);
    }

    /**
     * @param name 属性表达式
     * @return {@link #metaParameters}的{@link MetaObject#getValue(String)}传入{@code name}
     */
    public Object getAdditionalParameter(String name) {
        return metaParameters.getValue(name);
    }

}