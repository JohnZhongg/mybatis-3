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

import java.sql.ResultSet;

/**
 * 参数映射
 *
 * @author Clinton Begin
 */
public class ParameterMapping {

    private Configuration configuration;

    /**
     * 属性的名字
     */
    private String property;
    /**
     * 参数类型。
     *
     * 目前只需要关注 ParameterMode.IN 的情况，另外的 OUT、INOUT 是在存储过程中使用，暂时无视
     */
    private ParameterMode mode;
    /**
     * Java 类型
     */
    private Class<?> javaType = Object.class;
    /**
     * JDBC 类型
     */
    private JdbcType jdbcType;
    /**
     * 对于数值类型，还有一个小数保留位数的设置，来确定小数点后保留的位数
     */
    private Integer numericScale;
    /**
     * TypeHandler 对象
     *
     * {@link Builder#resolveTypeHandler()}
     */
    private TypeHandler<?> typeHandler;
    /**
     * 貌似只在 ParameterMode 在 OUT、INOUT 是在存储过程中使用
     */
    private String resultMapId;
    /**
     * 貌似只在 ParameterMode 在 OUT、INOUT 是在存储过程中使用
     */
    private String jdbcTypeName;
    /**
     * 表达式。
     *
     * ps：目前暂时不支持
     */
    private String expression;

    private ParameterMapping() {
    }

    public static class Builder {

        private ParameterMapping parameterMapping = new ParameterMapping();

        public Builder(Configuration configuration, String property, TypeHandler<?> typeHandler) {
            parameterMapping.configuration = configuration;
            parameterMapping.property = property;
            parameterMapping.typeHandler = typeHandler;
            parameterMapping.mode = ParameterMode.IN;
        }

        public Builder(Configuration configuration, String property, Class<?> javaType) {
            parameterMapping.configuration = configuration;
            parameterMapping.property = property;
            parameterMapping.javaType = javaType;
            parameterMapping.mode = ParameterMode.IN;
        }

        public Builder mode(ParameterMode mode) {
            parameterMapping.mode = mode;
            return this;
        }

        public Builder javaType(Class<?> javaType) {
            parameterMapping.javaType = javaType;
            return this;
        }

        public Builder jdbcType(JdbcType jdbcType) {
            parameterMapping.jdbcType = jdbcType;
            return this;
        }

        public Builder numericScale(Integer numericScale) {
            parameterMapping.numericScale = numericScale;
            return this;
        }

        public Builder resultMapId(String resultMapId) {
            parameterMapping.resultMapId = resultMapId;
            return this;
        }

        public Builder typeHandler(TypeHandler<?> typeHandler) {
            parameterMapping.typeHandler = typeHandler;
            return this;
        }

        public Builder jdbcTypeName(String jdbcTypeName) {
            parameterMapping.jdbcTypeName = jdbcTypeName;
            return this;
        }

        public Builder expression(String expression) {
            parameterMapping.expression = expression;
            return this;
        }

        /**
         * 调用{@link #resolveTypeHandler()}解析{@link #parameterMapping}的{@link ParameterMapping#typeHandler}，然后调用{@link #validate()}进行对{@link #parameterMapping}进行校验，然后返回{@link #parameterMapping}
         *
         * @return
         */
        public ParameterMapping build() {
            resolveTypeHandler();
            validate();
            return parameterMapping;
        }

        /**
         * 校验当前parameterMapping：
         * <ul>
         *     <li>
         *         如果当前的{@link #parameterMapping}对应的{@link ParameterMapping#javaType}是{@link ResultSet}.class：则当前{@link #parameterMapping}的{@link ParameterMapping#resultMapId}不能为null，否则抛出异常
         *     </li>
         *     <li>
         *         如果当前的{@link #parameterMapping}对应的{@link ParameterMapping#javaType}不是{@link ResultSet}.class并且它的{@link ParameterMapping#typeHandler}又是null，则抛出异常
         *     </li>
         * </ul>
         *
         */
        private void validate() {
            if (ResultSet.class.equals(parameterMapping.javaType)) {
                if (parameterMapping.resultMapId == null) {
                    throw new IllegalStateException("Missing resultmap in property '"
                            + parameterMapping.property + "'.  "
                            + "Parameters of type java.sql.ResultSet require a resultmap.");
                }
            } else {
                if (parameterMapping.typeHandler == null) {
                    throw new IllegalStateException("Type handler was null on parameter mapping for property '"
                            + parameterMapping.property + "'. It was either not specified and/or could not be found for the javaType ("
                            + parameterMapping.javaType.getName() + ") : jdbcType (" + parameterMapping.jdbcType + ") combination.");
                }
            }
        }

        /**
         * 如果parameterMapping.typeHandler为null则通过parameterMapping.javaType和parameterMapping.jdbcType解析当前parameterMapping对应的{@link TypeHandler}对象：<br>
         *
         * 如果parameterMapping的{@link ParameterMapping#typeHandler}是null并且{@link ParameterMapping#javaType}不是null：从{@link #parameterMapping}的{@link ParameterMapping#configuration}获取
         * {@link Configuration}对象然后通过{@link Configuration#getTypeHandlerRegistry()}获取{@link TypeHandlerRegistry}对象然后通过{@link TypeHandlerRegistry#getTypeHandler(Class, JdbcType)}分别
         * 传入{@link #parameterMapping}的{@link ParameterMapping#javaType}和{@link ParameterMapping#jdbcType}获得对应的{@link TypeHandler}直接设置到{@link #parameterMapping}的{@link ParameterMapping#typeHandler}；
         * 否则啥也不做
         */
        private void resolveTypeHandler() {
            if (parameterMapping.typeHandler == null && parameterMapping.javaType != null) {
                Configuration configuration = parameterMapping.configuration;
                TypeHandlerRegistry typeHandlerRegistry = configuration.getTypeHandlerRegistry();
                parameterMapping.typeHandler = typeHandlerRegistry.getTypeHandler(parameterMapping.javaType, parameterMapping.jdbcType);
            }
        }

    }

    public String getProperty() {
        return property;
    }

    /**
     * Used for handling output of callable statements
     * @return
     */
    public ParameterMode getMode() {
        return mode;
    }

    /**
     * Used for handling output of callable statements
     * @return
     */
    public Class<?> getJavaType() {
        return javaType;
    }

    /**
     * Used in the UnknownTypeHandler in case there is no handler for the property type
     * @return
     */
    public JdbcType getJdbcType() {
        return jdbcType;
    }

    /**
     * Used for handling output of callable statements
     * @return
     */
    public Integer getNumericScale() {
        return numericScale;
    }

    /**
     * Used when setting parameters to the PreparedStatement
     * @return
     */
    public TypeHandler<?> getTypeHandler() {
        return typeHandler;
    }

    /**
     * Used for handling output of callable statements
     * @return
     */
    public String getResultMapId() {
        return resultMapId;
    }

    /**
     * Used for handling output of callable statements
     * @return
     */
    public String getJdbcTypeName() {
        return jdbcTypeName;
    }

    /**
     * Not used
     * @return
     */
    public String getExpression() {
        return expression;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ParameterMapping{");
        //sb.append("configuration=").append(configuration); // configuration doesn't have a useful .toString()
        sb.append("property='").append(property).append('\'');
        sb.append(", mode=").append(mode);
        sb.append(", javaType=").append(javaType);
        sb.append(", jdbcType=").append(jdbcType);
        sb.append(", numericScale=").append(numericScale);
        //sb.append(", typeHandler=").append(typeHandler); // typeHandler also doesn't have a useful .toString()
        sb.append(", resultMapId='").append(resultMapId).append('\'');
        sb.append(", jdbcTypeName='").append(jdbcTypeName).append('\'');
        sb.append(", expression='").append(expression).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
