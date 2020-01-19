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

import org.apache.ibatis.mapping.ParameterMapping;
import org.apache.ibatis.mapping.ParameterMode;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.parsing.TokenHandler;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.scripting.xmltags.DynamicContext;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link SqlSource} 构建器，负责将 SQL 语句中的 `#{}` 替换成相应的 ? 占位符，并获取该占位符对应的 {@link org.apache.ibatis.mapping.ParameterMapping} 对象
 *
 * @author Clinton Begin
 */
public class SqlSourceBuilder extends BaseBuilder {

    private static final String parameterProperties = "javaType,jdbcType,mode,numericScale,resultMap,typeHandler,jdbcTypeName";

    /**
     * 调用父构造方法{@link BaseBuilder#BaseBuilder(Configuration)}传入{@code configuration}
     *
     * @param configuration
     */
    public SqlSourceBuilder(Configuration configuration) {
        super(configuration);
    }

    /**
     * 执行解析原始 SQL ，成为 SqlSource 对象
     * <ol>
     *     <li>
     *         调用{@link ParameterMappingTokenHandler#ParameterMappingTokenHandler(Configuration, Class, Map)}传入{@link #configuration}、{@code parameterType}、{@code additionalParameters}实例化一个"#{}"Token处理器{@link ParameterMappingTokenHandler}对象（解析除一个{@link ParameterMapping}对象以及在对应的sql字符串中替换该Token为"?"）
     *     </li>
     *     <li>
     *         调用{@link GenericTokenParser#GenericTokenParser(String, String, TokenHandler)}传入"${"、"}"、第一步得到的"#{}"Token处理器对象{@link ParameterMappingTokenHandler}对象 得到一个完整的"#{}"Token解析器
     *     </li>
     *     <li>
     *         调用{@link GenericTokenParser#parse(String)}传入{@code originalSql}进行sql解析，返回的结果是一个将"#{}"替换成"?"的sql字符串
     *     </li>
     *     <li>
     *         调用构造器{@link StaticSqlSource#StaticSqlSource(Configuration, String, List)}传入 {@link #configuration}、第三步得到的sql字符串、经过第三步的处理之后的第一步new的{@link ParameterMappingTokenHandler}对象的{@link ParameterMappingTokenHandler#getParameterMappings()} 三个参数构造一个{@link StaticSqlSource}对象并返回，本方法结束
     *     </li>
     * </ol>
     *
     * @param originalSql 处理了动态sql（含有mybatis sql标签和"${}"token的{@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <update/>}、{@code <selectKey/>}标签）之后构建出来的一条还含有"#{}"Token的完整sql（经过了{@link org.apache.ibatis.scripting.xmltags.SqlNode#apply(DynamicContext)}的{@link DynamicContext#getSql()}）
     * @param parameterType 承载了sql参数值的对象的类型（如果对象是null则是{@link Object}.class）（{@link DynamicContext#bindings}["_parameter"].getClass()）
     * @param additionalParameters 附加参数集合。 {@link DynamicContext#getBindings()} （可能是空集合）
     * @return SqlSource 对象
     */
    public SqlSource parse(String originalSql, Class<?> parameterType, Map<String, Object> additionalParameters) {
        ParameterMappingTokenHandler handler = new ParameterMappingTokenHandler(configuration, parameterType, additionalParameters);
        GenericTokenParser parser = new GenericTokenParser("#{", "}", handler);
        String sql = parser.parse(originalSql);
        return new StaticSqlSource(configuration, sql, handler.getParameterMappings());
    }

    private static class ParameterMappingTokenHandler extends BaseBuilder implements TokenHandler {

        /**
         * {@link ParameterMapping} 集合（一个"#{}"Token会有一个{@link ParameterMapping}对象元素）
         */
        private List<ParameterMapping> parameterMappings = new ArrayList<>();
        /**
         * 承载了sql参数值的对象的类型（如果对象是null则是{@link Object}.class）（{@link DynamicContext#bindings}["_parameter"].getClass()）
         */
        private Class<?> parameterType;
        /**
         * {@link DynamicContext#getBindings()}对应的 {@link MetaObject} 对象
         */
        private MetaObject metaParameters;

        /**
         * <ol>
         *     <li>
         *         调用父构造器{@link BaseBuilder#BaseBuilder(Configuration)}传入{@code configuration}构造父对象
         *     </li>
         *     <li>
         *         {@code parameterType}赋值到{@link #parameterType}
         *     </li>
         *     <li>
         *         调用{@code configuration}的{@link Configuration#newMetaObject(Object)}传入{@code additionalParameters}构建其对应的{@link MetaObject}对象赋值到{@link #metaParameters}
         *     </li>
         * </ol>
         *
         * @param configuration 全局的{@link Configuration}对象
         * @param parameterType 承载了sql参数值的对象的类型（如果对象是null则是{@link Object}.class）（{@link DynamicContext#bindings}["_parameter"].getClass()）
         * @param additionalParameters 附加参数集合。 {@link DynamicContext#getBindings()} （可能是空集合）
         */
        public ParameterMappingTokenHandler(Configuration configuration, Class<?> parameterType, Map<String, Object> additionalParameters) {
            super(configuration);
            this.parameterType = parameterType;
            this.metaParameters = configuration.newMetaObject(additionalParameters);
        }

        /**
         * @return {@link #parameterMappings}
         */
        public List<ParameterMapping> getParameterMappings() {
            return parameterMappings;
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #buildParameterMapping(String)}传入{@code content}针对一个当前Token构建一个{@link ParameterMapping}对象，然后添加到{@link #parameterMappings}中
         *     </li>
         *     <li>
         *         返回"?"字符串，替换sql中的"#{xxxx}"
         *     </li>
         * </ol>
         *
         * @param content Token 字符串 （"#{}"中的内容）
         * @return
         */
        @Override
        public String handleToken(String content) {
            parameterMappings.add(buildParameterMapping(content));
            return "?";
        }

        /**
         * TODO
         * <ol>
         *     <li>
         *         调用{@link #parseParameterMapping(String)}传入{@code content}对Token字符串进行解析得到一个{@link ParameterExpression}对象
         *     </li>
         *     <li>
         *         获取mybatis自动识别的属性java类型（就是缺省值）"propertyType"：
         *         <ul>
         *             调用{@link ParameterExpression#get(Object)}传入"property"获取对应的字符串，对于获取到的字符串进行如下判断和操作：
         *             <li>
         *                 调用{@link #metaParameters}的{@link MetaObject#hasGetter(String)}传入"property"对应的字符串返回结果为true：调用{@link #metaParameters}的{@link MetaObject#getGetterType(String)}传入"property"对应的字符串得到当前"#{}"Token中定义的属性的类型{@link Class}对象然后赋值到变量"propertyType"中，步骤2结束，进入步骤3；否则进入步骤2下一判断
         *             </li>
         *             <li>
         *                 调用{@link #typeHandlerRegistry}的{@link org.apache.ibatis.type.TypeHandlerRegistry#hasTypeHandler(Class)}传入{@link #parameterType}返回结果为true：赋值{@link #parameterType}到变量"propertyType"中，步骤2结束，进入步骤3；否则进入步骤2下一判断
         *             </li>
         *             <li>
         *                 调用{@link JdbcType#CURSOR}的{@link JdbcType#name()}（"CURSOR"），然后调用得到的字符串的{@link String#equals(Object)}传入从第一步得到的{@link ParameterExpression}的{@link ParameterExpression#get(Object)}中获取到的"jdbcType"对应的字符串返回true：赋值{@link java.sql.ResultSet}.class到变量"propertyType"，步骤2结束，进入步骤3；否则进入步骤2下一判断
         *             </li>
         *             <li>
         *                 如果从{@link ParameterExpression#get(Object)}获取的"property"对应的字符串为null 或者 {@link Map}.class的{@link Class#isAssignableFrom(Class)}传入{@link #parameterType}为true：{@link Object}.class赋值到变量"propertyType"，步骤2结束，进入步骤3；否则进入步骤2下一判断
         *             </li>
         *             <li>
         *                 来到这里，所有操作都是必然执行的：
         *                 <ol>
         *                     <li>
         *                         调用{@link MetaClass#forClass(Class, ReflectorFactory)}传入{@link #parameterType}、{@link #configuration}的{@link Configuration#getReflectorFactory()} 两个参数构建{@link #parameterType}.getClass()的{@link MetaClass}对象
         *                     </li>
         *                     <li>
         *                         调用{@link MetaClass#hasGetter(String)}传入从{@link ParameterExpression#get(Object)}获取的"property"对应的字符串，判断返回结果是否为true：
         *                         <ul>
         *                             <li>
         *                                 true：调用{@link MetaClass#getGetterType(String)}传入从{@link ParameterExpression#get(Object)}获取的"property"对应的字符串得到的结果赋值到变量"propertyType"，步骤2结束，进入步骤3
         *                             </li>
         *                             <li>
         *                                 false：赋值{@link Object}.class到变量"propertyType"，步骤2结束，进入步骤3
         *                             </li>
         *                         </ul>
         *                     </li>
         *                 </ol>
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         调用{@link ParameterMapping.Builder#Builder(Configuration, String, Class)}传入{@link #configuration}、从{@link ParameterExpression#get(Object)}获取的"property"对应的字符串、第二步骤获取到的属性java类型缺省值 三个参数构建一个{@link ParameterMapping.Builder}对象
         *     </li>
         *     <li>
         *         处理{@link ParameterExpression}中"property"之外的value：
         *         <ol>
         *             遍历迭代{@link ParameterExpression#entrySet()}，对于每一个迭代的{@link java.util.Map.Entry}对象：
         *             <li>
         *                 调用{@link Map.Entry#getKey()}得到当前迭代元素的key赋值到变量"name"、调用{@link Map.Entry#getValue()}得到当前迭代元素的key赋值到变量"value"
         *             </li>
         *             <li>
         *                 对前面得到的"name"变量和"value"变量作以下判断和操作：
         *                 <ul>
         *                     <li>
         *                         如果 字符串"javaType".equals("name"变量)：调用{@link BaseBuilder#resolveClass(String)}传入"value"变量得到的结果再传入到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#javaType(Class)}，覆盖前面自动识别的缺省值，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"jdbcType".equals("name"变量)：调用{@link BaseBuilder#resolveJdbcType(String)}传入"value"变量得到的结果再传入到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#jdbcType(JdbcType)}，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"mode".equals("name"变量)：调用{@link BaseBuilder#resolveParameterMode(String)} 传入"value"变量得到的结果再传入到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#mode(ParameterMode)}，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"numericScale".equals("name"变量)：调用{@link Integer#valueOf(String)} 传入"value"变量得到的结果再传入到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#numericScale(Integer)}，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"resultMap".equals("name"变量)：直接传入"value"变量到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#resultMapId(String)} ，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"typeHandler".equals("name"变量)：调用{@link BaseBuilder#resolveTypeHandler(Class, String)}传入"第二步中mybatis自动识别的"javaType"缺省值或者第一个判断中用户自定义的值"、"value"变量 两个参数解析对应的{@link org.apache.ibatis.type.TypeHandler}对象，然后再传入到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#typeHandler(TypeHandler)} ，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"jdbcTypeName".equals("name"变量)：直接传入"value"变量到方法调用{@link org.apache.ibatis.mapping.ParameterMapping.Builder#jdbcTypeName(String)} ，本迭代结束，进入下一循环迭代；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         如果 字符串"property".equals("name"变量)：什么也不做，进入下一循环迭代（第二步已经处理了）
         *                     </li>
         *                     <li>
         *                         如果 字符串"expression".equals("name"变量)：抛出异常{@link BuilderException}("Expression based parameters are not supported yet")；否则什么也不做，进入下一判断
         *                     </li>
         *                     <li>
         *                         来到这里，直接抛出异常{@link BuilderException}【#{}中只能有以下自定义内容（{@link ParameterExpression}的"attributes"部分）：javaType,jdbcType,mode,numericScale,resultMap,typeHandler,jdbcTypeName】
         *                     </li>
         *                 </ul>
         *             </li>
         *         </ol>
         *     </li>
         *     <li>
         *         上述处理完成之后，调用{@link ParameterMapping.Builder#build()}方法构建一个{@link ParameterMapping}对象并返回，本方法结束
         *     </li>
         * </ol>
         *
         * @param content Token 字符串 （"#{}"中的内容）
         * @return
         */
        private ParameterMapping buildParameterMapping(String content) {
            Map<String, String> propertiesMap = parseParameterMapping(content);
            String property = propertiesMap.get("property");
            Class<?> propertyType;
            if (metaParameters.hasGetter(property)) { // issue #448 get type from additional params
                propertyType = metaParameters.getGetterType(property);
            } else if (typeHandlerRegistry.hasTypeHandler(parameterType)) {
                propertyType = parameterType;
            } else if (JdbcType.CURSOR.name().equals(propertiesMap.get("jdbcType"))) {
                propertyType = java.sql.ResultSet.class;
            } else if (property == null || Map.class.isAssignableFrom(parameterType)) {
                propertyType = Object.class;
            } else {
                MetaClass metaClass = MetaClass.forClass(parameterType, configuration.getReflectorFactory());
                if (metaClass.hasGetter(property)) {
                    propertyType = metaClass.getGetterType(property);
                } else {
                    propertyType = Object.class;
                }
            }
            ParameterMapping.Builder builder = new ParameterMapping.Builder(configuration, property, propertyType);
            Class<?> javaType = propertyType;
            String typeHandlerAlias = null;
            for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
                String name = entry.getKey();
                String value = entry.getValue();
                if ("javaType".equals(name)) {
                    javaType = resolveClass(value);
                    builder.javaType(javaType);
                } else if ("jdbcType".equals(name)) {
                    builder.jdbcType(resolveJdbcType(value));
                } else if ("mode".equals(name)) {
                    builder.mode(resolveParameterMode(value));
                } else if ("numericScale".equals(name)) {
                    builder.numericScale(Integer.valueOf(value));
                } else if ("resultMap".equals(name)) {
                    builder.resultMapId(value);
                } else if ("typeHandler".equals(name)) {
                    typeHandlerAlias = value;
                } else if ("jdbcTypeName".equals(name)) {
                    builder.jdbcTypeName(value);
                } else if ("property".equals(name)) {
                    // Do Nothing
                } else if ("expression".equals(name)) {
                    throw new BuilderException("Expression based parameters are not supported yet");
                } else {
                    throw new BuilderException("An invalid property '" + name + "' was found in mapping #{" + content + "}.  Valid properties are " + parameterProperties);
                }
            }
            if (typeHandlerAlias != null) {
                builder.typeHandler(resolveTypeHandler(javaType, typeHandlerAlias));
            }
            return builder.build();
        }

        /**
         * 尝试调用{@link ParameterExpression#ParameterExpression(String)}传入{@code content}对Token字符串进行解析得到一个{@link ParameterExpression}对象
         *
         * @param content Token 字符串 （"#{}"中的内容）
         * @return 一个{@link ParameterExpression}对象
         */
        private Map<String, String> parseParameterMapping(String content) {
            try {
                return new ParameterExpression(content);
            } catch (BuilderException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new BuilderException("Parsing error was found in mapping #{" + content + "}.  Check syntax #{property|(expression), var1=value1, var2=value2, ...} ", ex);
            }
        }
    }

}
