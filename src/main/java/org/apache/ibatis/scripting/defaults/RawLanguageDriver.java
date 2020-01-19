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

import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.session.Configuration;

/**
 * As of 3.2.4 the default XML language is able to identify static statements
 * and create a {@link RawSqlSource}. So there is no need to use RAW unless you
 * want to make sure that there is not any dynamic tag for any reason.
 *
 * {@link RawSqlSource} 语言驱动器实现类，底层调用的是{@link XMLLanguageDriver}的api，对底层返回的{@link SqlSource}进行校验是否为{@link RawSqlSource}，不是就抛出异常
 *
 * @since 3.2.0
 * @author Eduardo Macarron
 */
public class RawLanguageDriver extends XMLLanguageDriver {

    /**
     * <ol>
     *     <li>
     *         调用父方法 {@link XMLLanguageDriver#createSqlSource(Configuration, XNode, Class)}传入{@code configuration}、{@code script}、{@code parameterType}三个参数，构建{@link SqlSource}对象
     *     </li>
     *     <li>
     *         将第一步返回的{@link SqlSource}对象传入{@link #checkIsNotDynamic(SqlSource)}进行校验
     *     </li>
     *     <li>
     *         return 第一步返回的{@link SqlSource}对象，本方法结束
     *     </li>
     * </ol>
     *
     * @param configuration The MyBatis configuration
     * @param script        XNode parsed from a XML file （{@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}、{@code <selectKey/>}等标签对应的{@link XNode}对象）
     * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.({@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}等标签才有的"parameterType"属性，如果是{@code <selectKey/>}标签该参数则取自外层标签{@code <insert/>}或者{@code <update/>}的"parameterType"属性)
     * @return
     */
    @Override
    public SqlSource createSqlSource(Configuration configuration, XNode script, Class<?> parameterType) {
        SqlSource source = super.createSqlSource(configuration, script, parameterType);
        checkIsNotDynamic(source);
        return source;
    }

    /**
     * <ol>
     *     <li>
     *         调用父方法 {@link XMLLanguageDriver#createSqlSource(Configuration, String, Class)}传入{@code configuration}、{@code script}、{@code parameterType}三个参数，构建{@link SqlSource}对象
     *     </li>
     *     <li>
     *         将第一步返回的{@link SqlSource}对象传入{@link #checkIsNotDynamic(SqlSource)}进行校验
     *     </li>
     *     <li>
     *         return 第一步返回的{@link SqlSource}对象，本方法结束
     *     </li>
     * </ol>
     *
     * @param configuration The MyBatis configuration
     * @param script        XNode parsed from a XML file （{@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}、{@code <selectKey/>}等标签对应的{@link XNode}对象）
     * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.({@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}等标签才有的"parameterType"属性，如果是{@code <selectKey/>}标签该参数则取自外层标签{@code <insert/>}或者{@code <update/>}的"parameterType"属性)
     * @return
     */
    @Override
    public SqlSource createSqlSource(Configuration configuration, String script, Class<?> parameterType) {
        SqlSource source = super.createSqlSource(configuration, script, parameterType);
        checkIsNotDynamic(source);
        return source;
    }

    /**
     * 校验：如果!{@link RawSqlSource}.class.equals(传入的{@code source}.getClass())就直接抛出异常{@link BuilderException}；否则什么也不做，本方法结束
     *
     * @param source 创建的 SqlSource 对象
     */
    private void checkIsNotDynamic(SqlSource source) {
        if (!RawSqlSource.class.equals(source.getClass())) {
            throw new BuilderException("Dynamic content is not allowed when using RAW language");
        }
    }

}