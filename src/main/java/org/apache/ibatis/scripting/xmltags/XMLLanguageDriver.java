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

import org.apache.ibatis.builder.xml.XMLMapperEntityResolver;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.mapping.BoundSql;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.scripting.defaults.DefaultParameterHandler;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.xml.sax.EntityResolver;

import java.util.Properties;

/**
 * XML 语言驱动实现类
 *
 * @author Eduardo Macarron
 */
public class XMLLanguageDriver implements LanguageDriver {

    @Override
    public ParameterHandler createParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
        // 创建 DefaultParameterHandler 对象
        return new DefaultParameterHandler(mappedStatement, parameterObject, boundSql);
    }

    /**
     * 根据{@link XNode}对象{@code script}创建{@link SqlSource}对象
     * <ol>
     *     <li>
     *         调用构造器{@link XMLScriptBuilder#XMLScriptBuilder(Configuration, XNode, Class)}传入{@code configuration}、{@code script}、{@code parameterType}构建一个{@link XMLScriptBuilder}对象
     *     </li>
     *     <li>
     *         调用{@link XMLScriptBuilder#parseScriptNode()}得到一个{@link SqlSource}对象并返回，本方法结束
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
        XMLScriptBuilder builder = new XMLScriptBuilder(configuration, script, parameterType);
        return builder.parseScriptNode();
    }

    /**
     * <ul>
     *     判断：{@code script}.startWith("{@code <script>}")
     *     <li>
     *         true：
     *         <ol>
     *             <li>
     *                 调用构造器{@link XPathParser#XPathParser(String, boolean, Properties, EntityResolver)}传入{@code script}、false、{@link Configuration#getVariables()}、new 一个{@link XMLMapperEntityResolver}对象 共四个参数构建一个{@link XPathParser}对象
     *             </li>
     *             <li>
     *                 调用{@link XPathParser#evalNode(String)}传入"/script"解析得到对应的{@link XNode}对象，然后调用{@link #createSqlSource(Configuration, XNode, Class)}传入{@code configuration}、前面得到的{@link XNode}对象、{@code parameterTyp} 共三个参数重用xml的方法进行{@link SqlSource}对象构建并返回，本方法结束
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         false：
     *         <ol>
     *             <li>
     *                 调用{@link PropertyParser#parse(String, Properties)}传入{@code script}和{@link Configuration#getVariables()}两个参数，对{@code script}中含有的"${}"token进行变量替换
     *             </li>
     *             <li>
     *                 调用构造器{@link TextSqlNode#TextSqlNode(String)}传入变量替换后的{@code script}构建一个{@link TextSqlNode}对象，然后调用{@link TextSqlNode#isDynamic()}判断是否包含"#{}"：
     *                 <ul>
     *                     <li>
     *                         包含：调用构造器{@link DynamicSqlSource#DynamicSqlSource(Configuration, SqlNode)}传入{@code configuration}、前面构建的{@link TextSqlNode}构建一个{@link DynamicSqlSource}对象并返回，本方法结束
     *                     </li>
     *                     <li>
     *                         不包含：调用构造器{@link RawSqlSource#RawSqlSource(Configuration, String, Class)}传入{@code configuration}、{@code script}、{@code parameterType}构建一个{@link RawSqlSource}对象并返回，本方法结束
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ol>
     *     </li>
     * </ul>
     *
     * @param configuration The MyBatis configuration
     * @param script        The content of the annotation
     * @param parameterType input parameter type got from a mapper method or specified in the parameterType xml attribute. Can be null.
     * @return
     */
    @Override
    public SqlSource createSqlSource(Configuration configuration, String script, Class<?> parameterType) {
        // issue #3
        if (script.startsWith("<script>")) {
            XPathParser parser = new XPathParser(script, false, configuration.getVariables(), new XMLMapperEntityResolver());
            return createSqlSource(configuration, parser.evalNode("/script"), parameterType);
        } else {
            // issue #127
            script = PropertyParser.parse(script, configuration.getVariables());
            TextSqlNode textSqlNode = new TextSqlNode(script);
            if (textSqlNode.isDynamic()) {
                return new DynamicSqlSource(configuration, textSqlNode);
            } else {
                return new RawSqlSource(configuration, script, parameterType);
            }
        }
    }

}
