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
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Node;

import java.util.List;
import java.util.Locale;

/**
 * Statement XML 配置构建器，主要负责解析 Statement 配置，即 {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 标签
 *
 * @author Clinton Begin
 */
public class XMLStatementBuilder extends BaseBuilder {

    private final MapperBuilderAssistant builderAssistant;
    /**
     * 当前 XML 节点，例如：{@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 标签对应的{@link XNode}对象
     */
    private final XNode context;
    /**
     * 要求的 databaseId （{@link Configuration#databaseId}）
     */
    private final String requiredDatabaseId;

    public XMLStatementBuilder(Configuration configuration, MapperBuilderAssistant builderAssistant, XNode context) {
        this(configuration, builderAssistant, context, null);
    }

    /**
     * 调用父构造器{@link BaseBuilder#BaseBuilder(Configuration)}传入{@code configuration}构建一个父对象，{@code builderAssistant}赋值到{@link #builderAssistant}、{@code context}赋值到{@link #context}、{@code databaseId}赋值到{@link #requiredDatabaseId}
     *
     * @param configuration
     * @param builderAssistant
     * @param context
     * @param databaseId
     */
    public XMLStatementBuilder(Configuration configuration, MapperBuilderAssistant builderAssistant, XNode context, String databaseId) {
        super(configuration);
        this.builderAssistant = builderAssistant;
        this.context = context;
        this.requiredDatabaseId = databaseId;
    }

    /**
     * 执行解析 {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 标签：
     * <ol>
     *     <li>
     *         通过{@link XNode#getStringAttribute(String)}获取成员变量{@link #context}的属性"id"和"databaseId"的值
     *     </li>
     *     <li>
     *         调用{@link #databaseIdMatchesCurrent(String, String, String)}传入上一步获取的"id"、"databaseId"属性值、成员变量{@link #requiredDatabaseId} 三个参数判断databaseId是否匹配：不匹配直接return；匹配则继续往下走。
     *     </li>
     *     <li>
     *         <ol>
     *             <li>
     *                 通过{@link XNode#getIntAttribute(String)}获取"fetchSize"和"timeout"的属性值
     *             </li>
     *             <li>
     *                 通过{@link XNode#getStringAttribute(String)}获取"parameterMap"、"parameterType"、"resultMap"、"resultType"、"lang"、"resultSetType"属性的值
     *             </li>
     *             <li>
     *                 调用{@link XNode#getStringAttribute(String, String)}获取"statementType"属性值，默认值为{@link StatementType#PREPARED}.toString()
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <ol>
     *             <li>
     *                 调用{@link #getLanguageDriver(String)}传入第3步获得的"lang"属性值取得对应的{@link LanguageDriver}对象，如果没有配置返回的就是{@link org.apache.ibatis.scripting.xmltags.XMLLanguageDriver}对象
     *             </li>
     *             <li>
     *                 调用{@link #resolveClass(String)}分别解析"parameterType"和"resultType"属性值获取对应的{@link Class}对象
     *             </li>
     *             <li>
     *                 调用{@link #resolveResultSetType(String)}解析"resultSetType"属性值获得对应的{@link ResultSetType}对象
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         通过{@link XNode#getNode()}获取当前标签对应的{@link org.w3c.dom.Node}对象，然后通过{@link Node#getNodeName()}获取当前标签名称，通过调用该名称的{@link String#toUpperCase(Locale)}传入{@link Locale#ENGLISH}将
     *         名称转成大写然后传入{@link SqlCommandType#valueOf(String)}获取标签名称对应的{@link SqlCommandType}对象
     *     </li>
     *     <li>
     *         通过{@link XNode#getBooleanAttribute(String, Boolean)}分别获取"flushCache"、"useCache"、"resultOrdered"的属性值，如果获取不到设置的默认值分别为：第5步获取的{@link SqlCommandType}对象 != {@link SqlCommandType#SELECT}、第5步获取的{@link SqlCommandType}对象 == {@link SqlCommandType#SELECT}的结果、false
     *     </li>
     *     <li>
     *         调用构造器{@link XMLIncludeTransformer#XMLIncludeTransformer(Configuration, MapperBuilderAssistant)}传入{@link #configuration}和{@link #builderAssistant}构建一个{@code <include/>}标签解析器，然后调用{@link XMLIncludeTransformer#applyIncludes(Node)}传入{@link #context}的{@link XNode#getNode()}进行{@code <include/>}标签的解析
     *     </li>
     *     <li>
     *         调用{@link #processSelectKeyNodes(String, Class, LanguageDriver)}传入 第1步获得的"id"属性值、第3.2步获得的"parameterType"属性值、第4.1步获得的"lang"属性值，处理可能存在的{@code <selectKey/>}标签
     *     </li>
     *     <li>
     *         调用第4.1步获得的"lang"属性值对应的{@link LanguageDriver}的{@link LanguageDriver#createSqlSource(Configuration, XNode, Class)}（一般是{@link org.apache.ibatis.scripting.xmltags.XMLLanguageDriver#createSqlSource(Configuration, XNode, Class)}）传入{@link #configuration}、{@link #context}、第3.2步获得的"parameterType"属性值经过第4.2步转化成对应的{@link Class}对象 共三个参数创建一个{@link SqlSource}对象
     *     </li>
     *     <li>
     *         调用{@link #context}的{@link XNode#getStringAttribute(String)}分别传入"resultSets"、"keyProperty"、"keyColumn"获取对应的属性值
     *     </li>
     *     <li>
     *         给第1步获取到的"id"属性值拼接前缀{@link SelectKeyGenerator#SELECT_KEY_SUFFIX}成为"{@link SelectKeyGenerator}的id"，然后调用{@link #builderAssistant}的{@link MapperBuilderAssistant#applyCurrentNamespace(String, boolean)}传入拼接好的id和true，获取全限定id
     *     </li>
     *     <li>
     *         调用{@link #configuration}的{@link Configuration#hasKeyGenerator(String)}传入前一步拼接好后的"{@link SelectKeyGenerator}的全限定id"判断第8步动作是否有解析{@code <selectKey/>}标签成对应的{@link SelectKeyGenerator}对象并设置到了{@link Configuration}中：
     *         <ul>
     *             <li>
     *                 有：{@link #configuration}的{@link Configuration#getKeyGenerator(String)} 传入前一步拼接好后的"{@link SelectKeyGenerator}的全限定id" 获取第8步动作是否有解析{@code <selectKey/>}标签成对应的{@link SelectKeyGenerator}对象
     *             </li>
     *             <li>
     *                 没有：<br>
     *                 判断：调用{@link #context}的{@link XNode#getBooleanAttribute(String, Boolean)}传入<u><b>"useGeneratedKeys"和"{@link #configuration}的{@link Configuration#isUseGeneratedKeys()} && {@link SqlCommandType#INSERT}.equals(第5步获取的{@link SqlCommandType}对象)"</b></u> （先看当前标签有没有声明了要使用"Generate Key"的功能对应的属性；如果没有再看全局有没有声明且当前sql为insert）
     *                 <ul>
     *                     <li>
     *                         true：获取{@link Jdbc3KeyGenerator#INSTANCE}
     *                     </li>
     *                     <li>
     *                         false：获取{@link NoKeyGenerator#INSTANCE}
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link #builderAssistant}的{@link MapperBuilderAssistant#addMappedStatement(String, SqlSource, StatementType, SqlCommandType, Integer, Integer, String, Class, String, Class, ResultSetType, boolean, boolean, boolean, KeyGenerator, String, String, String, LanguageDriver, String)}传入
     *         <ol>
     *             <li>第1步获取的"id"属性值</li>
     *             <li>第9步创建的{@link SqlSource}对象</li>
     *             <li>第3.3步获取的"statementType"属性值</li>
     *             <li>第5步获取的{@link SqlCommandType}对象</li>
     *             <li>第3.1步获取的"fetchSize"属性值</li>
     *             <li>第3.1步获取的"timeout"属性值</li>
     *             <li>第3.2步获取的"parameterMap"属性值</li>
     *             <li>第3.2步获得的"parameterType"属性值经过第4.2步转化成对应的{@link Class}对象</li>
     *             <li>第3.2步获取的"resultMap"属性值</li>
     *             <li>第3.2步获得的"resultType"属性值经过第4.2步转化成对应的{@link Class}对象</li>
     *             <li>第3.2步获得的"resultSetType"属性值经过第4.3步转化成对应的{@link ResultSetType}对象</li>
     *             <li>第6步获取的"flushCache"属性值</li>
     *             <li>第6步获取的"useCache"属性值</li>
     *             <li>第6步获取的"resultOrdered"属性值</li>
     *             <li>第12步获取的{@link KeyGenerator}对象（{@link SelectKeyGenerator}或者{@link Jdbc3KeyGenerator}或者{@link NoKeyGenerator}）</li>
     *             <li>第10步获取的"keyProperty"属性值</li>
     *             <li>第10步获取的"keyColumn"属性值</li>
     *             <li>第1步获取的"databaseId"属性值</li>
     *             <li>第4.1步获取的{@link LanguageDriver}对象（一般是{@link org.apache.ibatis.scripting.xmltags.XMLLanguageDriver}）</li>
     *             <li>第1步获取的"resultSets"属性值</li>
     *         </ol>
     *         共20个参数构建一个{@link MappedStatement}对象并添加到{@link Configuration}中
     *     </li>
     * </ol>
     *
     */
    public void parseStatementNode() {
        String id = context.getStringAttribute("id");
        String databaseId = context.getStringAttribute("databaseId");
        if (!databaseIdMatchesCurrent(id, databaseId, this.requiredDatabaseId)) {
            return;
        }

        Integer fetchSize = context.getIntAttribute("fetchSize");
        Integer timeout = context.getIntAttribute("timeout");
        String parameterMap = context.getStringAttribute("parameterMap");
        String parameterType = context.getStringAttribute("parameterType");
        Class<?> parameterTypeClass = resolveClass(parameterType);
        String resultMap = context.getStringAttribute("resultMap");
        String resultType = context.getStringAttribute("resultType");
        String lang = context.getStringAttribute("lang");

        LanguageDriver langDriver = getLanguageDriver(lang);

        Class<?> resultTypeClass = resolveClass(resultType);
        String resultSetType = context.getStringAttribute("resultSetType");
        ResultSetType resultSetTypeEnum = resolveResultSetType(resultSetType);
        StatementType statementType = StatementType.valueOf(context.getStringAttribute("statementType", StatementType.PREPARED.toString()));

        String nodeName = context.getNode().getNodeName();
        SqlCommandType sqlCommandType = SqlCommandType.valueOf(nodeName.toUpperCase(Locale.ENGLISH));
        boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
        boolean flushCache = context.getBooleanAttribute("flushCache", !isSelect);
        boolean useCache = context.getBooleanAttribute("useCache", isSelect);
        boolean resultOrdered = context.getBooleanAttribute("resultOrdered", false);

        // Include Fragments before parsing
        XMLIncludeTransformer includeParser = new XMLIncludeTransformer(configuration, builderAssistant);
        includeParser.applyIncludes(context.getNode());

        // Parse selectKey after includes and remove them.
        processSelectKeyNodes(id, parameterTypeClass, langDriver);

        // Parse the SQL (pre: <selectKey> and <include> were parsed and removed)
        SqlSource sqlSource = langDriver.createSqlSource(configuration, context, parameterTypeClass);
        String resultSets = context.getStringAttribute("resultSets");
        String keyProperty = context.getStringAttribute("keyProperty");
        String keyColumn = context.getStringAttribute("keyColumn");
        KeyGenerator keyGenerator;
        String keyStatementId = id + SelectKeyGenerator.SELECT_KEY_SUFFIX;
        keyStatementId = builderAssistant.applyCurrentNamespace(keyStatementId, true);
        if (configuration.hasKeyGenerator(keyStatementId)) {
            keyGenerator = configuration.getKeyGenerator(keyStatementId);
        } else {
            keyGenerator =
                    context.getBooleanAttribute("useGeneratedKeys", configuration.isUseGeneratedKeys() && SqlCommandType.INSERT.equals(sqlCommandType))
                    ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
        }

        builderAssistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
                fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass,
                resultSetTypeEnum, flushCache, useCache, resultOrdered,
                keyGenerator, keyProperty, keyColumn, databaseId, langDriver, resultSets);
    }

    /**
     * 处理当前节点{@link #context}（{@code <insert />}、{@code <update />}、{@code <select />}、{@code <delete />} 标签）可能存在的{@code <selectKey />}子节点们（只有前两者才有），解析其内容并将其内容提取出来替换其在父节点中的位置：
     * <ol>
     *     <li>
     *         调用成员变量{@link #context}的{@link XNode#evalNodes(String)}传入"selectKey"获取当前标签下的所有{@code <selectKey/>}标签列表 List<{@link XNode}> 对象
     *     </li>
     *     <li>
     *         如果成员变量{@link #configuration}的{@link Configuration#getDatabaseId()}不为null
     *         <ul>
     *             <li>
     *                 是则：调用{@link #parseSelectKeyNodes(String, List, Class, LanguageDriver, String)}继续进行解析，传入{@code id}、上面获得的 List<{@link XNode}> 对象、{@code parameterTypeClass}、{@code langDriver}、{@link Configuration#getDatabaseId()}
     *             </li>
     *             <li>
     *                 否则：调用{@link #parseSelectKeyNodes(String, List, Class, LanguageDriver, String)}继续进行解析，传入{@code id}、上面获得的 List<{@link XNode}> 对象、{@code parameterTypeClass}、{@code langDriver}、null
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link #removeSelectKeyNodes(List)}传入上面获得的 List<{@link XNode}> 对象移出当前节点{@link #context}下所有 {@code <selectKey/>} 标签对象
     *     </li>
     * </ol>
     *
     * @param id {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 标签的"id"属性
     * @param parameterTypeClass {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 标签的"parameterType"属性
     * @param langDriver {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 标签的"lang"属性
     */
    private void processSelectKeyNodes(String id, Class<?> parameterTypeClass, LanguageDriver langDriver) {
        List<XNode> selectKeyNodes = context.evalNodes("selectKey");
        if (configuration.getDatabaseId() != null) {
            parseSelectKeyNodes(id, selectKeyNodes, parameterTypeClass, langDriver, configuration.getDatabaseId());
        }
        parseSelectKeyNodes(id, selectKeyNodes, parameterTypeClass, langDriver, null);
        removeSelectKeyNodes(selectKeyNodes);
    }

    /**
     * 解析同一节点下的所有{@code <selectKey/>}标签（本方法主要作用是判断{@code <selectKey/>}中的"databaseId"是否和全局一致）：
     * <ol>
     *     遍历{@code list}，得到每一个迭代的{@link XNode}对象：
     *     <li>
     *         拼接{@code parentId}和{@link SelectKeyGenerator#SELECT_KEY_SUFFIX}得到一个id（这里对于每一个迭代的对象得到的应该都是同一个值，感觉可以提到遍历外面）
     *     </li>
     *     <li>
     *         调用{@link XNode#getStringAttribute(String)}传入"databaseId"获取对应的属性值
     *     </li>
     *     <li>
     *         调用{@link #databaseIdMatchesCurrent(String, String, String)}传入第一步拼接好的id、第二步获得"databaseId"属性值、{@code skRequiredDatabaseId}，判断当前databaseId是否与全局的一致：
     *         <ul>
     *             <li>
     *                 是则：调用{@link #parseSelectKeyNode(String, XNode, Class, LanguageDriver, String)}传入第一步拼接好的id、当前迭代的{@link XNode}对象、{@code parameterTypeClass}、{@code langDriver}、第二步获得"databaseId"属性值开始真正的解析每一个{@code <selectKey/>}标签
     *             </li>
     *             <li>
     *                 否则：什么也不做，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param parentId 当前{@code <selectKey/>}标签们的父节点"id"属性（可能是{@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />}，调用本方法的方法没有做筛选，所有动作都会调用本方法，所以这四个标签都会调用本方法）
     * @param list {@code <selectKey/>}标签们， List<{@link XNode}> 对象，可能是空列表
     * @param parameterTypeClass 当前{@code <selectKey/>}标签们的父节点"parameterType"属性（可能是{@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />}，调用本方法的方法没有做筛选，所有动作都会调用本方法，所以这四个标签都会调用本方法）
     * @param langDriver 当前{@code <selectKey/>}标签们的父节点"lang"属性（{可能是@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />}，调用本方法的方法没有做筛选，所有动作都会调用本方法，所以这四个标签都会调用本方法）
     * @param skRequiredDatabaseId {@link Configuration#databaseId}
     */
    private void parseSelectKeyNodes(String parentId, List<XNode> list, Class<?> parameterTypeClass, LanguageDriver langDriver, String skRequiredDatabaseId) {
        for (XNode nodeToHandle : list) {
            String id = parentId + SelectKeyGenerator.SELECT_KEY_SUFFIX;
            String databaseId = nodeToHandle.getStringAttribute("databaseId");
            if (databaseIdMatchesCurrent(id, databaseId, skRequiredDatabaseId)) {
                parseSelectKeyNode(id, nodeToHandle, parameterTypeClass, langDriver, databaseId);
            }
        }
    }

    /**
     * 解析{@code <selectKey/>}标签为一个包含了{@link MappedStatement}对象的{@link SelectKeyGenerator}对象，并设置到{@link Configuration}中：
     * <ol>
     *     <li>
     *         获取{@code <selectKey/>}标签的各种属性：
     *         <ol>
     *             <li>
     *                 调用{@link XNode#getStringAttribute(String)}传入"resultType"获取对应的属性值，调用{@link #resolveClass(String)}传入属性值获取对应的{@link Class}对象
     *             </li>
     *             <li>
     *                 同样调用{@link XNode#getStringAttribute(String, String)}传入"statementType"和{@link StatementType#PREPARED}.toString() 获取对应的属性值，如果获取不到就是返回"PREPARED"，然后调用{@link StatementType#valueOf(String)}传入前面获取到的字符串得到对应的{@link StatementType}枚举对象
     *             </li>
     *             <li>
     *                 同样调用{@link XNode#getStringAttribute(String)}传入"keyProperty"获取对应的属性值
     *             </li>
     *             <li>
     *                 同样调用{@link XNode#getStringAttribute(String)}传入"keyColumn"获取对应的属性值
     *             </li>
     *             <li>
     *                 同样调用{@link XNode#getStringAttribute(String, String)}传入"order"和"AFTER" 获取对应的属性值，如果获取不到就返回"AFTER"，然后判断返回的字符串是否 equals "BEFORE"
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         调用各种默认值：
     *         <pre>
     *         boolean useCache = false;
     *         boolean resultOrdered = false;
     *         KeyGenerator keyGenerator = {@link NoKeyGenerator#INSTANCE};
     *         Integer fetchSize = null;
     *         Integer timeout = null;
     *         boolean flushCache = false;
     *         String parameterMap = null;
     *         String resultMap = null;
     *         ResultSetType resultSetTypeEnum = null;
     *         SqlCommandType sqlCommandType = {@link SqlCommandType#SELECT};
     *         </pre>
     *     </li>
     *     <li>
     *         调用{@code langDriver}的{@link LanguageDriver#createSqlSource(Configuration, XNode, Class)}传入{@link #configuration}、{@code nodeToHandle}、{@code parameterTypeClass}创建{@link SqlSource}对象（如果在当前{@code <selectKey/>}标签的外层{@code <insert/>}或者{@code <update/>}标签中没有配置"lang"属性这里的{@link LanguageDriver}实现就是{@link org.apache.ibatis.scripting.xmltags.XMLLanguageDriver}，参考{@link org.apache.ibatis.scripting.xmltags.XMLLanguageDriver#createSqlSource(Configuration, XNode, Class)}）
     *     </li>
     *     <li>
     *         调用{@link #builderAssistant}的{@link MapperBuilderAssistant#addMappedStatement(String, SqlSource, StatementType, SqlCommandType, Integer, Integer, String, Class, String, Class, ResultSetType, boolean, boolean, boolean, KeyGenerator, String, String, String, LanguageDriver, String)}
     *         传入
     *         <ol>
     *             <li>{@code id}</li>
     *             <li>第3步获取的{@link SqlSource}对象</li>
     *             <li>第一步获取的"statementType"属性值对应的{@link StatementType}对象</li>
     *             <li>第二步设置的默认值{@link SqlCommandType#SELECT}</li>
     *             <li>第二步设置的"fetchSize"默认值null</li>
     *             <li>第二步设置的"timeout"默认值null</li>
     *             <li>第二步设置的"parameterMap"默认值null</li>
     *             <li>{@code parameterTypeClass}</li>
     *             <li>第二步设置的"resultMap"默认值null</li>
     *             <li>第一步获取的"resultType"属性值对应的{@link Class}对象</li>
     *             <li>第二步设置的"resultSetTypeEnum"默认值null</li>
     *             <li>第二步设置的"flushCache"默认值false</li>
     *             <li>第二步设置的"useCache"默认值false</li>
     *             <li>第二步设置的"resultOrdered"默认值false</li>
     *             <li>第二步设置的"keyGenerator"默认值{@link NoKeyGenerator#INSTANCE}</li>
     *             <li>第一步获取的"keyProperty"属性值</li>
     *             <li>第一步获取的"keyColumn"属性值</li>
     *             <li>{@code databaseId}</li>
     *             <li>{@code langDriver}</li>
     *             <li>null</li>
     *         </ol>
     *         共20个参数构建一个{@link MappedStatement}对象并添加到{@link Configuration}中然后返回
     *     </li>
     *     <li>
     *         调用{@link #builderAssistant}的{@link MapperBuilderAssistant#applyCurrentNamespace(String, boolean)}传入{@code id}和false进行全限定id校验
     *     </li>
     *     <li>
     *         调用{@link #configuration}的{@link Configuration#getMappedStatement(String, boolean)}传入前一步校验过后的全限定id和false获取刚构建的{@link MappedStatement}对象
     *     </li>
     *     <li>
     *         调用构造器{@link SelectKeyGenerator#SelectKeyGenerator(MappedStatement, boolean)}传入前一步获取的{@link MappedStatement}对象和1.5步获得的boolean值（execute before or after），new 一个{@link SelectKeyGenerator}对象
     *     </li>
     *     <li>
     *         调用{@link #configuration}的{@link Configuration#addKeyGenerator(String, KeyGenerator)}传入经过第5步校验的{@code id}和前一步构建好的{@link SelectKeyGenerator}对象，put到{@link Configuration}中
     *     </li>
     * </ol>
     *
     * @param id 当前{@code <selectKey/>}标签的父节点"id"属性 拼接上 {@link SelectKeyGenerator#SELECT_KEY_SUFFIX}（只会是{@code <insert />}、{@code <update />}的其中一个）
     * @param nodeToHandle 当前{@code <selectKey/>}标签对应的{@link XNode}对象
     * @param parameterTypeClass 当前{@code <selectKey/>}标签的父节点"parameterType"属性（只会是{@code <insert />}、{@code <update />}的其中一个）
     * @param langDriver 当前{@code <selectKey/>}标签的父节点"lang"属性（只会是{@code <insert />}、{@code <update />}的其中一个）
     * @param databaseId 当前{@code <selectKey/>}标签的"databaseId"属性值
     */
    private void parseSelectKeyNode(String id, XNode nodeToHandle, Class<?> parameterTypeClass, LanguageDriver langDriver, String databaseId) {
        String resultType = nodeToHandle.getStringAttribute("resultType");
        Class<?> resultTypeClass = resolveClass(resultType);
        StatementType statementType = StatementType.valueOf(nodeToHandle.getStringAttribute("statementType", StatementType.PREPARED.toString()));
        String keyProperty = nodeToHandle.getStringAttribute("keyProperty");
        String keyColumn = nodeToHandle.getStringAttribute("keyColumn");
        boolean executeBefore = "BEFORE".equals(nodeToHandle.getStringAttribute("order", "AFTER"));

        // defaults
        boolean useCache = false;
        boolean resultOrdered = false;
        KeyGenerator keyGenerator = NoKeyGenerator.INSTANCE;
        Integer fetchSize = null;
        Integer timeout = null;
        boolean flushCache = false;
        String parameterMap = null;
        String resultMap = null;
        ResultSetType resultSetTypeEnum = null;

        SqlSource sqlSource = langDriver.createSqlSource(configuration, nodeToHandle, parameterTypeClass);
        SqlCommandType sqlCommandType = SqlCommandType.SELECT;

        builderAssistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType,
                fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass,
                resultSetTypeEnum, flushCache, useCache, resultOrdered,
                keyGenerator, keyProperty, keyColumn, databaseId, langDriver, null);

        // 获得 SelectKeyGenerator 的编号，格式为 `${namespace}.${id}`
        id = builderAssistant.applyCurrentNamespace(id, false);
        MappedStatement keyStatement = configuration.getMappedStatement(id, false);
        configuration.addKeyGenerator(id, new SelectKeyGenerator(keyStatement, executeBefore));
    }

    /**
     * 将{@code selectKeyNodes}中的每一个节点从其父节点中移除：<br>
     *
     * 遍历{@code selectKeyNodes}，对于每一个迭代的{@link XNode}对象：调用{@link XNode#getParent()}获取其父{@link XNode}然后调用{@link XNode#getNode()}获取对应的父节点的{@link Node}对象
     * 然后调用{@link Node#removeChild(Node)}传入当前{@link XNode}对象的{@link XNode#getNode()}从当前节点的父节点中移除当前节点
     * @param selectKeyNodes {@code <selectKey/>}标签对应的{@link XNode}对象列表
     */
    private void removeSelectKeyNodes(List<XNode> selectKeyNodes) {
        for (XNode nodeToHandle : selectKeyNodes) {
            nodeToHandle.getParent().getNode().removeChild(nodeToHandle.getNode());
        }
    }

    /**
     * 判断当前标签中配置的databaseId是否和configure中全局配置的databaseId一致：
     * <ol>
     *     <li>
     *         <ul>
     *             判断{@code requiredDatabaseId}是否为null
     *             <li>
     *                 不为null则：直接返回{@code requiredDatabaseId}.equals({@code databaseId})
     *             </li>
     *             <li>
     *                 为null则：
     *                 <ol>
     *                     <li>
     *                         判断{@code databaseId}是否不为null：是则直接返回false；否则继续往下走
     *                     </li>
     *                     <li>
     *                         调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#applyCurrentNamespace(String, boolean)}传入{@code id}和false，对id进行校验
     *                     </li>
     *                     <li>
     *                         调用成员变量{@link #configuration}的{@link Configuration#hasStatement(String, boolean)}传入{@code id}和false判断该id指向的{@link MappedStatement}对象已经存在
     *                         <ul>
     *                             <li>
     *                                 是则：调用成员变量{@link #configuration}的{@link Configuration#getMappedStatement(String, boolean)} 传入{@code id}和false 获取已经存在的{@link MappedStatement}对象，通过{@link MappedStatement#getDatabaseId()}获取其{@link MappedStatement#databaseId}：如果是null则返回true；否则返回false（保证如果有已存在的{@link MappedStatement}的databaseId和全局databaseId一致）
     *                             </li>
     *                             <li>
     *                                 否则：什么也不做，继续往下走
     *                             </li>
     *                         </ul>
     *                     </li>
     *                 </ol>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         来到这里直接返回true
     *     </li>
     * </ol>
     *
     * @param id 当前标签的id
     * @param databaseId 当前标签中配置的databaseId
     * @param requiredDatabaseId configure中配置的全局databaseId
     * @return
     */
    private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
        if (requiredDatabaseId != null) {
            return requiredDatabaseId.equals(databaseId);
        } else {
            if (databaseId != null) {
                return false;
            }
            // skip this statement if there is a previous one with a not null databaseId
            id = builderAssistant.applyCurrentNamespace(id, false);
            if (this.configuration.hasStatement(id, false)) {
                MappedStatement previous = this.configuration.getMappedStatement(id, false); // issue #2
                return previous.getDatabaseId() == null;
            }
        }
        return true;
    }

    /**
     * 获得对应的 LanguageDriver 对象：
     * <ul>
     *     判断{@code lang}是否为null：
     *     <li>
     *         为null则直接调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#getLanguageDriver(Class)}传入null并return其结果
     *     </li>
     *     <li>
     *         不为null则调用{@link #resolveClass(String)}传入{@code lang}解析得到对应的{@link Class}对象，然后调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#getLanguageDriver(Class)}传入前面解析得到的{@link Class}对象并return其结果
     *     </li>
     * </ul>
     *
     * @param lang "lang"属性值
     * @return LanguageDriver 对象
     */
    private LanguageDriver getLanguageDriver(String lang) {
        Class<? extends LanguageDriver> langClass = null;
        if (lang != null) {
            langClass = resolveClass(lang);
        }
        return builderAssistant.getLanguageDriver(langClass);
    }

}
