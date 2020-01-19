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

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.mapping.SqlSource;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.scripting.defaults.RawSqlSource;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * XML 动态语句( SQL )构建器，负责将 SQL 解析成 SqlSource 对象
 *
 * @author Clinton Begin
 */
public class XMLScriptBuilder extends BaseBuilder {

    /**
     * 当前 SQL 的 {@link XNode} 对象（可以是{@code <select/>}、{@code <update/>}、{@code <insert/>}、{@code <delete/>}、{@code <selectKey/>}）
     */
    private final XNode context;
    /**
     * 是否为动态 SQL（如果当前{@link #context}含有其他xml标签节点或者文本节点中含有"${}"token，当前属性为true）
     */
    private boolean isDynamic;
    /**
     * SQL 参数类型
     */
    private final Class<?> parameterType;
    /**
     * NodeNodeHandler 的映射（参考{@link #initNodeHandlerMap()}）
     */
    private final Map<String, NodeHandler> nodeHandlerMap = new HashMap<>();

    public XMLScriptBuilder(Configuration configuration, XNode context) {
        this(configuration, context, null);
    }

    /**
     * 给{@link BaseBuilder#configuration}、{@link #context}、{@link #parameterType}赋值，然后调用{@link #initNodeHandlerMap()}初始化各个sql片段标签名称和对应的处理器（{@link NodeHandler}）对象到{@link #nodeHandlerMap}
     *
     * @param configuration
     * @param context （{@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}、{@code <selectKey/>}等标签对应的{@link XNode}对象）
     * @param parameterType ({@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}等标签才有的"parameterType"属性，如果是{@code <selectKey/>}标签该参数则取自外层标签{@code <insert/>}或者{@code <update/>}的"parameterType"属性)
     */
    public XMLScriptBuilder(Configuration configuration, XNode context, Class<?> parameterType) {
        super(configuration);
        this.context = context;
        this.parameterType = parameterType;
        initNodeHandlerMap();
    }

    /**
     * 初始化 {@link #nodeHandlerMap} 属性
     * <pre>
     *         nodeHandlerMap.put("trim", new {@link TrimHandler}());
     *         nodeHandlerMap.put("where", new {@link WhereHandler}());
     *         nodeHandlerMap.put("set", new {@link SetHandler}());
     *         nodeHandlerMap.put("foreach", new {@link ForEachHandler}());
     *         nodeHandlerMap.put("if", new {@link IfHandler}());
     *         nodeHandlerMap.put("choose", new {@link ChooseHandler}());
     *         nodeHandlerMap.put("when", new {@link IfHandler}());
     *         nodeHandlerMap.put("otherwise", new {@link OtherwiseHandler}());
     *         nodeHandlerMap.put("bind", new {@link BindHandler}());
     * </pre>
     */
    private void initNodeHandlerMap() {
        nodeHandlerMap.put("trim", new TrimHandler());
        nodeHandlerMap.put("where", new WhereHandler());
        nodeHandlerMap.put("set", new SetHandler());
        nodeHandlerMap.put("foreach", new ForEachHandler());
        nodeHandlerMap.put("if", new IfHandler());
        nodeHandlerMap.put("choose", new ChooseHandler());
        nodeHandlerMap.put("when", new IfHandler());
        nodeHandlerMap.put("otherwise", new OtherwiseHandler());
        nodeHandlerMap.put("bind", new BindHandler());
    }

    /**
     * 负责将 SQL 的{@link XNode}对象 解析成 {@link SqlSource} 对象：
     * <ol>
     *     <li>
     *         调用{@link #parseDynamicTags(XNode)}传入{@link #context}解析{SQL}的{@link XNode}对象为一个{@link MixedSqlNode}对象
     *     </li>
     *     <li>
     *         判断{@link #isDynamic}是否为true：TODO：
     *         <ul>
     *             <li>
     *                 是：调用构造器{@link DynamicSqlSource#DynamicSqlSource(Configuration, SqlNode)}传入{@link #configuration}和第一步得到的{@link MixedSqlNode}对象构造一个{@link DynamicSqlSource}对象并返回
     *             </li>
     *             <li>
     *                 否：调用构造器{@link RawSqlSource#RawSqlSource(Configuration, SqlNode, Class)}传入{@link #configuration}、第一步得到的{@link MixedSqlNode}对象、{@link #parameterType}三个参数构造一个{@link RawSqlSource}对象并返回
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         本方法结束
     *     </li>
     * </ol>
     *
     * @return SqlSource 对象
     */
    public SqlSource parseScriptNode() {
        MixedSqlNode rootSqlNode = parseDynamicTags(context);
        SqlSource sqlSource;
        if (isDynamic) {
            sqlSource = new DynamicSqlSource(configuration, rootSqlNode);
        } else {
            sqlSource = new RawSqlSource(configuration, rootSqlNode, parameterType);
        }
        return sqlSource;
    }

    /**
     * 解析当前{@link XNode}节点 成 {@link MixedSqlNode} 对象（该对象包含了本节点的其他节点：1、对于文本节点和{@code <bind/>}标签节点，要么是{@link TextSqlNode}对象、要么是{@link StaticTextSqlNode}对象和{@link VarDeclSqlNode}对象；2、对于其他标签节点，则会在对应的标签节点处理器中的{@link NodeHandler#handleNode(XNode, List)}方法对本方法 {@link #parseDynamicTags(XNode)} 进行调用，将子标签节点也转成一个{@link MixedSqlNode}对象，这是个间接递归调用）：
     * <ol>
     *     <li>
     *         new 一个空的泛型为{@link SqlNode}的{@link ArrayList}列表对象
     *     </li>
     *     <li>
     *         调用{@code node}的{@link XNode#getNode()}再调用{@link Node#getChildNodes()}获得{@code node}的所有子节点列表对象{@link NodeList}
     *     </li>
     *     <li>
     *         遍历第二步获得的{@link NodeList}对象（通过{@link NodeList#getLength()}获取列表对象的长度和{@link NodeList#item(int)}传入索引值获取指定位置的元素来实现遍历），对于每一个遍历迭代的当前子节点对象{@link Node}：
     *         <ul>
     *             先调用{@code node}的{@link XNode#newXNode(Node)}方法传入当前子节点对象构建其{@link XNode}对象，然后判断：子节点{@link Node#getNodeType()} == {@link Node#CDATA_SECTION_NODE} || 子节点{@link Node#getNodeType()} == {@link Node#TEXT_NODE} （当前子节点是一个文本节点对象）
     *             <li>
     *                 是则：<br>
     *                 <ol>
     *                     <li>
     *                         调用当前子节点{@link XNode}对象的{@link XNode#getStringBody(String)}传入空字符串""获取文本内容，如果没有文本内容则返回默认值空字符串
     *                     </li>
     *                     <li>
     *                         调用{@link TextSqlNode#TextSqlNode(String)}传入上面获得的文本内容构建一个{@link TextSqlNode}对象
     *                     </li>
     *                     <li>
     *                         调用构建好的{@link TextSqlNode#isDynamic()}判断当前文本节点是否为动态的：
     *                         <ul>
     *                             <li>
     *                                 是则：添加当前{@link TextSqlNode}对象到第一步new的一个{@link ArrayList}对象中，并设置{@link #isDynamic}为true
     *                             </li>
     *                             <li>
     *                                 否则：调用{@link StaticTextSqlNode#StaticTextSqlNode(String)}传入前面获得的文本构建一个{@link StaticTextSqlNode}对象添加到第一步new的一个{@link ArrayList}对象中
     *                             </li>
     *                         </ul>
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 否则：判断 子节点{@link Node#getNodeType()} == {@link Node#ELEMENT_NODE}（当前节点是一个标签）
     *                 <ul>
     *                     <li>
     *                         是则：
     *                         <ol>
     *                             <li>
     *                                 调用子节点{@link Node#getNodeName()}获取标签名称，然后调用{@link #nodeHandlerMap}的{@link HashMap#get(Object)}传入获得标签名称获取对应的{@link NodeHandler}实现类。
     *                             </li>
     *                             <li>
     *                                 如果获得的{@link NodeHandler}对象是null：抛出异常{@link BuilderException}；否则什么也不做，继续往下走
     *                             </li>
     *                             <li>
     *                                 调用{@link NodeHandler#handleNode(XNode, List)}传入当前子节点对应的{@link XNode}对象和第一步new的一个{@link ArrayList}对象（进行标签处理，得到标签对应的一些{@link SqlNode}对象，添加到集合中）
     *                             </li>
     *                             <li>
     *                                 设置{@link #isDynamic}为true
     *                             </li>
     *                         </ol>
     *                     </li>
     *                     <li>
     *                         否则：什么也不做，继续往下走
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link MixedSqlNode#MixedSqlNode(List)}传入第一步new的{@link ArrayList}对象，构建一个{@link MixedSqlNode}对象返回，本方法结束
     *     </li>
     * </ol>
     *
     * @param node XNode 节点 （{@code <select/>}、{@code <update/>}、{@code <delete/>}、{@code <select/>}、{@code <selectKey/>}等标签对应的{@link XNode}对象）
     * @return MixedSqlNode
     */
    protected MixedSqlNode parseDynamicTags(XNode node) {
        List<SqlNode> contents = new ArrayList<>();
        NodeList children = node.getNode().getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            XNode child = node.newXNode(children.item(i));
            if (child.getNode().getNodeType() == Node.CDATA_SECTION_NODE || child.getNode().getNodeType() == Node.TEXT_NODE) {
                String data = child.getStringBody("");
                TextSqlNode textSqlNode = new TextSqlNode(data);
                if (textSqlNode.isDynamic()) {
                    contents.add(textSqlNode);
                    isDynamic = true;
                } else {
                    contents.add(new StaticTextSqlNode(data));
                }
            } else if (child.getNode().getNodeType() == Node.ELEMENT_NODE) { // issue #628
                String nodeName = child.getNode().getNodeName();
                NodeHandler handler = nodeHandlerMap.get(nodeName);
                if (handler == null) {
                    throw new BuilderException("Unknown element <" + nodeName + "> in SQL statement.");
                }
                handler.handleNode(child, contents);
                isDynamic = true;
            }
        }
        return new MixedSqlNode(contents);
    }

    /**
     * Node 处理器接口
     */
    private interface NodeHandler {

        /**
         * 处理标签对应的{@link XNode}对象，得到标签对应的{@link SqlNode}实现类对象，添加到{@code targetContents}中
         *
         * @param nodeToHandle 要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        void handleNode(XNode nodeToHandle, List<SqlNode> targetContents);

    }

    /**
     * {@code <bind />}标签的处理器（可以在{@code <select/>}、{@code <insert/>}、{@code <selectKey/>}、{@code <update/>}、{@code <delete/>}、{@code <sql/>}、{@code <trim/>}、{@code <where/>}、{@code <set/>}、{@code <foreach/>}、{@code <when/>}、{@code <otherwise/>}、{@code <if/>}标签中定义{@code <bind/>}标签）<br>
     * 该标签用于将"value"属性值作为一个OGNL表达式到{@link DynamicContext#bindings}中获取对应的对象然后将该对象作为value，该标签"name"属性值作为key，put到{@link DynamicContext#bindings}中【即给某些可能的参数设置一个快捷访问名称或者别名，不只只通过OGNL表达式访问】
     *
     * @see VarDeclSqlNode
     */
    private class BindHandler implements NodeHandler {

        public BindHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         分别调用{@code nodeToHandle}的{@link XNode#getStringAttribute(String)}传入"name"和"value"获得对应的属性值
         *     </li>
         *     <li>
         *         调用{@link VarDeclSqlNode#VarDeclSqlNode(String, String)}传入上面获得两个属性值构建一个{@link VarDeclSqlNode}对象
         *     </li>
         *     <li>
         *         将{@link VarDeclSqlNode}对象添加到{@code targetContents}中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle
         * @param targetContents
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            final String name = nodeToHandle.getStringAttribute("name");
            final String expression = nodeToHandle.getStringAttribute("value");
            final VarDeclSqlNode node = new VarDeclSqlNode(name, expression);
            targetContents.add(node);
        }
    }

    /**
     * {@code <trim />} 标签的处理器。（可以在{@code <select/>}、{@code <insert/>}、{@code <selectKey/>}、{@code <update/>}、{@code <delete/>}、{@code <sql/>}、{@code <trim/>}、{@code <where/>}、{@code <set/>}、{@code <foreach/>}、{@code <when/>}、{@code <otherwise/>}、{@code <if/>}标签中定义{@code <trim />}标签）<br>
     * 【{@code <trim/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）】<br>
     * 该标签用于检测标签内部的sql片段是否包含某些前缀或者某些后缀，如果包含则替换成某个前缀或者某个后缀
     *
     * @see TrimSqlNode
     */
    private class TrimHandler implements NodeHandler {

        public TrimHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #parseDynamicTags(XNode)}方法传入{@code <trim/>}标签对应的{@link XNode}对象{@code nodToHandle}，将其解析成一个{@link MixedSqlNode}对象【{@code <trim/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）】
         *     </li>
         *     <li>
         *         调用{@code nodeToHandle}的{@link XNode#getStringAttribute(String)}方法分别传入"prefix"、"prefixOverrides"、"suffix"、"suffixOverrides"获取对应的属性值
         *     </li>
         *     <li>
         *         调用构造器{@link TrimSqlNode#TrimSqlNode(Configuration, SqlNode, String, String, String, String)}传入{@link Configuration}对象、第一步获取的{@link MixedSqlNode}对象、第二步获取的4个属性值实例化一个{@link TrimSqlNode}对象
         *     </li>
         *     <li>
         *         将第三步实例化的{@link TrimSqlNode}对象添加到{@code targetContents}列表中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            String prefix = nodeToHandle.getStringAttribute("prefix");
            String prefixOverrides = nodeToHandle.getStringAttribute("prefixOverrides");
            String suffix = nodeToHandle.getStringAttribute("suffix");
            String suffixOverrides = nodeToHandle.getStringAttribute("suffixOverrides");
            TrimSqlNode trim = new TrimSqlNode(configuration, mixedSqlNode, prefix, prefixOverrides, suffix, suffixOverrides);
            targetContents.add(trim);
        }

    }

    /**
     * {@code <where />}标签的处理器<br>
     * 该标签用于检测将标签内的sql片段是否以"AND"或者"OR"开头，如果是替换为"WHERE"（实际上底层就是重用了{@link TrimSqlNode}的功能）
     *
     * @see WhereSqlNode
     */
    private class WhereHandler implements NodeHandler {

        public WhereHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #parseDynamicTags(XNode)}传入{@code nodeToHandle}解析{@code <where/>}标签节点及其内部节点为一个{@link MixedSqlNode}对象
         *     </li>
         *     <li>
         *         调用{@link WhereSqlNode#WhereSqlNode(Configuration, SqlNode)}传入{@link Configuration}对象和第一步获得的{@link MixedSqlNode}对象实例化一个{@link WhereSqlNode}对象
         *     </li>
         *     <li>
         *         最后将{@link WhereSqlNode}对象添加到{@code targetContents}中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            WhereSqlNode where = new WhereSqlNode(configuration, mixedSqlNode);
            targetContents.add(where);
        }

    }

    /**
     * {@code <set />} 标签的处理器<br>
     * 该标签用于检测给该标签内的sql片段直接添加前缀"SET"，然后检查是否以半角逗号","结尾，如果是就删除","（实际上底层就是重用了{@link TrimSqlNode}的功能）
     *
     * @see SetSqlNode
     */
    private class SetHandler implements NodeHandler {

        public SetHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #parseDynamicTags(XNode)}传入{@code nodeToHandle}解析{@code <set/>}标签节点及其内部节点为一个{@link MixedSqlNode}对象
         *     </li>
         *     <li>
         *         调用{@link WhereSqlNode#WhereSqlNode(Configuration, SqlNode)}传入{@link Configuration}对象和第一步获得的{@link MixedSqlNode}对象实例化一个{@link SetSqlNode}对象
         *     </li>
         *     <li>
         *         最后将{@link SetSqlNode}对象添加到{@code targetContents}中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            SetSqlNode set = new SetSqlNode(configuration, mixedSqlNode);
            targetContents.add(set);
        }

    }

    /**
     * {@code <foreach />} 标签的处理器<br>
     *
     * @see ForEachSqlNode
     */
    private class ForEachHandler implements NodeHandler {

        public ForEachHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #parseDynamicTags(XNode)}传入{@code nodeToHandle}解析{@code <foreach/>}标签节点及其内部节点为一个{@link MixedSqlNode}对象【{@code <foreach/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）】
         *     </li>
         *     <li>
         *         调用{@code nodeToHandle}的{@link XNode#getStringAttribute(String)}分别传入"collection"、"item"、"index"、"open"、"close"、"separator"获取相应的属性值
         *     </li>
         *     <li>
         *         调用{@link ForEachSqlNode#ForEachSqlNode(Configuration, SqlNode, String, String, String, String, String, String)} 传入 {@link #configuration}、第一步获得的{@link MixedSqlNode}对象、第二步获得所有属性值 构建一个{@link ForEachSqlNode}对象
         *     </li>
         *     <li>
         *         将{@link ForEachSqlNode}对象添加到{@code targetContents}中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            String collection = nodeToHandle.getStringAttribute("collection");
            String item = nodeToHandle.getStringAttribute("item");
            String index = nodeToHandle.getStringAttribute("index");
            String open = nodeToHandle.getStringAttribute("open");
            String close = nodeToHandle.getStringAttribute("close");
            String separator = nodeToHandle.getStringAttribute("separator");
            ForEachSqlNode forEachSqlNode = new ForEachSqlNode(configuration, mixedSqlNode, collection, index, item, open, close, separator);
            targetContents.add(forEachSqlNode);
        }

    }

    /**
     * {@code <if />}或者{@code <when/>} 标签的处理器
     *
     * @see IfSqlNode
     */
    private class IfHandler implements NodeHandler {

        public IfHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #parseDynamicTags(XNode)}传入{@code nodeToHandle}解析{@code <if/>}或者{@code <when/>}标签节点及其内部节点为一个{@link MixedSqlNode}对象【{@code <if/>}或者{@code <when/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）】
         *     </li>
         *     <li>
         *         调用{@code nodeToHandle}的{@link XNode#getStringAttribute(String)}传入"test"获取{@code <if/>}或者{@code <when/>}标签的"test"属性值
         *     </li>
         *     <li>
         *         调用{@link IfSqlNode#IfSqlNode(SqlNode, String)}传入第一步获得的{@link MixedSqlNode}对象和第二步获得的"test"属性值构建一个{@link IfSqlNode}对象
         *     </li>
         *     <li>
         *         将第三步构建的{@link IfSqlNode}对象添加到{@code targetContents}列表中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            String test = nodeToHandle.getStringAttribute("test");
            IfSqlNode ifSqlNode = new IfSqlNode(mixedSqlNode, test);
            targetContents.add(ifSqlNode);
        }

    }

    /**
     * {@code <otherwise />} 标签的处理器
     */
    private class OtherwiseHandler implements NodeHandler {

        public OtherwiseHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         调用{@link #parseDynamicTags(XNode)}传入{@code nodeToHandle}解析{@code <otherwise/>}标签节点及其内部节点为一个{@link MixedSqlNode}对象【{@code <otherwise/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）】
         *     </li>
         *     <li>
         *          来到{@code <otherwise />} 标签就直接执行其内含标签的业务操作即可，不用进行任何操作，所以将第一步得到的{@link MixedSqlNode}添加到{@code targetContents}列表即可
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            MixedSqlNode mixedSqlNode = parseDynamicTags(nodeToHandle);
            targetContents.add(mixedSqlNode);
        }

    }

    /**
     * {@code <choose />} 标签的处理器
     *
     * @see ChooseSqlNode
     */
    private class ChooseHandler implements NodeHandler {

        public ChooseHandler() {
            // Prevent Synthetic Access
        }

        /**
         * <ol>
         *     <li>
         *         分别new两个{@link ArrayList}对象，用来装载将要解析的当前{@code <choose/>}标签内的{@code <when/>}标签和{@code <otherwise/>}标签得到的对应的{@link SqlNode}对象
         *     </li>
         *     <li>
         *         调用{@link #handleWhenOtherwiseNodes(XNode, List, List)}传入{@code nodeToHandle}、用来装载{@code <when/>}标签的{@link SqlNode}对象的{@link ArrayList}、用来装载{@code <otherwise/>}标签的{@link SqlNode}对象的{@link ArrayList} 三个参数进行简单的"标签转化为{@link SqlNode}对象"（没有做什么格式语法校验）
         *     </li>
         *     <li>
         *         调用{@link #getDefaultSqlNode(List)}传入 用来装载{@code <otherwise/>}标签的{@link SqlNode}对象的{@link ArrayList} 校验{@code <otherwise/>}标签的个数并尝试返回一个{@code <otherwise/>}对应的{@link SqlNode}对象或者null
         *     </li>
         *     <li>
         *         调用{@link ChooseSqlNode#ChooseSqlNode(List, SqlNode)}传入 用来装载{@code <when/>}标签的{@link SqlNode}对象的{@link ArrayList}、前一步得到的{@code <otherise/>}标签的{@link SqlNode}对象或者null 两个参数构建一个{@link ChooseSqlNode}对象
         *     </li>
         *     <li>
         *         将{@link ChooseSqlNode}对象添加到{@code targetContents}中
         *     </li>
         * </ol>
         *
         * @param nodeToHandle   要处理的 {@link XNode} 节点
         * @param targetContents 目标的 {@link SqlNode} 列表。实际上，被处理的 {@link XNode} 节点会创建成对应的 {@link SqlNode} 对象，添加到 {@code targetContents} 中
         */
        @Override
        public void handleNode(XNode nodeToHandle, List<SqlNode> targetContents) {
            List<SqlNode> whenSqlNodes = new ArrayList<>();
            List<SqlNode> otherwiseSqlNodes = new ArrayList<>();
            handleWhenOtherwiseNodes(nodeToHandle, whenSqlNodes, otherwiseSqlNodes);
            SqlNode defaultSqlNode = getDefaultSqlNode(otherwiseSqlNodes);
            ChooseSqlNode chooseSqlNode = new ChooseSqlNode(whenSqlNodes, defaultSqlNode);
            targetContents.add(chooseSqlNode);
        }

        /**
         * <ol>
         *     <li>
         *         调用{@code chooseSqlNode}的{@link XNode#getChildren()}获取{@code <choose/>}标签的子标签们对应的{@link XNode}节点集合
         *     </li>
         *     <li>
         *         遍历第一步获得的{@link XNode}节点集合，对于每一个迭代的{@code <choose/>}标签的子节点{@link XNode}对象：
         *         <ol>
         *             <li>
         *                 调用{@link XNode#getNode()}获得子节点对应的{@link Node}对象然后调用{@link Node#getNodeName()}获取对应的标签名称，然后调用{@link #nodeHandlerMap}的{@link HashMap#get(Object)}传入得到的标签名称
         *                 得到对应的{@link NodeHandler}
         *             </li>
         *             <li>
         *                 判断 前面得到的{@link NodeHandler}对象 instanceof {@link IfHandler}：
         *                 <ul>
         *                     <li>
         *                         为true：调用{@link IfHandler#handleNode(XNode, List)}传入当前迭代的子节点{@link XNode}对象（{@code <when/>}标签）和{@code ifSqlNodes}，添加最终得到的{@link IfSqlNode}到{@code ifSqlNodes}中
         *                     </li>
         *                     <li>
         *                         为false：判断 前面得到的{@link NodeHandler}对象 instanceof {@link OtherwiseHandler}：
         *                         <ul>
         *                             <li>
         *                                 为true：调用{@link OtherwiseHandler#handleNode(XNode, List)}传入当前迭代的子节点{@link XNode}对象（{@code <otherwise/>}标签）和{@code defaultSqlNodes}，添加最终得到的{@link MixedSqlNode}（{@code <otherwise/>}标签解析之后得到的{@link SqlNode}对象）到{@code defaultSqlNodes}中
         *                             </li>
         *                             <li>
         *                                 为false：什么也不做，结束本方法
         *                             </li>
         *                         </ul>
         *                     </li>
         *                 </ul>
         *             </li>
         *         </ol>
         *     </li>
         * </ol>
         *
         * @param chooseSqlNode {@code <choose/>}标签对应的{@link XNode}节点
         * @param ifSqlNodes 用来承载{@code <when/>}标签对应的{@link XNode}节点们的集合
         * @param defaultSqlNodes
         */
        private void handleWhenOtherwiseNodes(XNode chooseSqlNode, List<SqlNode> ifSqlNodes, List<SqlNode> defaultSqlNodes) {
            List<XNode> children = chooseSqlNode.getChildren();
            for (XNode child : children) {
                String nodeName = child.getNode().getNodeName();
                NodeHandler handler = nodeHandlerMap.get(nodeName);
                if (handler instanceof IfHandler) {
                    handler.handleNode(child, ifSqlNodes);
                } else if (handler instanceof OtherwiseHandler) {
                    handler.handleNode(child, defaultSqlNodes);
                }
            }
        }

        /**
         * 校验经过方法{@link #handleWhenOtherwiseNodes(XNode, List, List)}解析是否得到了多个{@code <otherwise/>}标签对应的{@link SqlNode}（{@code <choose/>}标签声明了多个{@code <otherwise/>}）
         * <ul>
         *     判断：{@code defaultSqlNodes}.size() == 1
         *     <li>
         *         true：直接返回 defaultSqlNodes.get(0)，本方法结束
         *     </li>
         *     <li>
         *         false：判断：{@code defaultSqlNodes}.size() > 1
         *         <ul>
         *             <li>
         *                 true：抛出异常{@link BuilderException}("Too many default (otherwise) elements in choose statement.");
         *             </li>
         *             <li>
         *                 false：返回 null ，本方法结束
         *             </li>
         *         </ul>
         *     </li>
         * </ul>
         *
         * @param defaultSqlNodes {@code <choose/>}标签用来承载{@code <when/>}标签对应的{@link XNode}节点们的集合
         * @return
         */
        private SqlNode getDefaultSqlNode(List<SqlNode> defaultSqlNodes) {
            SqlNode defaultSqlNode = null;
            if (defaultSqlNodes.size() == 1) {
                defaultSqlNode = defaultSqlNodes.get(0);
            } else if (defaultSqlNodes.size() > 1) {
                throw new BuilderException("Too many default (otherwise) elements in choose statement.");
            }
            return defaultSqlNode;
        }
    }

}
