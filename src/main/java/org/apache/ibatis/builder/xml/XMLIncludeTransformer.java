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

import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * XML {@code <include />} 标签的转换器
 *
 * @author Frank D. Martinez [mnesarco]
 */
public class XMLIncludeTransformer {

    private final Configuration configuration;
    private final MapperBuilderAssistant builderAssistant;

    public XMLIncludeTransformer(Configuration configuration, MapperBuilderAssistant builderAssistant) {
        this.configuration = configuration;
        this.builderAssistant = builderAssistant;
    }

    /**
     * 传入可能包含{@code <include />}标签的标签{@code source}（目前看到调用本方法的来源就是解析 {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} ）对应的{@link Node}对象，并解析其包含的{@code <include />}标签们为对应的{@code <sql/>}标签的内容及标签内递归含有的token"${}"：
     * <ol>
     *     <li>
     *         创建一个空的{@link Properties}对象作为当前{@code source}的变量空间，然后调用成员变量{@link #configuration}的{@link Configuration#getVariables()}获取配置的全局{@link Properties}对象，如果不为null，则调用
     *         新创建的空的{@link Properties}的{@link Properties#putAll(Map)}将全局{@link Properties}包含进去
     *     </li>
     *     <li>
     *         调用{@link #applyIncludes(Node, Properties, boolean)}传入{@code source}、上面新建的{@link Properties}对象、false再进行详细解析
     *     </li>
     * </ol>
     *
     * @param source
     */
    public void applyIncludes(Node source) {
        Properties variablesContext = new Properties();
        Properties configurationVariables = configuration.getVariables();
        if (configurationVariables != null) {
            variablesContext.putAll(configurationVariables);
        }
        applyIncludes(source, variablesContext, false);
    }

    /**
     * Recursively apply includes through all SQL fragments.<br>
     *
     * <ul>
     *     使用递归的方式，将{@code source}中的 {@code <include />}标签替换成其引用的 {@code <sql />}，并解析其中可能存在的token"${}"（注意：只会解析{@code <sql />}及其子孙标签或者文本节点中的token；另外这里只替换{@code <include/>}标签为{@code <sql/>}标签的内容，其他标签不做替换处理）：<br>
     *     <b>
     *         在这里{@code source}可以是 {@code <select />}、{@code <insert />}、{@code <update />}、{@code <delete />} 、{@code <selectKey />} 、{@code <trim />}、{@code <where />}、{@code <set />} 、{@code <foreach />} 、{@code <choose />}和{@code <when />}和{@code <otherwise />}、{@code <if />} 、{@code <include />}、{@code <bind />}.
     *         其中在第一次对本方法调用的时候{@code source}只会是前四者中其中一个，到了后续递归调用可以是后十一者中其中一个标签或者标签内的文本对象（{@code <selectKey />}只能是{@code <insert />}和{@code <update />}的子标签，后面的标签则可以作为除了最后的两个标签{@code <include />}和{@code <bind />}之外的其他十三个标签的子标签：{@code <include />}只有{@code <property />}子标签；{@code <bind />}没有子标签）
     *     </b>
     *     <li>
     *         如果{@code source}的{@link Node#getNodeName()}equals("include")（正在处理的标签就是{@code <include/>}标签，找到其对应的{@code <sql/>}标签并替换）：
     *          <ol>
     *               <li>
     *                   调用{@link #getStringAttribute(Node, String)}传入{@code source}和"refid"获取当前{@code <include/>}标签的"refid"属性值，然后调用{@link #findSqlFragment(String, Properties)}传入获取到的"refid"属性值和{@code variablesContext}
     *                   处理其可能包含的"${}"，然后根据解析之后的"refid"属性值获取对应{@code <sql/>}{@link Node}对象副本
     *               </li>
     *               <li>
     *                   调用{@link #getVariablesContext(Node, Properties)}传入上面获取到的{@link Node}副本对象和{@code variablesContext}获取当前{@code <sql/>}对象之内拥有和继承的{@link Properties}变量
     *               </li>
     *               <li>
     *                   再次调用自己{@link #applyIncludes(Node, Properties, boolean)}传入前面获取到的{@code <sql/>}标签的{@link Node}对象副本、该副本拥有的变量{@link Properties}和true进行递归处理{@code <sql/>}中还可能拥有的{@code <include/>}标签
     *               </li>
     *               <li>
     *                   调用{@code source}的{@link Node#getOwnerDocument()}和前面根据"refid"属性值获取到的{@code <sql/>}标签的{@link Node#getOwnerDocument()}进行 != 比较：如果不是同一个对象，说明当前这个{@code <sql/>}标签不是在当前xml中声明的，
     *                   调用{@code source}的{@link Node#getOwnerDocument()}获取{@link org.w3c.dom.Document}对象然后再调用{@link org.w3c.dom.Document#importNode(Node, boolean)}传入获取到的{@code <sql/>}标签对应的{@link Node}对象和true将该
     *                   对象插入到当前{@code source}对应的标签对应的{@link org.w3c.dom.Document}中并返回新的{@code source}对应的{@link Node}对象；否则什么也不做继续往下走
     *               </li>
     *               <li>
     *                   调用{@code source}的{@link Node#getParentNode()}的{@link Node#replaceChild(Node, Node)}传入前面一步新构建好的{@code source}对应的{@link Node}对象和旧对象{@code source}本身将{@code source}的父节点中的原有的当前{@code source}
     *                   节点进行替换成新构建好的节点
     *               </li>
     *               <li>
     *                   第3步返回的对象是经过对递归处理（只替换所有子孙的{@code <include/>}标签为{@code <sql/>}而不替换其他标签；只处理所有{@code <include/>}对应的{@code <sql/>}及其子孙标签的属性或者文本中携带的token"${}"）之后的{@code <sql/>}标签，在这里将返回的{@code <sql/>}标签对应的{@link Node}对象进行
     *                   循环调用{@link Node#getParentNode()}获得父节点然后调用{@link Node#insertBefore(Node, Node)}传入：当前{@code <sql/>}的第一个子节点（通过{@link Node#getFirstChild()}获取）、当前{@code <sql/>}的{@link Node}对象表示将{@code <sql/>}
     *                   的第一个子节点插入到其所在父节点的前面的位置，然后通过{@link Node#hasChildNodes()}来判断当前{@link <sql/>}是否还有子节点而继续循环还是结束循环（这里应该是在父节点{@link Node#insertBefore(Node, Node)}或者当前{@code <sql/>}节点（通过{@link Node#getFirstChild()}
     *                   这里导致当前节点的第一个子节点在每次循环中都被移除了而实现循环的，具体是哪个步骤导致的没有细看）
     *               </li>
     *               <li>
     *                   通过{@link Node#getParentNode()}已经插入到当前{@link org.w3c.dom.Document}中的{@code <sql/>}对象的父节点调用{@link Node#removeChild(Node)}传入当前{@code <sql/>}标签，表示从当前{@link org.w3c.dom.Document}中移除{@code <sql/>}
     *                   （将标签内容替换该标签在父节点中的位置）
     *               </li>
     *           </ol>
     *     </li>
     *     <li>
     *         如果{@code source}不是{@code <include/>}标签（参考上面列出的可能的标签）并且调用{@code source}的{@link Node#getNodeType()} == {@link Node#ELEMENT_NODE} （是一个标签对象"{@code <XXX />}"）：
     *         <ol>
     *             <li>
     *                 判断如果 {@code included}为true 并且 {@code variablesContext}.isEmpty()为false：
     *                 <ul>
     *                     <li>
     *                         是则：通过{@code source}的{@link Node#getAttributes()}获取其所有属性对应的{@link NamedNodeMap}对象，然后遍历该对象（通过{@link NamedNodeMap#getLength()}和{@link NamedNodeMap#item(int)}传入指定的索引来实现遍历），
     *                         对于遍历中迭代的每一个{@link Node}对象（{@code source}的属性）调用{@link Node#getNodeValue()}获取其属性值然后调用{@link PropertyParser#parse(String, Properties)}传入获取到的属性值和{@code variablesContext}
     *                         替换处理属性值中可能存在的token"${}"，然后再调用{@link Node#setNodeValue(String)}传入处理后的属性值进行覆盖操作
     *                     </li>
     *                     <li>
     *                         否则：什么也不做
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 调用{@code source}的{@link Node#getChildNodes()}获取当前标签节点的所有子节点（可能是标签对象，也可能是文本对象）{@link NodeList}进行遍历（通过{@link NodeList#getLength()}和{@link NodeList#item(int)}传入指定的索引实现遍历），
     *                 对于每一个遍历中迭代的对象{@link Node}，递归调用本方法{@link #applyIncludes(Node, Properties, boolean)}传入当前迭代的对象、{@code variablesContext}、{@code included}进行递归处理
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         如果{@code source}不是{@code <include/>}标签（参考上面列出的可能的标签）并且调用{@code source}的{@link Node#getNodeType()} == {@link Node#TEXT_NODE} （是一个文本对象，"{@code <标签对象>文本对象XXX</标签对象>}"） 并且 {@code variablesContext}.isEmpty() 为false：
     *         <ul>
     *             <li>
     *                 是则：调用{@code source}的{@link Node#getNodeValue()}获取当前文本节点对象的内容，然后同样通过{@link PropertyParser#parse(String, Properties)}传入获取的文本内容和{@code variablesContext}处理可能存在的token"${}"，然后再调用{@link Node#setNodeValue(String)}传入
     *                 处理后的文本内容进行覆盖
     *             </li>
     *             <li>
     *                 否则：什么也不做
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     *
     *
     * @param source 可能包含了{@code <include/>}标签的标签对应{@link Node}对象（需要递归替换其包含的子孙{@code <include/>}标签）
     * @param variablesContext Current context for static variables with values
     * @param included 是否在处理一个 {@code <include />} 标签对应的 {@code <sql/>}标签其本身或者其内容（子孙标签或者文本）
     */
    private void applyIncludes(Node source, final Properties variablesContext, boolean included) {
        if (source.getNodeName().equals("include")) {
            Node toInclude = findSqlFragment(getStringAttribute(source, "refid"), variablesContext);
            Properties toIncludeContext = getVariablesContext(source, variablesContext);
            applyIncludes(toInclude, toIncludeContext, true);
            if (toInclude.getOwnerDocument() != source.getOwnerDocument()) {
                toInclude = source.getOwnerDocument().importNode(toInclude, true);
            }
            source.getParentNode().replaceChild(toInclude, source);
            while (toInclude.hasChildNodes()) {
                toInclude.getParentNode().insertBefore(toInclude.getFirstChild(), toInclude);
            }
            toInclude.getParentNode().removeChild(toInclude);
        } else if (source.getNodeType() == Node.ELEMENT_NODE) {
            if (included && !variablesContext.isEmpty()) {
                // replace variables in attribute values
                NamedNodeMap attributes = source.getAttributes();
                for (int i = 0; i < attributes.getLength(); i++) {
                    Node attr = attributes.item(i);
                    attr.setNodeValue(PropertyParser.parse(attr.getNodeValue(), variablesContext));
                }
            }
            NodeList children = source.getChildNodes();
            for (int i = 0; i < children.getLength(); i++) {
                applyIncludes(children.item(i), variablesContext, included);
            }
        } else if (included && source.getNodeType() == Node.TEXT_NODE
                && !variablesContext.isEmpty()) {
            source.setNodeValue(PropertyParser.parse(source.getNodeValue(), variablesContext));
        }
    }

    /**
     * 根据{@code refid}作为{@code <sql/>}标签的id获取其对应的{@link Node}对象：
     * <ol>
     *     <li>
     *         调用{@link PropertyParser#parse(String, Properties)}传入{@code refid}和{@code variables}，解析{@code refid}中可能存在的"${}"
     *     </li>
     *     <li>
     *         调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#applyCurrentNamespace(String, boolean)}传入上面处理过的{@code refid}和true，获取对应的绝对id
     *     </li>
     *     <li>
     *         <pre>
     *         try {
     *             XNode nodeToInclude = configuration.getSqlFragments().get(refid);
     *             return nodeToInclude.getNode().cloneNode(true);
     *         } catch (IllegalArgumentException e) {
     *             throw new IncompleteElementException("Could not find SQL statement to include with refid '" + refid + "'", e);
     *         }
     *         </pre>调用成员变量{@link #configuration}的{@link Configuration#getSqlFragments()}获取{@link Configuration#sqlFragments}然后再调用其方法{@link org.apache.ibatis.session.Configuration.StrictMap#get(Object)}
     *         传入处理过的绝对{@code refid}获取对应的{@link XNode}对象。然后调用{@link XNode#getNode()}获取对应的{@link Node}对象，最后再调用{@link Node#cloneNode(boolean)}传入true，克隆一个当前{@code <sql/>}标签对应的{@link Node}
     *         对象的副本进行返回。以上过程中抛出的{@link IllegalArgumentException}都会被捕获，然后抛出一个{@link IncompleteElementException}异常
     *     </li>
     * </ol>
     *
     * @param refid 指定编号
     * @param variables 全局变量
     * @return <sql /> 对应的节点
     */
    private Node findSqlFragment(String refid, Properties variables) {
        refid = PropertyParser.parse(refid, variables);
        refid = builderAssistant.applyCurrentNamespace(refid, true);
        try {
            XNode nodeToInclude = configuration.getSqlFragments().get(refid);
            return nodeToInclude.getNode().cloneNode(true);
        } catch (IllegalArgumentException e) {
            throw new IncompleteElementException("Could not find SQL statement to include with refid '" + refid + "'", e);
        }
    }

    /**
     * 调用{@code node}的{@link Node#getAttributes()}获取当前节点{@code node}的所有属性对象{@link NamedNodeMap}，然后调用{@link NamedNodeMap#getNamedItem(String)}传入
     * {@code name}根据属性名获取到对应的属性对象{@link Node}，然后调用{@link Node#getNodeValue()}获取对应的属性值
     *
     * @param node
     * @param name
     * @return
     */
    private String getStringAttribute(Node node, String name) {
        return node.getAttributes().getNamedItem(name).getNodeValue();
    }

    /**
     * Read placeholders and their values from include node definition. <br>
     * 获得包含 {@code <include />} 标签内的属性 Properties 对象（继承且覆盖{@code inheritedVariablesContext}中的内容）：
     * <ol>
     *     <li>
     *         声明一个{@link Map}{@code <String, String>}的变量指向null
     *     </li>
     *     <li>
     *         调用{@code node}的{@link Node#getChildNodes()}获取{@code <include />}下的所有子标签{@link NodeList}对象({@code <property/>}标签们)
     *     </li>
     *     <li>
     *         遍历{@link NodeList}对象（通过{@link NodeList#getLength()}获取列表长度，通过{@link NodeList#item(int)}传入index获取当前迭代的对象）：
     *         <ol>
     *             <li>
     *                 如果当前遍历对象{@link Node#getNodeType()} == {@link Node#ELEMENT_NODE}，则两次分别调用{@link #getStringAttribute(Node, String)}传入当前迭代元素{@link Node}对象和"name"、当前迭代元素{@link Node}对象和"value"
     *                 分别获取"name"属性值和"value"属性值（在mybatis的dtd文件中已经限制了{@code <include/>}标签下只能含有{@code <property name="${}" value="${}${}"/>}标签），然后调用{@link PropertyParser#parse(String, Properties)}传入
     *                 "value"属性值和{@code inheritedVariablesContext}解析"value"属性值中的"${}"。
     *             </li>
     *             <li>
     *                 如果第一步声明的变量还是null，则new 一个{@link HashMap}对象赋值给它
     *             </li>
     *             <li>
     *                 调用第一步声明的变量指向的{@link Map}对象的{@link Map#put(Object, Object)}传入3.1获取的"name"属性值和获取并处理过"${}"的"value"属性值将当前迭代的{@code <property/>标签}的内容存下来，看下该方法是否返回null：返回null则什么也不做继续往下；如果不返回null
     *                 ，则说明当前{@code <include />}中定义了相同的"name"属性，存在重复的定义，直接抛出异常{@link BuilderException}。
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         <ul>
     *             第一步声明的变量是否还是null
     *             <li>
     *                 是则：说明在{@code </include>}中没有声明{@code <property/>}，直接返回{@code inheritedVariablesContext}
     *             </li>
     *             <li>
     *                 否则：说明在{@code </include>}中声明了{@code <property/>}，先new一个新的{@link Properties}对象，先调用{@link Properties#putAll(Map)}传入{@code inheritedVariablesContext}将继承的变量设置进去，然后再调用一次{@link Properties#putAll(Map)}传入第一步
     *                 声明的{@link Map}变量指向的对象将{@code <include/>}中包含的{@code <property/>}全部设置进行(该方法如果存在冲突则进行覆盖，{@code <include/>}中包含的{@code <property/>}覆盖继承的)，返回new的{@link Properties}对象
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param node Include node instance（{@code <include />} 标签）
     * @param inheritedVariablesContext Current context used for replace variables in new variables values
     * @return variables context from include instance (no inherited values)
     */
    private Properties getVariablesContext(Node node, Properties inheritedVariablesContext) {
        Map<String, String> declaredProperties = null;
        NodeList children = node.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node n = children.item(i);
            if (n.getNodeType() == Node.ELEMENT_NODE) {
                String name = getStringAttribute(n, "name");
                // Replace variables inside
                String value = PropertyParser.parse(getStringAttribute(n, "value"), inheritedVariablesContext);
                if (declaredProperties == null) {
                    declaredProperties = new HashMap<>();
                }
                if (declaredProperties.put(name, value) != null) {
                    throw new BuilderException("Variable " + name + " defined twice in the same include definition");
                }
            }
        }
        if (declaredProperties == null) {
            return inheritedVariablesContext;
        } else {
            Properties newProperties = new Properties();
            newProperties.putAll(inheritedVariablesContext);
            newProperties.putAll(declaredProperties);
            return newProperties;
        }
    }
}
