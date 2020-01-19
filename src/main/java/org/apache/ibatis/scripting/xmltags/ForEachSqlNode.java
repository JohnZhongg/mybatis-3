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

import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.parsing.TokenHandler;
import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Node;

import java.util.Iterator;
import java.util.Map;

/**
 * {@code <foreach />} 标签的 {@link SqlNode} 实现类。该实现类主要用于实现循环拼接sql。
 * <ul>
 *     <li>
 *         可以在{@code <select/>}、{@code <insert/>}、{@code <selectKey/>}、{@code <update/>}、{@code <delete/>}、{@code <sql/>}、{@code <trim/>}、{@code <where/>}、{@code <set/>}、{@code <foreach/>}、{@code <when/>}、{@code <otherwise/>}、{@code <if/>}标签中定义{@code <foreach />}标签
 *     </li>
 *     <li>
 *         {@code <foreach/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）
 *     </li>
 *     <li>
 *         本标签的"separator"属性在执行拼接的时候有三个地方要注意：
 *         <ol>
 *             <li>
 *                 当前sql上下文中的sql（{@link DynamicContext#sqlBuilder}）还没有任何内容的时候（null或者空字符串）不会拼接，参考{@link PrefixedContext#appendSql(String)}第1步判断中的第2和3个布尔表达式；
 *             </li>
 *             <li>
 *                 在{@code <foreach/>}标签循环的第一层迭代的时候或者之前的所有迭代都没有成功拼接过"separator'的时候，当前迭代也不会拼接"separator"而拼接一个空字符串""，参考{@link ForEachSqlNode#apply(DynamicContext)}的第4、6.2、6.7步骤以及{@link PrefixedContext#appendSql(String)}的第1步。
 *             </li>
 *             <li>
 *                 如果当前{@link PrefixedContext#prefixApplied}为true，即当前{@link PrefixedContext}已经拼接过"separator"了，就不会拼接了，同样参考{@link PrefixedContext#appendSql(String)}的第1步
 *             </li>
 *         </ol>
 *     </li>
 * </ul>
 *
 *
 * @author Clinton Begin
 */
public class ForEachSqlNode implements SqlNode {

    public static final String ITEM_PREFIX = "__frch_";

    /**
     * OGNL表达式"计算器"，其实就是封装了通过OGNL表达式获取对应的对象，并做一些检查和转换，参考{@link ExpressionEvaluator#evaluateIterable(String, Object)}
     */
    private final ExpressionEvaluator evaluator;
    /**
     * 集合对象所对应的OGNL表达式（用来在{@link DynamicContext#bindings}中进行解析得到该集合对象）（{@code <foreach/>}标签的"collection"属性）
     */
    private final String collectionExpression;
    /**
     * {@code <foreach/>}标签内部节点们对应的{@link SqlNode}对象（{@link MixedSqlNode}）
     */
    private final SqlNode contents;
    /**
     * {@code <foreach/>}的"open"属性值，当前{@code <foreach/>}标签对应的sql的前缀
     */
    private final String open;
    /**
     * {@code <foreach/>}的"close"属性值，当前{@code <foreach/>}标签对应的sql的后缀
     */
    private final String close;
    /**
     * {@code <foreach/>}的"separator"属性值，当前{@code <foreach/>}标签对应的集合的遍历过程中每一次迭代拼接sql的时候的分隔符
     */
    private final String separator;
    /**
     * {@code <foreach/>}标签的"item"属性值，用于在当前sql上下文中作为{@code <foreach/>}标签对应的集合的遍历过程中当前迭代的元素的引用
     */
    private final String item;
    /**
     * {@code <foreach/>}标签的"index"属性值，用于在当前sql上下文中作为{@code <foreach/>}标签对应的集合的遍历过程中当前迭代的索引的引用
     */
    private final String index;
    /**
     * 全局{@link Configuration}对象
     */
    private final Configuration configuration;

    /**
     * <ol>
     *     <li>
     *         new 一个{@link ExpressionEvaluator}对象赋值到{@link #evaluator}
     *     </li>
     *     <li>
     *         {@code contents}赋值到{@link #contents}、{@code open}赋值到{@link #open}、{@code close}赋值到{@link #close}、{@code separator}赋值到{@link #separator}、{@code index}赋值到{@link #index}、{@code item}赋值到{@link #item}、{@code configuration}赋值到{@link #configuration}、
     *     </li>
     * </ol>
     *
     * @param configuration 全局{@link Configuration}对象
     * @param contents {@code <foreach/>}标签内部节点们对应的{@link SqlNode}对象（{@link MixedSqlNode}）
     * @param collectionExpression {@code <foreach/>}标签的"collection"属性，集合对象所对应的OGNL表达式（用来在{@link DynamicContext#bindings}中进行解析得到该集合对象）
     * @param index {@code <foreach/>}标签的"index"属性值
     * @param item {@code <foreach/>}标签的"item"属性值
     * @param open {@code <foreach/>}的"open"属性值
     * @param close {@code <foreach/>}的"close"属性值
     * @param separator {@code <foreach/>}的"separator"属性值
     */
    public ForEachSqlNode(Configuration configuration, SqlNode contents, String collectionExpression, String index, String item, String open, String close, String separator) {
        this.evaluator = new ExpressionEvaluator();
        this.collectionExpression = collectionExpression;
        this.contents = contents;
        this.open = open;
        this.close = close;
        this.separator = separator;
        this.index = index;
        this.item = item;
        this.configuration = configuration;
    }

    /**
     * <ol>
     *     <li>
     *         调用{@code context}的{@link DynamicContext#getBindings()}获得参数上下文
     *     </li>
     *     <li>
     *         调用{@link #evaluator}的{@link ExpressionEvaluator#evaluateIterable(String, Object)}传入{@link #collectionExpression}和第一步获取的参数上下文对象，得到对应的集合对象{@link Iterable}
     *     </li>
     *     <li>
     *         调用{@link Iterable#iterator()}获取前一步获得的集合对象的迭代器对象{@link Iterator}然后调用{@link Iterator#hasNext()}检查该集合是否存在下一个元素：是则什么也不做继续往下走；否则直接返回true，本方法结束
     *     </li>
     *     <li>
     *         设置一个flag1记录为true（{@code <foreach/>}标签的"separator"属性值对应的业务是在每一次迭代的间隔拼接分隔符，而mybatis是通过每一次迭代都给sql片段拼接前缀，但是第一次拼接的是空字符串，其他次迭代都是拼接{@link ForEachSqlNode#separator}来实现的，而当前设置的变量就是为了区分哪次迭代是第一次。它会在进行第一次了{@link PrefixedContext#appendSql(String)}操作的那一次迭代中被设置为false，也即{@link PrefixedContext#prefixApplied}被设置为false的那一次迭代中。参考6.2和6.7步）
     *     </li>
     *     <li>
     *         调用{@link #applyOpen(DynamicContext)}传入sql上下文{@code context}拼接{@link #open}到上下文的{@link DynamicContext#sqlBuilder}
     *     </li>
     *     <li>
     *         设置一个flag2记录下面每一次循环的索引，从0开始<br>
     *         遍历迭代第三步获得的迭代器对象{@link Iterator}，对于每一个迭代的元素（OGNL表达式获取在承载参数对象上进行解析得到的一个集合对象的元素）：
     *         <ol>
     *             <li>
     *                 新声明一个{@link DynamicContext}类型的变量"oldContext"并将{@code context}的引用赋值给它而记录前一次迭代之后的sql上下文对象{@link DynamicContext}（如果本次迭代是第一次迭代，则记录的对象是进入当前迭代但是还未开始迭代中需要进行的业务操作的时候的sql上下文）
     *             </li>
     *             <li>
     *                 判断: <u><b>"第4步设置的flag1为true 或者 {@link #separator}为null"</b></u>
     *                 <ul>
     *                     <li>
     *                         true：调用{@link PrefixedContext#PrefixedContext(DynamicContext, String)}传入{@code context}和<u><b>空字符串""</b></u>，给{@code context}创建一个被委托者对象{@link PrefixedContext}
     *                     </li>
     *                     <li>
     *                         false：调用{@link PrefixedContext#PrefixedContext(DynamicContext, String)}传入{@code context}和<u><b>{@link #separator}</b></u>，给{@code context}创建一个被委托者对象{@link PrefixedContext}
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 调用{@code context}的{@link DynamicContext#getUniqueNumber()}获得在当前{@code <foreach/>}标签内通过自增保证每一次迭代都是唯一的编号
     *             </li>
     *             <li>
     *                 判断：<u><b>当前迭代的元素 instanceof {@link Map.Entry}</b></u>
     *                 <ul>
     *                     <li>
     *                         true：
     *                         <ol>
     *                             <li>
     *                                 将当前元素强转为{@link Map.Entry}类型
     *                             </li>
     *                             <li>
     *                                 调用{@link #applyIndex(DynamicContext, Object, int)}传入<b><u>{@code context}、{@link Map.Entry#getKey()}、前一步获得的当前迭代的编号</u></b>3个参数
     *                             </li>
     *                             <li>
     *                                 调用{@link #applyItem(DynamicContext, Object, int)}传入<b><u>{@code context}、{@link Map.Entry#getValue()} 、前一步获得的当前迭代的编号</u></b>3个参数
     *                             </li>
     *                         </ol>
     *                     </li>
     *                     <li>
     *                         false：
     *                         <ol>
     *                             <li>
     *                                 调用{@link #applyIndex(DynamicContext, Object, int)}传入<b><u>{@code context}、第6步一开始设置的记录了当前迭代索引的flag2、前一步获得的当前迭代的编号</u></b>3个参数
     *                             </li>
     *                             <li>
     *                                 调用{@link #applyItem(DynamicContext, Object, int)}传入<b><u>{@code context}、当前迭代的元素对象本身、前一步获得的当前迭代的编号</u></b>3个参数
     *                             </li>
     *                         </ol>
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 调用{@link FilteredDynamicContext#FilteredDynamicContext(Configuration, DynamicContext, String, String, int)}传入{@link #configuration}、{@code context}、{@link #index}、{@link #item}、前面得到的当前迭代的编号 5个参数
     *                 构建一个当前迭代中前面构建的{@link PrefixedContext}的被委托者{@link FilteredDynamicContext}，至此，仅仅在当前{@code <foreach/>}标签内，就对其上层标签传递下来的{@link DynamicContext}sql上下文对象进行了两次封装（委托）。
     *             </li>
     *             <li>
     *                 调用{@link #contents}的{@link SqlNode#apply(DynamicContext)}传入前一步构建的{@link FilteredDynamicContext}对象，执行{@code <foreach/>}内部标签基于当前迭代得到的变量的解析（此时传递给内部{@link SqlNode}的都是被委托者{@link FilteredDynamicContext}对象，所以其内部所以节点都能应用到{@code <foreach/>}标签的功能，另外为了防止多个内部标签重复使用{@link PrefixedContext}对象而重复拼接{@link #separator}，添加了{@link PrefixedContext#prefixApplied}作为标识进行标记）
     *             </li>
     *             <li>
     *                 判断第4步设置的flag1是否为true：
     *                 <ul>
     *                     <li>
     *                         true：如果{@code context}的{@link PrefixedContext#isPrefixApplied()}为true，则设置其为false
     *                     </li>
     *                     <li>
     *                         false：什么也不做
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 将6.1步中记录在"oldContext"变量中的原始{@link DynamicContext}重新赋值覆盖到{@code context}变量【在进入下一次迭代之前，重置{@code context}变量为相对{@code <foreach/>}标签来说是原始的{@link DynamicContext}sql上下文对象的引用，防止下一次迭代的时候被委托者受委托的还是上一次迭代的被委托者，导致出现某些功能重复执行的情况（被委托者封装的一些缓存或者外壳），同时重置后的{@link DynamicContext}的{@link DynamicContext#bindings}和{@link DynamicContext#sqlBuilder}也具备了上一次迭代的结果了】
     *             </li>
     *             <li>
     *                 将第6步一开始设置的flag2自增1
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         循环结束后，调用{@link #applyClose(DynamicContext)}拼接{@link #close}到{@link DynamicContext#sqlBuilder}
     *     </li>
     *     <li>
     *         调用{@code context}的{@link DynamicContext#getBindings()}通过{@link Map#remove(Object)}分别传入{@link #item}和{@link #index}移除{@code <foreach/>}标签中"item"和"index"属性值对应的绑定
     *     </li>
     *     <li>
     *         return true，方法结束
     *     </li>
     * </ol>
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        Map<String, Object> bindings = context.getBindings();
        final Iterable<?> iterable = evaluator.evaluateIterable(collectionExpression, bindings);
        if (!iterable.iterator().hasNext()) {
            return true;
        }
        boolean first = true;
        applyOpen(context);
        int i = 0;
        for (Object o : iterable) {
            DynamicContext oldContext = context;
            if (first || separator == null) {
                context = new PrefixedContext(context, "");
            } else {
                context = new PrefixedContext(context, separator);
            }
            int uniqueNumber = context.getUniqueNumber();
            // Issue #709
            if (o instanceof Map.Entry) {
                @SuppressWarnings("unchecked")
                Map.Entry<Object, Object> mapEntry = (Map.Entry<Object, Object>) o;
                applyIndex(context, mapEntry.getKey(), uniqueNumber);
                applyItem(context, mapEntry.getValue(), uniqueNumber);
            } else {
                applyIndex(context, i, uniqueNumber);
                applyItem(context, o, uniqueNumber);
            }
            contents.apply(new FilteredDynamicContext(configuration, context, index, item, uniqueNumber));
            if (first) {
                first = !((PrefixedContext) context).isPrefixApplied();
            }
            context = oldContext;
            i++;
        }
        applyClose(context);
        context.getBindings().remove(item);
        context.getBindings().remove(index);
        return true;
    }

    /**
     * <ul>
     *     如果{@link #index}不为null
     *     <li>
     *         true：
     *         <ol>
     *             <li>
     *                 调用{@code context}的{@link DynamicContext#bind(String, Object)}传入{@link #index}和{@code o}绑定该键值对到当前sql上下文中，每一次的循环迭代都会覆盖上一次的值。（此时{@code <foreach/>}标签内部的子孙{@link TextSqlNode}节点可以也只能根据"${index属性值}"或者当前迭代的索引）
     *             </li>
     *             <li>
     *                 先调用{@link #itemizeItem(String, int)}传入{@link #index}和{@code i}得到一个拼接后的key，然后调用{@code context}的{@link DynamicContext#bind(String, Object)}传入获得的拼接后的key和{@code o}绑定该键值对到当前sql上下文中。另外由方法逻辑已经决定是在同一个{@code <foreach/>}标签内是不存在重复或者覆盖的情况，而经过测试，即使存在多个{@code <foreach/>}标签也不会出现这种情况，貌似是一个sql用的就是一个上下文，多个{@code <foreach/>}标签也是调用同一个{@link DynamicContext#getUniqueNumber()}一直往上递增（结合{@link FilteredDynamicContext#appendSql(String)}方便后期整理预编译sql中的"#{}"）
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         false：什么也不做，本方法结束
     *     </li>
     * </ul>
     *
     * @param context 当前sql上下文
     * @param o 当前{@code <foreach/>}标签对应的集合的当前遍历得到的对象
     * @param i 当前{@code <foreach/>}标签对应的集合的当前遍历的(唯一)编号
     */
    private void applyIndex(DynamicContext context, Object o, int i) {
        if (index != null) {
            context.bind(index, o);
            context.bind(itemizeItem(index, i), o);
        }
    }

    /**
     * <ul>
     *     如果{@link #item}不为null
     *     <li>
     *         true：
     *         <ol>
     *             <li>
     *                 调用{@code context}的{@link DynamicContext#bind(String, Object)}传入{@link #item}和{@code o}绑定该键值对到当前sql上下文中，每一次的循环迭代都会覆盖上一次的值。（此时{@code <foreach/>}标签内部的子孙{@link TextSqlNode}节点可以也只能根据"${item属性值}"或者当前迭代的元素值）
     *             </li>
     *             <li>
     *                 先调用{@link #itemizeItem(String, int)}传入{@link #item}和{@code i}得到一个拼接后的key，然后调用{@code context}的{@link DynamicContext#bind(String, Object)}传入获得的拼接后的key和{@code o}绑定该键值对到当前sql上下文中。另外由方法逻辑已经决定是在同一个{@code <foreach/>}标签内是不存在重复或者覆盖的情况，而经过测试，即使存在多个{@code <foreach/>}标签也不会出现这种情况，貌似是一个sql用的就是一个上下文，多个{@code <foreach/>}标签也是调用同一个{@link DynamicContext#getUniqueNumber()}一直往上递增（结合{@link FilteredDynamicContext#appendSql(String)}方便后期整理预编译sql中的"#{}"）
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         false：什么也不做，本方法结束
     *     </li>
     * </ul>
     *
     * @param context 当前sql上下文
     * @param o 当前{@code <foreach/>}标签对应的集合的当前遍历得到的对象
     * @param i 当前{@code <foreach/>}标签对应的集合的当前遍历的(唯一)编号
     */
    private void applyItem(DynamicContext context, Object o, int i) {
        if (item != null) {
            context.bind(item, o);
            context.bind(itemizeItem(item, i), o);
        }
    }

    /**
     * <ul>
     *     {@link #open} != null:
     *     <li>
     *         是则：调用{@code context}的{@link DynamicContext#appendSql(String)}传入{@link #open}拼接到{@link DynamicContext#sqlBuilder}，本方法结束
     *     </li>
     *     <li>
     *         否则：什么也不做，本方法结束
     *     </li>
     * </ul>
     *
     * @param context sql上下文
     */
    private void applyOpen(DynamicContext context) {
        if (open != null) {
            context.appendSql(open);
        }
    }

    /**
     * <ul>
     *     {@link #close} != null:
     *     <li>
     *         是则：调用{@code close}的{@link DynamicContext#appendSql(String)}传入{@link #close}拼接到{@link DynamicContext#sqlBuilder}，本方法结束
     *     </li>
     *     <li>
     *         否则：什么也不做，本方法结束
     *     </li>
     * </ul>
     *
     * @param context sql上下文
     */
    private void applyClose(DynamicContext context) {
        if (close != null) {
            context.appendSql(close);
        }
    }

    /**
     * 给{@code <foreach/>}标签中"item"或者"index"属性值拼接前缀{@link #ITEM_PREFIX}和后缀{@code i}
     *
     * @param item {@code <foreach/>}标签中"item"或者"index"属性值
     * @param i 在遍历{@code <foreach/>}标签对应的集合的每一次迭代中得到的一个唯一编号
     * @return
     */
    private static String itemizeItem(String item, int i) {
        return ITEM_PREFIX + item + "_" + i;
    }

    /**
     *
     */
    private static class FilteredDynamicContext extends DynamicContext {

        /**
         * 委托者
         */
        private final DynamicContext delegate;
        /**
         * {@code <foreach/>}标签当前迭代对应的唯一标识
         * @see DynamicContext#getUniqueNumber()
         */
        private final int index;
        /**
         * @see  ForEachSqlNode#index
         */
        private final String itemIndex;
        /**
         * @see ForEachSqlNode#item
         */
        private final String item;

        /**
         * <ol>
         *     <li>
         *         调用父构造器{@link DynamicContext#DynamicContext(Configuration, Object)}传入{@code configuration}和null，构建父对象{@link DynamicContext}
         *     </li>
         *     <li>
         *         {@code delegate}赋值到{@link #delegate}、{@code i}赋值到{@link #index}、{@code itemIndex}赋值到{@link #itemIndex}、{@code item}赋值到{@link #item}
         *     </li>
         * </ol>
         *
         * @param configuration 全局{@link Configuration}对象
         * @param delegate 委托者
         * @param itemIndex {@code <foreach/>}标签的"index"属性值
         * @param item {@code <foreach/>}标签的"item"属性值
         * @param i {@code <foreach/>}标签遍历过程中当前迭代对应的索引
         */
        public FilteredDynamicContext(Configuration configuration, DynamicContext delegate, String itemIndex, String item, int i) {
            super(configuration, null);
            this.delegate = delegate;
            this.index = i;
            this.itemIndex = itemIndex;
            this.item = item;
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getBindings()}
         */
        @Override
        public Map<String, Object> getBindings() {
            return delegate.getBindings();
        }

        /**
         * 调用{@link #delegate}的{@link DynamicContext#bind(String, Object)}传入{@code name}和{@code value}
         *
         * @param name
         * @param value
         */
        @Override
        public void bind(String name, Object value) {
            delegate.bind(name, value);
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getSql()}
         */
        @Override
        public String getSql() {
            return delegate.getSql();
        }

        /**
         *
         *     查看token"#{}"的内容是否匹配"item属性值前面可以带有空白字符串，后面的字符可以是空白字符，半角逗号，半角句号，冒号"，这种情况下直接替换该Token内容字符串中的"item属性值"部分为"{@link ForEachSqlNode#ITEM_PREFIX}+item属性值+当前迭代得到的{@link DynamicContext#getUniqueNumber()}"，如果没有符合的情况，再使用"index"属性值做同样的操作。然后再给操作之后的字符串拼接"#{"前缀和"}"后缀并返回，然后拼接到当前sql上下文的{@link DynamicContext#sqlBuilder}中
         *     <pre>
         *         {@code
         *         <foreach collection="list" item="item" index="index" >
         *             #{  item}        --成功--> #{  __frch_item_0}
         *             #{   item. }     --成功--> #{   __frch_item_0. }
         *             #{   item,}      --成功--> #{   __frch_item_0,}
         *             #{item:a}        --成功--> #{__frch_item_0:a}
         *             #{   item a}     --成功--> #{   __frch_item_0 a}
         *             #{   item.item}  --成功--> #{   __frch_item_0.item}
         *             #{   item;a}     --失败--> #{   item;a}
         *             #{   itema}      --失败--> #{   itema}
         *             #{ index}        --成功--> #{ __frch_index_0}
         *             #{aindex}        --失败--> #{aindex}
         *             #{aindex.}       --失败--> #{aindex.}
         *         </foreach> }
         *     </pre>
         * <ol>
         *     <li>
         *         使用lambda构建一个Token处理器{@link org.apache.ibatis.parsing.TokenHandler}实例，其{@link org.apache.ibatis.parsing.TokenHandler#handleToken(String)}逻辑为：
         *         <ol>
         *             <li>
         *                 接收到的入参为一个检测出来的Token的内容
         *             </li>
         *             <li>
         *                 调用{@link #itemizeItem(String, int)}传入{@link #item}和{@link #index}拼接当前循环的唯一标识得到与{@link #applyIndex(DynamicContext, Object, int)}中第二步的key一致的值，调用该Token内容字符串的{@link String#replaceFirst(String, String)}传入 <u><b>正则表达式"^\\s*" + {@link #item} + "(?![^.,:\\s])"和前面得到的值</b></u> 进行替换
         *             </li>
         *             <li>
         *                 判断 {@link #itemIndex} != null 并且 上述替换动作之后得到的字符串.equals(原字符串):
         *                 <ul>
         *                     <li>
         *                         true：调用{@link #itemizeItem(String, int)}传入{@link #item}和{@link #item}拼接当前循环的唯一标识得到与{@link #applyItem(DynamicContext, Object, int)}中第二步的key一致的值，调用该Token内容字符串的{@link String#replaceFirst(String, String)}传入 <u><b>正则表达式"^\\s*" + {@link #itemIndex} + "(?![^.,:\\s])"和前面得到的值</b></u> 进行替换
         *                     </li>
         *                     <li>
         *                         false：什么也不做，继续往下走
         *                     </li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 return "#{" + 经过上述操作之后的Token的内容 + "}"
         *             </li>
         *         </ol>
         *     </li>
         *     <li>
         *         调用{@link GenericTokenParser#GenericTokenParser(String, String, TokenHandler)}传入"#{"、"}"、第一步得到的{@link TokenHandler}实例得到一个Token"#{}"的解析器，然后调用{@link GenericTokenParser#parse(String)}传入{@code sql}得到解析之后的sql
         *     </li>
         *     <li>
         *         调用{@link #delegate}的{@link DynamicContext#appendSql(String)}传入解析后的sql
         *     </li>
         * </ol>
         *
         * @param sql sql片段
         */
        @Override
        public void appendSql(String sql) {
            GenericTokenParser parser = new GenericTokenParser("#{", "}", content -> {
                String newContent = content.replaceFirst("^\\s*" + item + "(?![^.,:\\s])", itemizeItem(item, index));
                if (itemIndex != null && newContent.equals(content)) {
                    newContent = content.replaceFirst("^\\s*" + itemIndex + "(?![^.,:\\s])", itemizeItem(itemIndex, index));
                }
                return "#{" + newContent + "}";
            });

            delegate.appendSql(parser.parse(sql));
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getUniqueNumber()}
         */
        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }

    }

    /**
     * delegate委派模式<br>
     * 具有"在每次拼接sql片段到{@link DynamicContext#sqlBuilder}的时候给该sql片段添加前缀"功能的上下文，参考{@link PrefixedContext#appendSql(String)}
     */
    private class PrefixedContext extends DynamicContext {

        /**
         * 委托的{@link DynamicContext}对象
         */
        private final DynamicContext delegate;
        /**
         * 要拼接的前缀
         */
        private final String prefix;
        /**
         * 当前上下文对象是否已经拼接过 {@link #prefix} 到{@link DynamicContext#sqlBuilder}中了
         * （或者说调用过一次{@link PrefixedContext#appendSql(String)}了）。由于{@code <foreach/>}标签内部可能存在多个其他标签的，而基于myabtis实现动态sql标签使用的委托者模式，当前同一个{@link PrefixedContext}对象
         * 是回被传递给多个内部{@link SqlNode}节点的，所以为了避免这多个内部节点重复调用{@link PrefixedContext#appendSql(String)}的时候重复拼接{@link #prefix}，所以增加当前标识
         */
        private boolean prefixApplied;

        /**
         * <ol>
         *     <li>
         *         super(configuration, null);    //调用父构造器{@link DynamicContext#DynamicContext(Configuration, Object)}传入{@link #configuration}和null构造父对象
         *     </li>
         *     <li>
         *         {@code delegate}赋值到{@link #delegate}、{@code prefix}赋值到{@link #prefix}、赋值false给{@link #prefixApplied}
         *     </li>
         * </ol>
         *
         * @param delegate 委托的{@link DynamicContext}对象
         * @param prefix 要拼接的前缀
         */
        public PrefixedContext(DynamicContext delegate, String prefix) {
            super(configuration, null);
            this.delegate = delegate;
            this.prefix = prefix;
            this.prefixApplied = false;
        }

        /**
         * @return {@link #prefixApplied}
         */
        public boolean isPrefixApplied() {
            return prefixApplied;
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getBindings()}
         */
        @Override
        public Map<String, Object> getBindings() {
            return delegate.getBindings();
        }

        /**
         * 调用{@link #delegate}的{@link DynamicContext#bind(String, Object)}传入{@code name}和{@code value}
         *
         * @param name
         * @param value
         */
        @Override
        public void bind(String name, Object value) {
            delegate.bind(name, value);
        }

        /**
         * <ol>
         *     <li>
         *         判断 !prefixApplied && sql != null && sql.trim().length() > 0 【当前{@link PrefixedContext}对象的{@link PrefixedContext#prefix}没有被拼接过 并且 ({@code sql}不为null 并且 {@code sql} 不是空白字符串)(即当前)】：
         *         <ul>
         *             <li>
         *                 true：调用{@link #delegate}的{@link DynamicContext#appendSql(String)}传入{@link #prefix}拼接前缀到委托的{@link DynamicContext#sqlBuilder}中，然后设置{@link #prefixApplied}为true
         *             </li>
         *             <li>
         *                 false：什么也不做，继续往下走
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         调用{@link #delegate}的{@link DynamicContext#appendSql(String)}传入{@code sql}拼接sql到委托的{@link DynamicContext#sqlBuilder}中
         *     </li>
         * </ol>
         *
         * @param sql
         */
        @Override
        public void appendSql(String sql) {
            if (!prefixApplied && sql != null && sql.trim().length() > 0) {
                delegate.appendSql(prefix);
                prefixApplied = true;
            }

            delegate.appendSql(sql);
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getSql()}
         */
        @Override
        public String getSql() {
            return delegate.getSql();
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getUniqueNumber()}
         */
        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }
    }

}
