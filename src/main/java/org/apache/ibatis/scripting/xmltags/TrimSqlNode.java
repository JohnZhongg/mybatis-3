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

import org.apache.ibatis.session.Configuration;
import org.w3c.dom.Node;

import java.util.*;

/**
 * {@code <trim />} 标签的 SqlNode 实现类 <br>
 * （可以在{@code <select/>}、{@code <insert/>}、{@code <selectKey/>}、{@code <update/>}、{@code <delete/>}、{@code <sql/>}、{@code <trim/>}、{@code <where/>}、{@code <set/>}、{@code <foreach/>}、{@code <when/>}、{@code <otherwise/>}、{@code <if/>}标签中定义{@code <trim />}标签）<br>
 * 【{@code <trim/>}标签可以包含{@code <include />}、{@code <trim />}、{@code <where />}、{@code <set />}、{@code <foreach />}、{@code <choose />}、{@code <if />}、{@code <bind />}标签，其中{@code <include />}标签在到达本方法之前有专门的方法{@link org.apache.ibatis.builder.xml.XMLIncludeTransformer#applyIncludes(Node)}进行递归解析成对应的{@code <sql/>}标签中的文本或者其他标签（在{@code <sql/>}标签中再遇到包含{@code <include/>}标签，进行递归解析，其他标签不管）】<br>
 * 该标签用于检测标签内部的sql片段是否包含某些前缀或者某些后缀，如果包含则替换成某个前缀或者某个后缀
 *
 * @author Clinton Begin
 */
public class TrimSqlNode implements SqlNode {

    /**
     * {@code <trim />}标签内含的 {@link SqlNode} 节点
     */
    private final SqlNode contents;
    /**
     * 前缀
     */
    private final String prefix;
    /**
     * 后缀
     */
    private final String suffix;
    /**
     * 需要被删除的前缀
     */
    private final List<String> prefixesToOverride;
    /**
     * 需要被删除的后缀
     */
    private final List<String> suffixesToOverride;
    private final Configuration configuration;

    /**
     * <ol>
     *     <li>
     *         对于{@code prefixesToOverride}和{@code suffixesToOverride}分别调用{@link #parseOverrides(String)}进行转换成一个{@link ArrayList}对象
     *     </li>
     *     <li>
     *         调用构造器{@link #TrimSqlNode(Configuration, SqlNode, String, List, String, List)}传入{@code configuration}、{@code contents}、{@code prefix}、第一步转换后的{@code prefixesToOverride}、{@code suffix}、第一步转换后的{@code suffixesToOverride}进行实例化
     *     </li>
     * </ol>
     *
     * @param configuration {@link Configuration}对象
     * @param contents {@code <trim />}标签内含的 {@link SqlNode} 节点
     * @param prefix 前缀
     * @param prefixesToOverride 需要被删除的前缀们，使用"|"分割
     * @param suffix 后缀
     * @param suffixesToOverride 需要被删除的后缀们，使用"|"分割
     */
    public TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, String prefixesToOverride, String suffix, String suffixesToOverride) {
        this(configuration, contents, prefix, parseOverrides(prefixesToOverride), suffix, parseOverrides(suffixesToOverride));
    }

    /**
     * {@code configuration}、{@code contents}、{@code prefix}、{@code prefixesToOverride}、{@code suffix}、{@code suffixesToOverride}分别赋值到{@link #configuration}、{@link #contents}、{@link #prefix}、{@link #prefixesToOverride}、{@link #suffix}、{@link #suffixesToOverride}
     *
     * @param configuration {@link Configuration}对象
     * @param contents {@code <trim />}标签内含的 {@link SqlNode} 节点
     * @param prefix 前缀
     * @param prefixesToOverride 需要被删除的前缀们
     * @param suffix 后缀
     * @param suffixesToOverride 需要被删除的后缀们
     */
    protected TrimSqlNode(Configuration configuration, SqlNode contents, String prefix, List<String> prefixesToOverride, String suffix, List<String> suffixesToOverride) {
        this.contents = contents;
        this.prefix = prefix;
        this.prefixesToOverride = prefixesToOverride;
        this.suffix = suffix;
        this.suffixesToOverride = suffixesToOverride;
        this.configuration = configuration;
    }

    /**
     * <ol>
     *     <li>
     *         调用构造器{@link FilteredDynamicContext#FilteredDynamicContext(DynamicContext)}传入{@code context}构建一个{@link FilteredDynamicContext}对象，委托原本的{@code context}给它
     *     </li>
     *     <li>
     *         调用{@link #contents}的{@link SqlNode#apply(DynamicContext)}传入第一步得到的{@link FilteredDynamicContext}对象，执行{@code <trim/>}标签下所有标签对应的{@link SqlNode}节点的{@link SqlNode#apply(DynamicContext)}方法，
     *         只要有针对sql本身（{@link DynamicContext#sqlBuilder}）的标签，这些节点拼接的sql都会缓存到{@link FilteredDynamicContext}中
     *     </li>
     *     <li>
     *         调用{@link FilteredDynamicContext#applyAll()} 将第二步缓存的sql进行{@code <trim/>}标签的"trim"动作：遍历{@link #prefixesToOverride}和{@link #suffixesToOverride}，检测缓存的sql片段是否匹配其中要删除的prefix或者suffix，只要遇到第一个匹配的，
     *         删除该prefix或者suffix，然后结束遍历，在sql片段前面插入{@link #prefix}或者后面追加{@link #suffix}，然后拼接"trim"操作之后的sql片段到原始{@code context}中。详细参考{@link FilteredDynamicContext#applyAll()}
     *     </li>
     *     <li>
     *         return 第二步执行{@code <trim/>}标签下所有标签对应的{@link SqlNode}节点的{@link SqlNode#apply(DynamicContext)}方法返回的结果，本方法结束
     *     </li>
     * </ol>
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        FilteredDynamicContext filteredDynamicContext = new FilteredDynamicContext(context);
        boolean result = contents.apply(filteredDynamicContext);
        filteredDynamicContext.applyAll();
        return result;
    }

    /**
     * <ol>
     *     使用 | 分隔字符串成字符串数组，并都转换成大写：
     *     <li>
     *         判断{@code overrides}是否为null
     *         <ul>
     *             <li>
     *                 不为null：
     *                 <ol>
     *                     <li>
     *                         调用构造器{@link StringTokenizer#StringTokenizer(String, String, boolean)}传入{@code overrides}、"|"、false三个参数实例化一个{@link StringTokenizer}对象（第三个参数为false表示获得的Token字符串中不包含第二个参数，如果为true则表示获得的Token字符串中包含第二个参数）
     *                     </li>
     *                     <li>
     *                         调用{@link ArrayList#ArrayList(int)}传入前一步获得的{@link StringTokenizer#countTokens()}构建一个指定长度的集合
     *                     </li>
     *                     <li>
     *                         遍历{@link StringTokenizer}对象中的每个Token（通过{@link StringTokenizer#hasMoreTokens()}判断是否还有Token和{@link StringTokenizer#nextToken()}获得下一个Token来实现）：
     *                         对于每一个迭代的Token，调用{@link String#toUpperCase(Locale)}传入{@link Locale#ENGLISH}转换成大写，然后添加到前一步构建的{@link ArrayList}对象中
     *                     </li>
     *                     <li>
     *                         返回该{@link ArrayList}对象，本方法结束
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 为null：返回 {@link Collections#emptyList()}，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param overrides 字符串
     * @return 字符串数组
     */
    private static List<String> parseOverrides(String overrides) {
        if (overrides != null) {
            final StringTokenizer parser = new StringTokenizer(overrides, "|", false);
            final List<String> list = new ArrayList<>(parser.countTokens());
            while (parser.hasMoreTokens()) {
                list.add(parser.nextToken().toUpperCase(Locale.ENGLISH));
            }
            return list;
        }
        return Collections.emptyList();
    }

    /**
     * delegate 委派模式 <br>
     * 该对象用于缓存trim中的sql片段，并提供applyPrefix和applySuffix的逻辑
     *
     */
    private class FilteredDynamicContext extends DynamicContext {

        /**
         * 委托的 {@link DynamicContext} 对象
         */
        private DynamicContext delegate;
        /**
         * 是否 prefix 已经被应用
         */
        private boolean prefixApplied;
        /**
         * 是否 suffix 已经被应用
         */
        private boolean suffixApplied;
        /**
         * 缓存当前{@code <trim/>}标签内部包含的sql片段（无论是经过"trim"处理前还是处理后的）
         *
         * @see #appendSql(String)
         */
        private StringBuilder sqlBuffer;

        /**
         * <ol>
         *     <li>
         *         调用 {@code super(configuration, null);}进行父对象构造（即{@link DynamicContext#DynamicContext(Configuration, Object)}）
         *     </li>
         *     <li>
         *         {@code delegate}赋值到{@link #delegate}、赋值false到{@link #prefixApplied}和{@link #suffixApplied}、new 一个{@link StringBuilder}对象赋值到{@link #sqlBuffer}
         *     </li>
         * </ol>
         *
         * @param delegate 委托的 {@link DynamicContext} 对象
         */
        public FilteredDynamicContext(DynamicContext delegate) {
            super(configuration, null);
            this.delegate = delegate;
            this.prefixApplied = false;
            this.suffixApplied = false;
            this.sqlBuffer = new StringBuilder();
        }

        /**
         * <ol>
         *     <li>
         *         首先进行{@link #sqlBuffer}.toString().trim()操作对整个{@link #sqlBuffer}进行两段trim得到一个新的字符串，然后调用构造器{@link StringBuilder#StringBuilder(String)}传入新的字符串构造一个新的{@link StringBuilder}对象覆盖赋值到{@link #sqlBuffer}
         *     </li>
         *     <li>
         *         调用{@link #sqlBuffer}.toString()的{@link String#toUpperCase(Locale)}传入{@link Locale#ENGLISH}得到{@link #sqlBuffer}所有字符转换成大写之后的字符串（转成大写为了匹配{@link TrimSqlNode#parseOverrides(String)}中的"1.不为null.3"步骤的转成大写操作，参考{@link #applyPrefix(StringBuilder, String)}和{@link #applySuffix(StringBuilder, String)}中的"否.2.否"中的判断逻辑依赖于当前步骤）<br>
         *         判断第二步得到的字符串.length() 是否大于0：
         *         <ul>
         *             <li>
         *                 是则：调用{@link #applyPrefix(StringBuilder, String)}传入{@link #sqlBuffer}和第二步得到的trim和转大写之后的字符串进行前缀替换操作；然后再调用{@link #applySuffix(StringBuilder, String)}传入{@link #sqlBuffer}和第二步得到的trim和转大写之后的字符串进行后缀替换操作。
         *             </li>
         *             <li>
         *                 否则：什么也不做，继续往下走
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         调用{@link #delegate}的{@link DynamicContext#appendSql(String)}传入处理后得到的{@link #sqlBuffer}.toString()拼接到委托的{@link DynamicContext}中
         *     </li>
         * </ol>
         */
        public void applyAll() {
            sqlBuffer = new StringBuilder(sqlBuffer.toString().trim());
            String trimmedUppercaseSql = sqlBuffer.toString().toUpperCase(Locale.ENGLISH);
            if (trimmedUppercaseSql.length() > 0) {
                applyPrefix(sqlBuffer, trimmedUppercaseSql);
                applySuffix(sqlBuffer, trimmedUppercaseSql);
            }
            delegate.appendSql(sqlBuffer.toString());
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
         * @return {@link #delegate}的{@link DynamicContext#getUniqueNumber()}
         */
        @Override
        public int getUniqueNumber() {
            return delegate.getUniqueNumber();
        }

        /**
         * 调用{@link #sqlBuffer}的{@link StringBuilder#append(int)}传入{@code sql}。<b>（注意：本继承方法覆盖了委托对象的方法，其他继承方法都是直接调用委托对象的对应方法。{@link TrimSqlNode#apply(DynamicContext)}中
         * 对于传入的{@link DynamicContext}对象委托到本对象，然后再对于{@link TrimSqlNode}内部解析后得到的所有内含{@link SqlNode}对象循环调用其{@link SqlNode#apply(DynamicContext)}，此时的入参就是{@link FilteredDynamicContext}对象了
         * 。所有针对sql本身的{@link SqlNode}原本对于{@link DynamicContext#appendSql(String)}调用都变成了{@link FilteredDynamicContext#appendSql(String)}，即{@code <trim/>}标签内部的所有sql都会缓存到{@link FilteredDynamicContext#sqlBuffer}中了，
         * 然后在处理完所有{@code <trim/>}标签的内含标签的{@link SqlNode#apply(DynamicContext)}之后，对该{@link FilteredDynamicContext}对象进行{@link FilteredDynamicContext#applyAll()}调用，统一进行"trim"处理，然后再调用其委托对象{@link FilteredDynamicContext#delegate}的
         * {@link DynamicContext#appendSql(String)}传入{@link FilteredDynamicContext#sqlBuffer}.toString()拼接"trim"之后的sql到{@link DynamicContext}中，至此完成对于某段sql使用{@code <trim/>}标签进行"trim'操作的业务逻辑）</b>
         *
         * @param sql
         */
        @Override
        public void appendSql(String sql) {
            sqlBuffer.append(sql);
        }

        /**
         * @return {@link #delegate}的{@link DynamicContext#getSql()}
         */
        @Override
        public String getSql() {
            return delegate.getSql();
        }

        /**
         * <ul>
         *     判断{@link #prefixApplied}是否为true：
         *     <li>
         *         是则：什么也不做，本方法结束
         *     </li>
         *     <li>
         *         否则：
         *         <ol>
         *             <li>
         *                 {@link #prefixApplied}赋值为true
         *             </li>
         *             <li>
         *                 判断{@link #prefixesToOverride}是否为null：
         *                 <ul>
         *                     <li>
         *                         是则：什么也不做，继续往下走
         *                     </li>
         *                     <li>
         *                         否则：遍历{@link #prefixesToOverride}
         *                         <ul>
         *                             判断 <u><b>"{@code trimmedUppercaseSql}.startsWith(迭代的每一个{@link String}对象元素)"</b></u> 检测当前sql判断是否以当前字符串开头
         *                             <li>
         *                                 是则：调用{@code sql}.delete(0, 当前迭代的元素.trim().length())<b>（注意：这里在前面检测是否匹配前缀的时候没有trim，但是删除的时候又trim了。。）</b>，然后 break，结束循环。
         *                             </li>
         *                             <li>
         *                                 否则：进入下一迭代，直至遍历结束
         *                             </li>
         *                         </ul>
         *                     </li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 判断{@link #prefix}是否为null：是则什么也不做，本方法结束；否则调用{@code sql}.insert(0, " ")，然后再调用{@code sql}.insert(0, {@link #prefix})
         *             </li>
         *         </ol>
         *     </li>
         * </ul>
         *
         * @param sql {@code <trim/>}标签中包含的sql（即{@link FilteredDynamicContext#sqlBuffer}），经过两端trim操作的
         * @param trimmedUppercaseSql {@code <trim/>}标签中包含的sql（{@link FilteredDynamicContext#sqlBuffer}的副本，用于统一转换成大写然后根据前缀的长度确定原始sql片段需要删除的index），经过两端trim操作且转换成了大写的
         */
        private void applyPrefix(StringBuilder sql, String trimmedUppercaseSql) {
            if (!prefixApplied) {
                prefixApplied = true;
                if (prefixesToOverride != null) {
                    for (String toRemove : prefixesToOverride) {
                        if (trimmedUppercaseSql.startsWith(toRemove)) {
                            sql.delete(0, toRemove.trim().length());
                            break;
                        }
                    }
                }
                if (prefix != null) {
                    sql.insert(0, " ");
                    sql.insert(0, prefix);
                }
            }
        }

        /**
         * <ul>
         *     判断{@link #suffixApplied}是否为true：
         *     <li>
         *         是则：什么也不做，本方法结束
         *     </li>
         *     <li>
         *         否则：
         *         <ol>
         *             <li>
         *                 {@link #suffixApplied}赋值为true
         *             </li>
         *             <li>
         *                 判断{@link #suffixesToOverride}是否为null：
         *                 <ul>
         *                     <li>
         *                         是则：什么也不做，继续往下走
         *                     </li>
         *                     <li>
         *                         否则：遍历{@link #suffixesToOverride}
         *                         <ul>
         *                             判断 <u><b>"{@code trimmedUppercaseSql}.endsWith(迭代的每一个{@link String}对象元素) || {@code trimmedUppercaseSql}.endsWith(迭代的每一个{@link String}对象元素.trim())"</b></u> 检测当前sql判断是否以当前字符串或者经过了trim之后的当前字符串结尾
         *                             <li>
         *                                 是则：
         *                                 <ol>
         *                                     <li>
         *                                         调用{@code sql}.length() - 当前迭代元素.trim().length() 计算出要{@code sql}中要删除的起始index <b>（这里有问题吗？？？？万一当前迭代的元素两端携带了空白符然后{@code sql}又是以当前迭代的元素结尾的呢？那到时候不就是删少了吗。。）</b>
         *                                     </li>
         *                                     <li>
         *                                         {@code sql}.length()作为删除的结束index
         *                                     </li>
         *                                     <li>
         *                                         调用{@code sql}.delete(起始index, 结束index)进行截取
         *                                     </li>
         *                                     <li>
         *                                         break，结束遍历
         *                                     </li>
         *                                 </ol>
         *                             </li>
         *                             <li>
         *                                 否则：进入下一迭代，直至遍历结束
         *                             </li>
         *                         </ul>
         *                     </li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 判断{@link #suffix}是否为null：是则什么也不做，本方法结束；否则调用{@code sql}.append(" ")，然后再调用{@code sql}.append({@link #suffix})
         *             </li>
         *         </ol>
         *     </li>
         * </ul>
         *
         * @param sql {@code <trim/>}标签中包含的sql（即{@link FilteredDynamicContext#sqlBuffer}），经过两端trim操作的
         * @param trimmedUppercaseSql {@code <trim/>}标签中包含的sql（{@link FilteredDynamicContext#sqlBuffer}的副本，用于统一转换成大写然后根据后缀的长度确定原始sql片段需要删除的index），经过两端trim操作且转换成了大写的
         */
        private void applySuffix(StringBuilder sql, String trimmedUppercaseSql) {
            if (!suffixApplied) {
                suffixApplied = true;
                if (suffixesToOverride != null) {
                    for (String toRemove : suffixesToOverride) {
                        if (trimmedUppercaseSql.endsWith(toRemove) || trimmedUppercaseSql.endsWith(toRemove.trim())) {
                            int start = sql.length() - toRemove.trim().length();
                            int end = sql.length();
                            sql.delete(start, end);
                            break;
                        }
                    }
                }
                if (suffix != null) {
                    sql.append(" ");
                    sql.append(suffix);
                }
            }
        }

    }

}
