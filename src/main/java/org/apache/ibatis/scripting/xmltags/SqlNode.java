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

import org.apache.ibatis.parsing.XNode;

/**
 * 一个抽象的sql node接口，只抽象定义了sql节点的一个行为，就是将当前节点应用（解析）到当前sql上下文中（注意：经过{@link SqlNode}）处理之后得到的{@link DynamicContext}中的{@link DynamicContext#sqlBuilder}只是处理了"${}"，即动态sql指的是包含"${}"，"#{}"不是，它在{@link SqlNode}阶段还没有做赋值处理，还是保持"#{}"不变，只不过在{@code <foreach/>}标签那里针对每一次循环给其内容拼接了一些当次循环的唯一标识
 *
 * @author Clinton Begin
 */
public interface SqlNode {

    /**
     * 应用（解析）当前 {@link SqlNode} 节点对象，其功能主要分两方面：1、针对sql本身（拼接、插入、截断、修改等等的操作）（针对{@link DynamicContext#sqlBuilder}，最终都会调用{@link DynamicContext#appendSql(String)}）；2、针对sql上下文（针对{@link DynamicContext#bindings}，最终都会调用{@link DynamicContext#bind(String, Object)}）<br>
     * 其中要注意的是入参{@code context}{@link DynamicContext}对象使用了委托者模式，为了实现标签嵌套，看以下示例：
     * <pre>
     * {@code
     * <select>
     *     <foreach collection="" item="" index="" open="" close="" separator"">
     *         文本1${}
     *         <trim>
     *             文本2${}
     *             <choose>
     *                 <when test="true">文本3${动态sql占位符}#{预编译占位符}</when>
     *                 <when test="false">文本4${}</when>
     *                 <otherwisee>文本5${}</otherwisee>
     *             </choose>
     *             文本6${}
     *         </trim>
     *         <trim></trim>
     *         文本7${}
     *     </foreach>
     *     <trim>文本8${}</trim>
     * </select>}
     * </pre>
     * 上面的{@code <select/>}标签内部第一层有一个{@code <foreach/>}标签，两个{@code <trim/>}标签，对于{@code <select/>}标签来说，它经过{@link XMLScriptBuilder#parseDynamicTags(XNode)}之后会得到一个
     * {@link MixedSqlNode}对象，这个对象内部包含一个{@link ForEachSqlNode}和两个{@link TrimSqlNode}对象，在调用{@link MixedSqlNode#apply(DynamicContext)}的时候，其内部是这样按照顺序执行的：
     * <ol>
     *     <li>
     *         将刚创建的最原始{@link DynamicContext}sql上下文对象传递给{@code <foreach/>}标签对应的{@link ForEachSqlNode#apply(DynamicContext)}。
     *         <ul>
     *             对于{@link ForEachSqlNode}对象来说
     *             <li>
     *                 它的工作就是找到"collection"元素对应的集合对象，然后基于集合的大小进行遍历循环，而每一次循环迭代里面的业务操作，分为{@code <foreach/>}标签本身的工作和{@code <foreach>}标签内部节点提供的业务功能两部分
     *             </li>
     *             <li>
     *                 其中，{@code <foreach/>}标签本身的工作又有两部分：1、在每一次迭代中在{@link DynamicContext#bindings}中记录当次迭代的索引和对象的引用，方便其他内部节点根据"#{}"token获取它们。2、在第一次迭代拼接"open"
     *                 元素，在中间的迭代拼接"separator"元素，在最后一次迭代拼接"close"元素。这些工作中使用了{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}和{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext}
     *                 上下文被委托者进行封装
     *             </li>
     *         </ul>
     *         所以每次{@code <foreach/>}标签的迭代工作如下：
     *         <ol>
     *             <li>
     *                 其中第一部分方便内部节点根据"${}"token获取当前迭代的索引或者元素对{@link DynamicContext#bindings}进行赋值以及根据每一次迭代的唯一标识对"#{}"中的内容进行拼接操作方便后面执行sql的时候根据预编译的参数进行参数值获取由{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}实现
     *                 第二部分的拼接sql工作由{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext}实现。即每一次迭代中都会对上一次迭代完的{@link DynamicContext}上下文对象（第一次迭代操作的是{@code <select/>}标签的原始的{@link DynamicContext}对象）
     *                 委托给{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext}然后再将该{@link com.sun.scenario.effect.impl.prism.PrFilterContext}对象二次委托给{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}对象
     *             </li>
     *             <li>
     *                 在上述构建完被委托者{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}（其受委托的是{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext}）之后，对于
     *                 {@code <foreach/>}标签的内部{@link MixedSqlNode}执行{@link SqlNode#apply(DynamicContext)}并传入该{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}对象。而这个{@link MixedSqlNode}对象
     *                 又包含了两个{@code <trim/>}标签对应的{@link TrimSqlNode}对象，此时{@link MixedSqlNode#apply(DynamicContext)}即：
     *                 <ol>
     *                     <li>
     *                         第一个{@link TrimSqlNode}对象的{@link TrimSqlNode#apply(DynamicContext)}被调用，传入的是{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}对象，而{@link TrimSqlNode}的工作是：
     *                         <ol>
     *                             <li>
     *                                 缓存其内部节点包含的所有sql片段为一个sql片段，这个工作由{@link TrimSqlNode.FilteredDynamicContext}来实现，参考{@link TrimSqlNode#apply(DynamicContext)}的第二步，将传入的{@link ForEachSqlNode.FilteredDynamicContext}委托到{@link TrimSqlNode.FilteredDynamicContext}，然后调用该{@code <trim/>}标签的内部{@link MixedSqlNode#apply(DynamicContext)}传入该{@link TrimSqlNode.FilteredDynamicContext}，此时{@link MixedSqlNode}包含了一个{@code <choose/>}标签，即：
     *                                 <ol>
     *                                     <li>
     *                                         调用{@code <choose/>}标签对应的{@link ChooseSqlNode#apply(DynamicContext)}传入上述被委托者{@link TrimSqlNode.FilteredDynamicContext}，此时{@code <choose/>}标签只是一个选择功能，即遍历其内部的
     *                                         {@code <when/>}和{@code <otherwise/>}标签列表执行其对应的{@link SqlNode#apply(DynamicContext)}，没有对传入的{@link TrimSqlNode.FilteredDynamicContext}进一步委托，所以其作用就是传递接收到的{@link TrimSqlNode.FilteredDynamicContext}
     *                                         给"test"为true的{@code <when/>}标签或者{@code <otherwise/>}标签对应的{@link IfSqlNode#apply(DynamicContext)}或者{@link TextSqlNode}（{@code <otherwise/>}内部正好文本5），可以看到第一个{@code <when/>}标签的"test"是true，
     *                                         所以，此时会调用其对应{@link IfSqlNode#apply(DynamicContext)}传入{@link TrimSqlNode.FilteredDynamicContext}，此时又会调用该{@link IfSqlNode}的内部{@link MixedSqlNode#apply(DynamicContext)}，而该
     *                                         {@link MixedSqlNode}只包含一个{@link TextSqlNode}，即：
     *                                         <ol>
     *                                             <li>
     *                                                 调用文本3对应的{@link TextSqlNode#apply(DynamicContext)}传入上述{@link TrimSqlNode.FilteredDynamicContext}。<u><b><br>
     *                                                (<br>
     *                                                 注意，重点来了：此时{@link TextSqlNode#apply(DynamicContext)}中的最后一步
     *                                                 对于{@link DynamicContext#appendSql(String)}的调用，此时就会触发了被委托者的受委托的动作了。此时委托链：<br>最原始{@link DynamicContext}---委托到--->{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext}---委托到--->{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}---委托到--->{@link TrimSqlNode.FilteredDynamicContext}<br>
     *                                                 最左边的对象是被包含在最里面的，所以当调{@link TextSqlNode#apply(DynamicContext)}中调用{@link DynamicContext#appendSql(String)}的时候就会产生以下调用链：
     *                                                 <ol>
     *                                                     <li>
     *                                                         调用{@link TextSqlNode#apply(DynamicContext)}，第三步进行动态sql解析"${动态sql占位符}"，第四步调用{@link DynamicContext#appendSql(String)}相当于调用{@link org.apache.ibatis.scripting.xmltags.TrimSqlNode.FilteredDynamicContext#appendSql(String)}（这一步骤并没有调用其内部受委托对象{@link org.apache.ibatis.scripting.xmltags.TrimSqlNode.FilteredDynamicContext#delegate}的{@link DynamicContext#appendSql(String)}方法），此时会将经过第三步Token解析替换之后的sql片段全部缓存到{@link org.apache.ibatis.scripting.xmltags.TrimSqlNode.FilteredDynamicContext}中了
     *                                                     </li>
     *                                                     <li>
     *                                                         {@link TextSqlNode#apply(DynamicContext)} return true ，结束{@link TextSqlNode}业务，返回到{@link IfSqlNode#apply(DynamicContext)}，该方法再返回true，返回到{@link ChooseSqlNode#apply(DynamicContext)}，该方法返回true，回到{@link TrimSqlNode#apply(DynamicContext)}的第二步结束处，开始第三步
     *                                                     </li>
     *                                                 </ol>
     *                                                 )</b></u>
     *                                             </li>
     *                                         </ol>
     *                                     </li>
     *                                 </ol>
     *                             </li>
     *                             <li>
     *                                 上面步骤结束后，即到了{@link TrimSqlNode#apply(DynamicContext)}的第三步，对其缓存的内部sql片段进行"trim"业务操作，然后再调用受委托对象{@link TrimSqlNode.FilteredDynamicContext#delegate}的{@link DynamicContext#appendSql(String)}传入经过"trim"之后的sql片段，此时就是调用了{@link ForEachSqlNode.FilteredDynamicContext#appendSql(String)}，
     *                                 该方法接收经过前面受委托者处理后的sql片段对"#{预编译占位符}"的内容进行替换拼接(当前{@code <foreach/>}循环中设置的一些变量)，拼接业务完成后，再调用{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.FilteredDynamicContext}内部的受委托对象{@link ForEachSqlNode.FilteredDynamicContext#delegate}的{@link DynamicContext#appendSql(String)}方法传入处理后的sql片段，此时就是调用了{@link ForEachSqlNode.PrefixedContext#appendSql(String)}，在进行前后缀替换拼接之后，
     *                                 再调用{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext}内部的受委托对象{@link org.apache.ibatis.scripting.xmltags.ForEachSqlNode.PrefixedContext#delegate}的{@link DynamicContext#appendSql(String)}方法，此时调用的就是{@code <select/>}标签最原始的{@link DynamicContext}上下文对象，将经过所有处理之后的sql片段拼接到sql中。
     *                             </li>
     *                         </ol>
     *                     </li>
     *                     <li>
     *                         第二个{@link TrimSqlNode#apply(DynamicContext)}调用工作一样，而且传入的{@link DynamicContext}还是和第一个兄弟标签{@code <trim/>}一样，是同一个{@link ForEachSqlNode.FilteredDynamicContext}（{@link ForEachSqlNode.FilteredDynamicContext}后续的受委托者只是其外面包装了一些外壳，然后基于这些外壳状态操作该原始{@link ForEachSqlNode.FilteredDynamicContext}，来到当前节点得到的指针还是指向那个{@link ForEachSqlNode.FilteredDynamicContext}的，只不过里面的内容是经过之前的业务操作变化了的）
     *                     </li>
     *                 </ol>
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         经过{@code <foreach/>}标签的修饰之后来到这里就是和其并行的另一个{@code <trim/>}标签了，即{@code <select/>}标签的{@link MixedSqlNode}的第二循环或者最后一遍循环，此时回传入{@code <select/>}标签的最原始的那个{@link DynamicContext}标签，对其执行{@code <trim/>}标签的业务
     *     </li>
     * </ol>
     *
     *
     * @param context 上下文
     * @return 当前 SQL Node 节点是否应用成功。
     */
    boolean apply(DynamicContext context);

}