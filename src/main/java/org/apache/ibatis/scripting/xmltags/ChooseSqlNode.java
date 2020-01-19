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

import java.util.List;

/**
 * {@code <choose />} 标签的 {@link SqlNode} 实现类
 *
 * @author Clinton Begin
 */
public class ChooseSqlNode implements SqlNode {

    /**
     * {@code <otherwise />} 标签对应的 {@link SqlNode} 节点
     */
    private final SqlNode defaultSqlNode;
    /**
     * {@code <when />} 标签对应的 {@link SqlNode} 节点集合
     */
    private final List<SqlNode> ifSqlNodes;

    /**
     * {@code ifSqlNodes}赋值到{@link #ifSqlNodes}、{@code defaultSqlNode}赋值到{@link #defaultSqlNode}
     *
     * @param ifSqlNodes {@code <when />} 标签对应的 {@link SqlNode} 节点集合
     * @param defaultSqlNode {@code <otherwise />} 标签对应的 {@link SqlNode} 节点
     */
    public ChooseSqlNode(List<SqlNode> ifSqlNodes, SqlNode defaultSqlNode) {
        this.ifSqlNodes = ifSqlNodes;
        this.defaultSqlNode = defaultSqlNode;
    }

    /**
     * <ol>
     *     <li>
     *         遍历迭代{@code <when/>}标签对应的{@link #ifSqlNodes}集合，对于每一个迭代的元素{@link SqlNode}对象（{@link IfSqlNode}）：
     *         <ul>
     *             调用{@link IfSqlNode#apply(DynamicContext)}传入{@code context}，并判断结果是否为true：
     *             <li>
     *                 true：直接 return true，本方法结束
     *             </li>
     *             <li>
     *                 false：进行循环下一迭代，直到return true或者循环结束
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         来到这里，说明{@code <when/>}标签要么没有定义要么没有应用成功，就要尝试应用{@code <otherwise/>}标签了，判断{@link #defaultSqlNode} 不为null：
     *         <ul>
     *             <li>
     *                 true：直接调用{@link #defaultSqlNode}的{@link MixedSqlNode#apply(DynamicContext)}，并返回true，本方法结束
     *             </li>
     *             <li>
     *                 false：返回false，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        for (SqlNode sqlNode : ifSqlNodes) {
            if (sqlNode.apply(context)) {
                return true;
            }
        }
        if (defaultSqlNode != null) {
            defaultSqlNode.apply(context);
            return true;
        }
        return false;
    }

}