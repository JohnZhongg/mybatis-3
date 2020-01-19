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
 * 混合的 SqlNode 实现类（其实就是一个提供了集中多个{@link SqlNode}对象，然后一次调用循环处理这些对象的api）
 *
 * @author Clinton Begin
 */
public class MixedSqlNode implements SqlNode {

    /**
     * 内嵌的 {@link SqlNode} 列表
     */
    private final List<SqlNode> contents;

    /**
     * {@code contents}赋值到{@link #contents}
     *
     * @param contents
     */
    public MixedSqlNode(List<SqlNode> contents) {
        this.contents = contents;
    }

    /**
     * 遍历{@link #contents}，对于每一个元素调用{@link SqlNode#apply(DynamicContext)}传入{@code context}。循环调用完之后返回true
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        for (SqlNode sqlNode : contents) {
            sqlNode.apply(context);
        }
        return true;
    }

}