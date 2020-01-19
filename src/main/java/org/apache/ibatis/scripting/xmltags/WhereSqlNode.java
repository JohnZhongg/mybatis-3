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

import java.util.Arrays;
import java.util.List;

/**
 * {@code <where />} 标签的 SqlNode 实现类 <br>
 * 该标签用于检测将标签内的sql片段是否以"AND"或者"OR"开头，如果是替换为"WHERE"（实际上底层就是重用了{@link TrimSqlNode}的功能）
 *
 * @author Clinton Begin
 */
public class WhereSqlNode extends TrimSqlNode {

    /**
     * 需要被替换的前缀们："AND ", "OR ", "AND\n", "OR\n", "AND\r", "OR\r", "AND\t", "OR\t"
     */
    private static List<String> prefixList = Arrays.asList("AND ", "OR ", "AND\n", "OR\n", "AND\r", "OR\r", "AND\t", "OR\t");

    /**
     * 代码：
     * <pre>
     *     super({@code configuration}, {@code contents}, "WHERE", {@link #prefixList}, null, null);
     * </pre>
     * 调用父构造器{@link TrimSqlNode#TrimSqlNode(Configuration, SqlNode, String, List, String, List)}传入{@code configuration}, {@code contents}, "WHERE", {@link #prefixList}, null, null 五个参数进行父对象构造，所有api复用{@link TrimSqlNode}的
     *
     * @param configuration 全局{@link Configuration}
     * @param contents {@code <where/>}标签内部节点构成的{@link SqlNode}（{@link MixedSqlNode}）
     */
    public WhereSqlNode(Configuration configuration, SqlNode contents) {
        super(configuration, contents, "WHERE", prefixList, null, null);
    }

}