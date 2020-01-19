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

import java.util.Collections;
import java.util.List;

/**
 * `<set />` 标签的 SqlNode 实现类 <br>
 * 该标签用于检测给该标签内的sql片段直接添加前缀"SET"，然后检查是否以半角逗号","结尾，如果是就删除","（实际上底层就是重用了{@link TrimSqlNode}的功能）
 *
 * @author Clinton Begin
 */
public class SetSqlNode extends TrimSqlNode {

    /**
     * 要被删除的后缀：","
     */
    private static List<String> suffixList = Collections.singletonList(",");

    /**
     * 代码：
     * <pre>
     *     super({@code configuration}, {@code contents}, "SET", null, null, {@link #suffixList});
     * </pre>
     * 调用父构造器{@link TrimSqlNode#TrimSqlNode(Configuration, SqlNode, String, List, String, List)}传入{@code configuration}, {@code contents}, "SET", null, null, {@link #suffixList} 五个参数进行父对象构造，所有api复用{@link TrimSqlNode}的
     *
     * @param configuration 全局{@link Configuration}
     * @param contents {@code <set/>}标签内部节点构成的{@link SqlNode}（{@link MixedSqlNode}）
     */
    public SetSqlNode(Configuration configuration, SqlNode contents) {
        super(configuration, contents, "SET", null, null, suffixList);
    }

}