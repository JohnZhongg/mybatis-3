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

/**
 * 静态文本的 SqlNode 实现类
 *
 * @author Clinton Begin
 */
public class StaticTextSqlNode implements SqlNode {

    /**
     * 静态文本
     */
    private final String text;

    /**
     * {@code text}赋值到{@link #text}
     *
     * @param text
     */
    public StaticTextSqlNode(String text) {
        this.text = text;
    }

    /**
     * 调用{@code context}的{@link DynamicContext#appendSql(String)}传入{@link #text}拼接sql到{@link DynamicContext#sqlBuilder}，然后返回true
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        context.appendSql(text);
        return true;
    }

}