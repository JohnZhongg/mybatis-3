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
 * {@code <bind />} 标签的 SqlNode 实现类<br>
 * 该标签用于将"value"属性值作为一个OGNL表达式到{@link DynamicContext#bindings}中获取对应的对象然后将该对象作为value，该标签"name"属性值作为key，put到{@link DynamicContext#bindings}中【即给某些可能的参数设置一个快捷访问名称或者别名，不只只通过OGNL表达式访问】
 *
 * @author Frank D. Martinez [mnesarco]
 */
public class VarDeclSqlNode implements SqlNode {

    /**
     * 名字
     */
    private final String name;
    /**
     * OGNL表达式
     */
    private final String expression;

    /**
     * {@code var}赋值到{@link #name}、{@code exp}赋值到{@link #expression}
     *
     * @param var {@code <bind />} 标签的name属性值
     * @param exp {@code <bind />} 标签的value属性值
     */
    public VarDeclSqlNode(String var, String exp) {
        name = var;
        expression = exp;
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link OgnlCache#getValue(String, Object)}传入{@link #expression}和{@code context}的{@link DynamicContext#getBindings()}从当前上下文获取对应的对象
     *     </li>
     *     <li>
     *         将{@link #name}作为key，上面获得的对象作为value，调用{@code context}的{@link DynamicContext#bind(String, Object)}put到{@link DynamicContext#bindings}中
     *     </li>
     *     <li>
     *         返回true
     *     </li>
     * </ol>
     *
     * @param context sql上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        final Object value = OgnlCache.getValue(expression, context.getBindings());
        context.bind(name, value);
        return true;
    }

}