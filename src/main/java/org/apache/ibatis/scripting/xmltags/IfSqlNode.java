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
 * {@code <if />}或者{@code <when/>}标签的 {@link SqlNode} 实现类
 *
 * @author Clinton Begin
 */
public class IfSqlNode implements SqlNode {

    /**
     * OGNL 表达式计算器（转换器）{@link ExpressionEvaluator}
     */
    private final ExpressionEvaluator evaluator;
    /**
     * {@code <if />}或者{@code <when/>}标签的属性值，OGNL表达式（OGNL表达式支持一些布尔计算，例如："a.b != 1"）
     */
    private final String test;
    /**
     * 内嵌的 {@link SqlNode} 节点们({@link MixedSqlNode})
     */
    private final SqlNode contents;

    /**
     * {@code test}赋值到{@link #test}，{@code contents}赋值到{@link #contents}、new 一个{@link ExpressionEvaluator}对象赋值到{@link #evaluator}
     *
     * @param contents 内嵌的 {@link SqlNode} 节点们({@link MixedSqlNode})
     * @param test {@code <if />}或者{@code <when/>}标签的属性值，OGNL表达式（OGNL表达式支持一些布尔计算，例如："a.b != 1"）
     */
    public IfSqlNode(SqlNode contents, String test) {
        this.test = test;
        this.contents = contents;
        this.evaluator = new ExpressionEvaluator();
    }

    /**
     *
     *         调用{@link #evaluator}的{@link ExpressionEvaluator#evaluateBoolean(String, Object)}传入{@link #test}和{@code context}的{@link DynamicContext#getBindings()}进行OGNL表达式计算（这里并没有进行"${}"Token处理操作，不支持），判断返回的结果是否为true：
     *         <ul>
     *             <li>
     *                 true：调用当前{@link IfSqlNode}的内含节点{@link #contents}的{@link SqlNode#apply(DynamicContext)}传入sql上下文{@code context}执行其内含节点的业务操作，然后返回true，方法结束
     *             </li>
     *             <li>
     *                 false：返回false，方法结束
     *             </li>
     *         </ul>
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        if (evaluator.evaluateBoolean(test, context.getBindings())) {
            contents.apply(context);
            return true;
        }
        return false;
    }

}