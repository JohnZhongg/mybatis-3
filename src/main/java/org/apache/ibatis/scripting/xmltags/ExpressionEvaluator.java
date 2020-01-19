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

import org.apache.ibatis.builder.BuilderException;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OGNL 表达式计算器（转换器）
 *
 * @author Clinton Begin
 */
public class ExpressionEvaluator {

    /**
     * 判断表达式对应的值并转换为布尔值
     * <ol>
     *     <li>
     *         调用{@link OgnlCache#getValue(String, Object)}传入{@code expression}和{@code parameterObject}获取对应的对象
     *     </li>
     *     <li>
     *         判断如果该对象 instanceof {@link Boolean}，直接强转该对象为 {@link Boolean} 并返回，本方法结束；否则什么也不做，继续往下走
     *     </li>
     *     <li>
     *         如果如果该对象 instanceof {@link Number}：
     *         <ul>
     *             <li>
     *                 是则：return new {@link BigDecimal}(String.valueOf(value)).compareTo({@link BigDecimal#ZERO}) != 0;（即判断其是否不为0，是就返回true；否则返回false。本方法结束）
     *             </li>
     *             <li>
     *                 否则：什么也不做，继续往下走
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         来到这里，直接 return value != null;（即该对象为null则返回false，不为null则返回true。结束）
     *     </li>
     * </ol>
     *
     * @param expression OGNL表达式
     * @param parameterObject 表达式解析的对象，应该是{@link DynamicContext#bindings}
     * @return 是否为 true
     */
    public boolean evaluateBoolean(String expression, Object parameterObject) {
        Object value = OgnlCache.getValue(expression, parameterObject);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return new BigDecimal(String.valueOf(value)).compareTo(BigDecimal.ZERO) != 0;
        }
        return value != null;
    }

    /**
     * 获得OGNL表达式对应的集合，并做一些检查和转换
     * <ol>
     *     <li>
     *         调用{@link OgnlCache#getValue(String, Object)}传入{@code expression}和{@code parameterObject}获取对应的对象
     *     </li>
     *     <li>
     *         判断如果该对象 == null：true则抛出异常{@link BuilderException}；false则什么也不做，继续往下走
     *     </li>
     *     <li>
     *         判断如果该对象 instanceof {@link Iterable}：true则直接强转该对象为{@link Iterable}并返回；否则什么也不做，继续往下走
     *     </li>
     *     <li>
     *         判断如果该对象.getClass().isArray()（是一个数组）：
     *         <ul>
     *             <li>
     *                 true则：
     *                 <ol>
     *                     <li>
     *                         new 一个{@link ArrayList}对象
     *                     </li>
     *                     <li>
     *                         调用{@link Array#getLength(Object)}传入该对象，获取数组长度；调用{@link Array#get(Object, int)}传入该数组和对应的indes获取指定的数组元素。根据这两个api遍历数组中的元素并全部添加到
     *                         new出来的{@link ArrayList}中
     *                     </li>
     *                     <li>
     *                         return 该{@link ArrayList}对象，本方法结束
     *                     </li>
     *                     <li>
     *                         代码：
     *                   <pre>
     *             // the array may be primitive, so Arrays.asList() may throw
     *             // a ClassCastException (issue 209).  Do the work manually
     *             // Curse primitives! :) (JGB)
     *             int size = Array.getLength(value);
     *             List<{@link Object}> answer = new ArrayList<>();
     *             for (int i = 0; i < size; i++) {
     *                 Object o = Array.get(value, i);
     *                 answer.add(o);
     *             }
     *             return answer;
     *                 </pre>
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false则：什么也不做，继续往下走
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         判断该对象 instanceof {@link Map}：
     *         <ul>
     *             <li>
     *                 true则：强转该对象为{@link Map}然后直接返回{@link Map#entrySet()}，本方法结束
     *             </li>
     *             <li>
     *                 false则：什么也不做，继续往下走
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         来到这里直接抛出异常{@link BuilderException}
     *     </li>
     * </ol>
     *
     * @param expression OGNL表达式
     * @param parameterObject 表达式解析的对象，应该是{@link DynamicContext#bindings}
     * @return 迭代器对象
     */
    public Iterable<?> evaluateIterable(String expression, Object parameterObject) {
        Object value = OgnlCache.getValue(expression, parameterObject);
        if (value == null) {
            throw new BuilderException("The expression '" + expression + "' evaluated to a null value.");
        }
        if (value instanceof Iterable) {
            return (Iterable<?>) value;
        }
        if (value.getClass().isArray()) {
            // the array may be primitive, so Arrays.asList() may throw
            // a ClassCastException (issue 209).  Do the work manually
            // Curse primitives! :) (JGB)
            int size = Array.getLength(value);
            List<Object> answer = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                Object o = Array.get(value, i);
                answer.add(o);
            }
            return answer;
        }
        if (value instanceof Map) {
            return ((Map) value).entrySet();
        }
        throw new BuilderException("Error evaluating expression '" + expression + "'.  Return value (" + value + ") was not iterable.");
    }

}