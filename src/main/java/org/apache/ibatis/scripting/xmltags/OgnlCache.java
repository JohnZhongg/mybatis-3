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

import ognl.*;
import org.apache.ibatis.builder.BuilderException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Caches OGNL parsed expressions.
 *
 * OGNL 缓存类
 *
 * @author Eduardo Macarron
 *
 * @see <a href='http://code.google.com/p/mybatis/issues/detail?id=342'>Issue 342</a>
 */
public final class OgnlCache {

    /**
     * {@link OgnlMemberAccess} 单例
     */
    private static final OgnlMemberAccess MEMBER_ACCESS = new OgnlMemberAccess();
    /**
     * OgnlClassResolver 单例
     */
    private static final OgnlClassResolver CLASS_RESOLVER = new OgnlClassResolver();

    /**
     * <ul>
     *     表达式的缓存的映射
     *     <li>
     *         KEY：OGNL 表达式
     *     </li>
     *     <li>
     *         VALUE：OGNL表达式编译之后的对象的缓存（{@link Ognl#parseExpression(String)}）
     *     </li>
     * </ul>
     *
     */
    private static final Map<String, Object> expressionCache = new ConcurrentHashMap<>();

    private OgnlCache() {
        // Prevent Instantiation of Static Class
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link Ognl#createDefaultContext(Object, MemberAccess, ClassResolver, TypeConverter)}传入 {@code root}、{@link #MEMBER_ACCESS}、{@link #CLASS_RESOLVER}、null 四个参数获取一个OGNL命名上下文{@link Map}对象  <b>（其实这个上下文对象在OGNL中就相当于myabtis里面的变量property，使其支持变量设置，但是mybatis这里创建的上下文其实并没有实际用途，参考{@link DynamicContext.ContextAccessor#getProperty(Map, Object, Object)}。个人理解是mybatis本身引入OGNL就是为了解决sql中的变量，所以，禁止套娃 ^_^）</b>
     *     </li>
     *     <li>
     *         调用{@link #parseExpression(String)}获取表达式解释之后得到的一个树形对象，然后调用 {@link Ognl#getValue(Object, Map, Object)}传入获得的树形对象、第一步获得的OGNL命名上下文、{@code root} 三个参数获取对应的结果并返回该结果，本方法结束
     *     </li>
     * </ol>
     *
     * @param expression OGNL表达式
     * @param root 两个用途：1、创建本次OGNL解析的上下文设置该对象为这个上下文的root；2、设置为本次OGNL解析的root对象
     * @return
     */
    public static Object getValue(String expression, Object root) {
        try {
            Map context = Ognl.createDefaultContext(root, MEMBER_ACCESS, CLASS_RESOLVER, null);
            return Ognl.getValue(parseExpression(expression), context, root);
        } catch (OgnlException e) {
            throw new BuilderException("Error evaluating expression '" + expression + "'. Cause: " + e, e);
        }
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link #expressionCache}的{@link ConcurrentHashMap#get(Object)}传入{@code expresstion}获取对应的缓存对象
     *     </li>
     *     <li>
     *         判断第一步返回的结果是否为null：
     *         <ul>
     *             <li>
     *                 为null：调用{@link Ognl#parseExpression(String)}传入{@code expression}得到编译之后的结果对象，并调用{@link #expressionCache}的{@link ConcurrentHashMap#put(Object, Object)}传入{@code expresstion}和获取的结果设置缓存，然后返回该结果，本方法结束
     *             </li>
     *             <li>
     *                 不为null：直接返回第一步获取到的结果，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param expression 要解析的OGNL表达式
     * @return
     * @throws OgnlException
     */
    private static Object parseExpression(String expression) throws OgnlException {
        Object node = expressionCache.get(expression);
        if (node == null) {
            node = Ognl.parseExpression(expression);
            expressionCache.put(expression, node);
        }
        return node;
    }

}