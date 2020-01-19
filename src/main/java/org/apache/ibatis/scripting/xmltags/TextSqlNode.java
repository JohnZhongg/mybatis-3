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

import org.apache.ibatis.parsing.GenericTokenParser;
import org.apache.ibatis.parsing.TokenHandler;
import org.apache.ibatis.scripting.ScriptingException;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.SimpleTypeRegistry;

import java.util.regex.Pattern;

/**
 * 文本节点对象的 {@link SqlNode} 实现类。
 *
 * @author Clinton Begin
 */
public class TextSqlNode implements SqlNode {

    /**
     * 文本内容
     */
    private final String text;
    /**
     * 目前该属性只在单元测试中使用，暂时无视（用于监测动态sql${}中是否存在不想要的内容：如sql注入监测等。参考{@link BindingTokenParser#checkInjection(java.lang.String)}）
     */
    private final Pattern injectionFilter;

    /**
     * 调用{@link #TextSqlNode(String, Pattern)}传入{@code text}和null分别设到{@link #text}和{@link #injectionFilter}
     *
     * @param text
     */
    public TextSqlNode(String text) {
        this(text, null);
    }

    /**
     * 设置{@code text}到{@link #text}、{@code injectionFilter}到{@link #injectionFilter}
     *
     * @param text
     * @param injectionFilter
     */
    public TextSqlNode(String text, Pattern injectionFilter) {
        this.text = text;
        this.injectionFilter = injectionFilter;
    }

    /**
     * <ol>
     *     <li>
     *         new 一个{@link DynamicCheckerTokenParser}对象
     *     </li>
     *     <li>
     *         调用{@link #createParser(TokenHandler)}传入第一步获得的token处理器构建一个token解析器返回（token为"${}"、识别token的动作参考{@link GenericTokenParser#parse(String)}、处理token的动作参考{@link DynamicCheckerTokenParser#handleToken(String)}）
     *     </li>
     *     <li>
     *         调用第二步获得的token解析器的{@link GenericTokenParser#parse(String)}进行token解析处理，如果识别到了token就会设置第一步的{@link DynamicCheckerTokenParser#isDynamic}为true
     *     </li>
     *     <li>
     *         返回{@link DynamicCheckerTokenParser#isDynamic()}
     *     </li>
     * </ol>
     *
     * @return
     */
    public boolean isDynamic() {
        DynamicCheckerTokenParser checker = new DynamicCheckerTokenParser();
        GenericTokenParser parser = createParser(checker);
        parser.parse(text);
        return checker.isDynamic();
    }

    /**
     * <ol>
     *     <li>
     *         调用{@link BindingTokenParser#BindingTokenParser(DynamicContext, Pattern)}传入{@code context}和{@link #injectionFilter}构建一个{@link BindingTokenParser}对象
     *     </li>
     *     <li>
     *         调用{@link #createParser(TokenHandler)}传入第一步构建的{@link BindingTokenParser}对象组合其"处理token"的业务（{@link BindingTokenParser#handleToken(String)}）到一个新建的{@link GenericTokenParser#parse(String)}对象中，然后返回该对象
     *     </li>
     *     <li>
     *         调用前一步得到的{@link GenericTokenParser}对象的{@link GenericTokenParser#parse(String)}传入{@link #text}对文本进行解析
     *     </li>
     *     <li>
     *         调用{@code context}的{@link DynamicContext#appendSql(String)}拼接解析之后的文本（sql片段）到{@link DynamicContext#sqlBuilder}，然后返回true
     *     </li>
     * </ol>
     *
     * @param context 上下文
     * @return
     */
    @Override
    public boolean apply(DynamicContext context) {
        GenericTokenParser parser = createParser(new BindingTokenParser(context, injectionFilter));
        context.appendSql(parser.parse(text));
        return true;
    }

    /**
     * 调用构造器{@link GenericTokenParser#GenericTokenParser(String, String, TokenHandler)}传入token的open部分"${"和close部分"}"以及{@code handler}构建成一个token解析器（包括token的识别和处理）<u><b>（同时注意：这里的动态sql定义的token是"${}"，不包含"#{}"）</b></u>，
     * 其中token解析器的识别token的动作在{@link GenericTokenParser#parse(String)}中通过hard code实现了，而处理token的部分则通过组合的方式通过{@link GenericTokenParser#handler}的{@link TokenHandler#handleToken(String)}来实现
     *
     * @param handler Token处理器实现类
     * @return
     */
    private GenericTokenParser createParser(TokenHandler handler) {
        return new GenericTokenParser("${", "}", handler);
    }

    /**
     * 内部的{@link TokenHandler}（token处理器）实现类（）
     */
    private static class BindingTokenParser implements TokenHandler {

        /**
         * 当前sql的上下文
         */
        private DynamicContext context;
        /**
         * sql中不能存在的内容
         */
        private Pattern injectionFilter;

        /**
         * {@link #context} = {@code context} <br>
         * {@link #injectionFilter} = {@code injectionFilter}
         *
         * @param context 赋值到{@link #context}
         * @param injectionFilter 赋值到{@link #injectionFilter}
         */
        public BindingTokenParser(DynamicContext context, Pattern injectionFilter) {
            this.context = context;
            this.injectionFilter = injectionFilter;
        }

        /**
         * 识别到Token之后调用本方法并传入Token：
         * <ol>
         *     <li>
         *         从当前的sql上下文{@link #context}中获取其参数上下文{@link DynamicContext#getBindings()}，然后调用 {@link org.apache.ibatis.scripting.xmltags.DynamicContext.ContextMap#get(Object)}传入"_parameter" 尝试从参数上下文中获取承载了各参数值的对象（参考{@link DynamicContext#DynamicContext(Configuration, Object)}的第二步）
         *     </li>
         *     <li>
         *         判断上一步获取的承载了参数值的对象是否为null：
         *         <ul>
         *             <li>
         *                 为null：调用{@link #context}的{@link DynamicContext#getBindings()}然后调用{@link org.apache.ibatis.scripting.xmltags.DynamicContext.ContextMap#put(Object, Object)}传入"value"和null
         *             </li>
         *             <li>
         *                 不为null：判断{@link SimpleTypeRegistry#isSimpleType(Class)}传入"第一步获取的承载了参数值的对象.getClass()"的结果是否为true
         *                 <ul>
         *                     <li>
         *                         为true：调用{@link #context}的{@link DynamicContext#getBindings()}然后调用{@link org.apache.ibatis.scripting.xmltags.DynamicContext.ContextMap#put(Object, Object)}传入"value"和第一步获取的承载了参数值的对象
         *                     </li>
         *                     <li>
         *                         为false：什么也不做往下走
         *                     </li>
         *                 </ul>
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         调用{@link OgnlCache#getValue(String, Object)}传入{@code content}、{@link #context}的{@link DynamicContext.ContextMap#getBindings()}获得该Token对应的OGNL表达式的对应的对象
         *     </li>
         *     <li>
         *         判断上一步获得对象是否为null，是则获得空字符串""；否则调用{@link String#valueOf(Object)}传入获得的对象得到对应的字符串
         *     </li>
         *     <li>
         *         调用{@link #checkInjection(String)}传入上一步获得的字符串检查是否包含不规范的内容
         *     </li>
         *     <li>
         *         返回第四步获得字符串
         *     </li>
         * </ol>
         *
         * @param content Token 字符串（一个OGNL表达式）
         * @return
         */
        @Override
        public String handleToken(String content) {
            Object parameter = context.getBindings().get("_parameter");
            if (parameter == null) {
                context.getBindings().put("value", null);
            } else if (SimpleTypeRegistry.isSimpleType(parameter.getClass())) {
                context.getBindings().put("value", parameter);
            }
            Object value = OgnlCache.getValue(content, context.getBindings());
            String srtValue = (value == null ? "" : String.valueOf(value)); // issue #274 return "" instead of "null"
            checkInjection(srtValue);
            return srtValue;
        }

        /**
         * 检查传入的字符串{@code value}是否不包含{@link #injectionFilter}：
         * <pre>
         *             if ({@link #injectionFilter} != null && !{@link #injectionFilter}.matcher(value).matches()) {
         *                 throw new {@link ScriptingException}("Invalid input. Please conform to regex" + {@link #injectionFilter}.pattern());
         *             }
         * </pre>
         * @param value
         */
        private void checkInjection(String value) {
            if (injectionFilter != null && !injectionFilter.matcher(value).matches()) {
                throw new ScriptingException("Invalid input. Please conform to regex" + injectionFilter.pattern());
            }
        }
    }

    /**
     * 内部的{@link TokenHandler}（token处理器）实现类（对携带token的sql进行标识）
     */
    private static class DynamicCheckerTokenParser implements TokenHandler {

        /**
         * 是否为动态文本
         */
        private boolean isDynamic;

        public DynamicCheckerTokenParser() {
            // Prevent Synthetic Access
        }

        /**
         * @return {@link #isDynamic}
         */
        public boolean isDynamic() {
            return isDynamic;
        }

        /**
         * 识别到Token之后调用本方法并传入Token，说明肯定存在Token，即为动态sql，设置{@link #isDynamic}为true
         *
         * @param content Token 字符串
         * @return
         */
        @Override
        public String handleToken(String content) {
            this.isDynamic = true;
            return null;
        }
    }

}