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
package org.apache.ibatis.builder;

import java.util.HashMap;

/**
 *
 * Inline parameter expression parser. Supported grammar (simplified):
 *
 * <pre>
 * inline-parameter = (propertyName | expression) oldJdbcType attributes
 * propertyName = /expression language's property navigation path/
 * expression = '(' /expression language's expression/ ')'
 * oldJdbcType = ':' /any valid jdbc type/
 * attributes = (',' attribute)*
 * attribute = name '=' value
 * </pre>
 *
 * 解析后的参数表达式的缓存（"#{}"中的内容进行解析），最终解析之后得到的一个缓存内容为：
 * <ol>
 *     <li>
 *         <pre>
 * "expression"：el表达式部分 (expression)
 * "jdbcType"：jdbc type部分 （oldJdbcType）
 * "name1"："value1" （attributes）（name可以是"javaType,mode,numericScale,resultMap,typeHandler,jdbcTypeName"）
 * "name2"："value2" （attributes）（name可以是"javaType,mode,numericScale,resultMap,typeHandler,jdbcTypeName"）
 * ...
 *         </pre>
 *     </li>
 *     <li>
 *         <pre>
 * "property"：el表达式语法的属性导航路径部分 (propertyName)
 * "jdbcType"：jdbc type部分 （oldJdbcType）
 * "name1"："value1" （attributes）（name可以是"javaType,mode,numericScale,resultMap,typeHandler,jdbcTypeName"）
 * "name2"："value2" （attributes）（name可以是"javaType,mode,numericScale,resultMap,typeHandler,jdbcTypeName"）
 * ...
 *         </pre>
 *     </li>
 * </ol>
 *
 * @author Frank D. Martinez [mnesarco]
 */
public class ParameterExpression extends HashMap<String, String> {

    private static final long serialVersionUID = -2417552199605158680L;

    /**
     * 调用{@link #parse(String)}传入{@code expression}进行表达式解析
     *
     * @param expression  Token 字符串 （"#{}"中的内容）
     */
    public ParameterExpression(String expression) {
        parse(expression);
    }

    /**
     * 解析"#{}"中的表达式：
     * <ol>
     *     <li>
     *         调用{@link #skipWS(String, int)}传入{@code expression}和0，获取整个表达式中第一个非空字符的索引位置
     *     </li>
     *     <li>
     *         判断第一步得到的索引对应的字符是否是'(':
     *         <ul>
     *             <li>
     *                 是：认为当前{@code expression}定义的是一个el表达式的表达式（expression language's expression），调用{@link #expression(String, int)}传入"{@code expression}和第一步获得的索引值+1"进行解析
     *             </li>
     *             <li>
     *                 否：认为当前{@code expression}定义的是一个el表达式的属性访问路径（expression language's property navigation path），调用{@link #property(String, int)}传入"{@code expression}和第一步获得的索引值"进行解析
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param expression Token 字符串 （"#{}"中的内容）
     */
    private void parse(String expression) {
        int p = skipWS(expression, 0);
        if (expression.charAt(p) == '(') {
            expression(expression, p + 1);
        } else {
            property(expression, p);
        }
    }

    /**
     * 截取{@code expression}中的"expression"部分并put到缓存中，然后再调用{@link #jdbcTypeOpt(String, int)}解析剩余部分：
     * <ol>
     *     <li>
     *         初始化"match"左圆括号计数器为1、当前迭代的字符在{@code expression}中的索引"right"为{@code left}+1
     *     </li>
     *     <li>
     *         开始一个循环迭代【循环条件是"match"大于0（就是还有左圆括号没有匹配到右圆括号）】，对于每一轮迭代（注意：本方法中根据索引获取字符串对应的字符{@link String#charAt(int)}的时候没有校验是否索引超长，会抛出底层的异常）：
     *         <ol>
     *             <li>
     *                 判断当前索引"right"对应的字符是否为')'
     *                 <ul>
     *                     <li>
     *                         是："match"自减1，表示有一个左圆括号匹配到了右圆括号，然后"right"自增1，进入下一循环
     *                     </li>
     *                     <li>
     *                         否：判断当前索引"right"对应的字符是否为'('：
     *                         <ul>
     *                             <li>
     *                                 是："match"自增1，表示增加一个没有匹配到右圆括号的左圆括号，然后"right"自增1，进入下一循环
     *                             </li>
     *                             <li>
     *                                 否：什么也不做，然后"right"自增1，进入下一循环
     *                             </li>
     *                         </ul>
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         调用{@link #put(Object, Object)}传入"expression"和{@code expression}.substring({@code left}, "right" - 1)，将截取出来的"el表达式"部分设置到缓存中
     *     </li>
     *     <li>
     *         调用{@link #jdbcTypeOpt(String, int)}传入{@code expression}和"right"
     *     </li>
     * </ol>
     *
     * @param expression Token 字符串 （"#{}"中的内容）
     * @param left {@code expression}中第一个非空白字符的索引
     */
    private void expression(String expression, int left) {
        int match = 1;
        int right = left + 1;
        while (match > 0) {
            if (expression.charAt(right) == ')') {
                match--;
            } else if (expression.charAt(right) == '(') {
                match++;
            }
            right++;
        }
        put("expression", expression.substring(left, right - 1));
        jdbcTypeOpt(expression, right);
    }

    /**
     * 截取{@code expression}中的"propertyName"部分并put到缓存中，然后再调用{@link #jdbcTypeOpt(String, int)}解析剩余部分：
     * <ul>
     *     判断{@code left}小于{@code expression}.length()
     *     <li>
     *         是：
     *         <ol>
     *             <li>
     *                 调用{@link #skipUntil(String, int, String)}传入{@code expression}、{@code left}、",:" 获取"propertyName"部分字符串的最后一个字符索引值"right"
     *             </li>
     *             <li>
     *                 先调用{@link #trimmedStr(String, int, int)}传入{@code expression}、{@code left}、前一步得到的索引值"right"对{@code expression}中的"propertyName"部分子字符串进行截取和trim，然后调用{@link #put(Object, Object)}传入"property"和前面得到的trim之后的"propertyName"部分字符串设置到缓存中
     *             </li>
     *             <li>
     *                 调用{@link #jdbcTypeOpt(String, int)}传入{@code expression}和前面得到的索引值"right"对剩余字符串进行解析
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         否：什么也不做，本方法结束
     *     </li>
     * </ul>
     *
     * @param expression Token 字符串 （"#{}"中的内容）
     * @param left {@code expression}中第一个非空白字符的索引
     */
    private void property(String expression, int left) {
        if (left < expression.length()) {
            int right = skipUntil(expression, left, ",:");
            put("property", trimmedStr(expression, left, right));
            jdbcTypeOpt(expression, right);
        }
    }

    /**
     * 从一个字符串的某一段开始定位第一个非空白字符的索引位置：
     * <ul>
     *     从{@code expression}的{@code p}索引对应的字符开始遍历迭代字符串{@code expression}后面的每一个字符，判断当前迭代的字符是否 > 0x20（0x20：16进制下的20对应10禁止的32，在ASCII码表中0到32都是空白字符，可以认为大于32都是非空白字符）
     *     <li>
     *         true：返回当前迭代的字符索引
     *     </li>
     *     <li>
     *         false：进入循环的下一迭代，直到循环结束，则返回{@code expression}.length()
     *     </li>
     * </ul>
     *
     * @param expression 要操作的字符串
     * @param p 从{@code expression}的第几位开始进行空白符定位
     * @return 第一个非空白字符在 {@code expression} 中的索引位置
     */
    private int skipWS(String expression, int p) {
        for (int i = p; i < expression.length(); i++) {
            if (expression.charAt(i) > 0x20) {
                return i;
            }
        }
        return expression.length();
    }

    /**
     * 从一个字符串的某一段开始定位剩余字符串部分遇到第一个{@code endChars}字符串中包含的任意一个字符之前的一个字符对应的索引（以{@code endChars}中任意字符作为结束字符对字符串{@code expression}进行截取）：
     * <ol>
     *     从{@code expression}的{@code p}索引对应的字符开始遍历迭代字符串{@code expression}后面的每一个字符，判断当前迭代的字符是否被包含在{@code endChars}中（调用{@code endChars}的{@link String#indexOf(int)}传入当前迭代的字符，看返回的结果是否大于-1）
     *     <ul>
     *         <li>
     *             是：返回当前迭代的字符在{@code expression}中的索引值
     *         </li>
     *         <li>
     *             否：进入循环的下一迭代，直到循环结束，则返回{@code expression}.length()
     *         </li>
     *     </ul>
     * </ol>
     *
     * @param expression 要操作的字符串
     * @param p 从{@code expression}的第几位开始进行{@code expression}要截取的子字符串的结束索引定位
     * @param endChars 结束字符的集合
     * @return
     */
    private int skipUntil(String expression, int p, final String endChars) {
        for (int i = p; i < expression.length(); i++) {
            char c = expression.charAt(i);
            if (endChars.indexOf(c) > -1) {
                return i;
            }
        }
        return expression.length();
    }

    /**
     * 解析{@code expression}中除了"propertyName"或者"expression"之外剩余的部分（"oldJdbcType"和"attributes"部分）设置到缓存中
     * <ol>
     *     <li>
     *         调用{@link #skipWS(String, int)}传入{@code expression}和{@code p}获得第一个非空字符的索引
     *     </li>
     *     <li>
     *         如果第一步获取到的索引值小于{@code expression}.length()（还在字符串范围内）
     *         <ul>
     *             <li>
     *                 true：判断第一步获取到的索引值对应的字符是否为":"：
     *                 <ul>
     *                     <li>
     *                         是：调用{@link #jdbcType(String, int)}传入{@code expression}和第一步获得的索引值+1进行解析
     *                     </li>
     *                     <li>
     *                         否：判断第一步获取到的索引值对应的字符是否为","：
     *                         <ul>
     *                             <li>
     *                                 是：调用{@link #option(String, int)} 传入{@code expression}和第一步获得的索引值+1进行解析
     *                             </li>
     *                             <li>
     *                                 否：抛出异常{@link BuilderException}
     *                             </li>
     *                         </ul>
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 false：什么也不做，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param expression Token 字符串 （"#{}"中的内容）
     * @param p {@code expression}中截取了"propertyName"或者"expression"之后的第一个字符的索引
     */
    private void jdbcTypeOpt(String expression, int p) {
        p = skipWS(expression, p);
        if (p < expression.length()) {
            if (expression.charAt(p) == ':') {
                jdbcType(expression, p + 1);
            } else if (expression.charAt(p) == ',') {
                option(expression, p + 1);
            } else {
                throw new BuilderException("Parsing error in {" + expression + "} in position " + p);
            }
        }
    }

    /**
     * <ol>
     *     尝试截取{@code expression}中"oldJdbcType"部分字符串（"propertyName"或者"expression"之外剩下字符串中字符':'和','之间的字符串）put到缓存中
     *     <li>
     *         调用{@link #skipWS(String, int)}传入{@code expression}和{@code p}获取第一个非空字符的索引值"left"
     *     </li>
     *     <li>
     *         调用{@link #skipUntil(String, int, String)}传入{@code expression}、第一步获取的第一个非空字符的索引值、"," 三个参数获取{@code expressiono}中除去"el表达式"剩下的后面部分字符串中，位于第一个':'字符之后的第一个非空白字符之后的','字符之前的一个字符的索引值"right"
     *     </li>
     *     <li>
     *         判断前一步获得的','字符的前一个字符的索引是否大于第一步获得的第一个非空白字符的索引：
     *         <ul>
     *             <li>
     *                 是：调用{@link #trimmedStr(String, int, int)}传入{@code expression}、第一步得到的索引值"left"、第二步得到的索引值"end" 三个参数对':'和','之间的字符串进行trim操作，调用{@link #put(Object, Object)}传入"jdbcType"和trim之后的{@code expression}的子字符串进行缓存设置，然后继续往下走
     *             </li>
     *             <li>
     *                 否：抛出异常{@link BuilderException}，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link #option(String, int)}传入{@code expression}和第二步得到的索引值"right"+1对剩余的字符串进行解析
     *     </li>
     * </ol>
     *
     * @param expression Token 字符串 （"#{}"中的内容）
     * @param p {@code expression}中截取了"propertyName"或者"expression"之后剩下的字符串中":"字符后面的第一个字符的索引值
     */
    private void jdbcType(String expression, int p) {
        int left = skipWS(expression, p);
        int right = skipUntil(expression, left, ",");
        if (right > left) {
            put("jdbcType", trimmedStr(expression, left, right));
        } else {
            throw new BuilderException("Parsing error in {" + expression + "} in position " + p);
        }
        option(expression, right + 1);
    }

    /**
     * <ol>
     *     尝试截取{@code expression}中"attributes"部分字符串【（"propertyName"或者"expression"）和"oldJdbcType"部分之外剩下字符串部分，以','开头，'='隔开"name"和"value"】设置到缓存中
     *     <li>
     *         调用{@link #skipWS(String, int)}传入{@code expression}和{@code p}获取第一个非空白字符索引值"left"
     *     </li>
     *     <li>
     *         判断第一步得到的索引值小于{@code expression}.length()
     *         <ul>
     *             <li>
     *                 是：
     *                 <ol>
     *                     <li>
     *                         调用{@link #skipUntil(String, int, String)}传入{@code expression}、第一步获取到的索引值"left"、"="获取"name"部分（','和'='之间的子字符串）字符串的结尾索引"right"
     *                     </li>
     *                     <li>
     *                         调用{@link #trimmedStr(String, int, int)}传入{@code expression}、第一步得到的索引"left"、前一步得到的索引"right"对{@code expression}进行子字符串截取和trim得到当前"attribute"的"name"部分
     *                     </li>
     *                     <li>
     *                         "right"+1赋值到"left"得到"value"部分（'='和','之间的子字符串）字符串的起始索引，再调用{@link #skipUntil(String, int, String)}传入{@code expression}、前面赋值之后的"left"、","获取"value"部分字符串的结束索引
     *                     </li>
     *                     <li>
     *                         调用{@link #trimmedStr(String, int, int)}传入{@code expression}、上面一步得到的索引"left"、上面一步得到的索引"right"对{@code expression}进行子字符串截取和trim得到当前"attribute"的"value"部分
     *                     </li>
     *                     <li>
     *                         调用{@link #put(Object, Object)}传入经过以上步骤得到的{@code expression}中"attributes"部分当前"attribute"中的"name'和"value"部分字符串设置到缓存中
     *                     </li>
     *                     <li>
     *                         递归调用本方法{@link #option(String, int)}传入{@code expression}和"right"+1，进行"attributes"剩余部分的解析
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 否：什么也不做，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param expression Token 字符串 （"#{}"中的内容）
     * @param p {@code expression}中截取了"propertyName"或者"expression"之后剩下的字符串中":"字符后面的第一个字符的索引值
     */
    private void option(String expression, int p) {
        int left = skipWS(expression, p);
        if (left < expression.length()) {
            int right = skipUntil(expression, left, "=");
            String name = trimmedStr(expression, left, right);
            left = right + 1;
            right = skipUntil(expression, left, ",");
            String value = trimmedStr(expression, left, right);
            put(name, value);
            option(expression, right + 1);
        }
    }

    /**
     * 对于传入的字符串{@code str}的{@code start}索引和{@code end}索引之间的子字符串（包头不包尾）进行手动trim然后返回trim之后的子字符串：
     * <ol>
     *     <li>
     *         从索引{@code start}对应的字符开始进行循环迭代，循环条件是判断该字符是否小于等于"0x20"（是空白字符）
     *         <ul>
     *             <li>
     *                 是则：{@code start}自增1，进入下一循环迭代（过滤空白字符对应的索引）
     *             </li>
     *             <li>
     *                 否则：终止循环，继续往下走（记录正向往后推第一个非空白字符的索引）
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         从索引{@code end}-1对应的字符开始进行循环迭代，循环条件是判断该字符是否小于等于"0x20"（是空白字符）
     *         <ul>
     *             <li>
     *                 是则：{@code end}自减1，进入下一循环迭代（过滤空白字符对应的索引）
     *             </li>
     *             <li>
     *                 否则：终止循环，继续往下走（记录逆向往前推第一个非空白字符的索引）
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         判断第一步记录的索引是否大于等于第二步记录的索引（起始索引大于等于结束索引）：
     *         <ul>
     *             <li>
     *                 是：return 空字符串 ""，本方法结束
     *             </li>
     *             <li>
     *                 否：return {@code str}.sunstring(第一步记录的索引，第二步记录的索引)，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param str 要trim的字符串
     * @param start 要从字符串的哪一位开始trim操作（子字符串的开始索引）
     * @param end 要从字符串的哪一位结束trim操作（子字符串的结束索引）
     * @return
     */
    private String trimmedStr(String str, int start, int end) {
        while (str.charAt(start) <= 0x20) {
            start++;
        }
        while (str.charAt(end - 1) <= 0x20) {
            end--;
        }
        return start >= end ? "" : str.substring(start, end);
    }

}
