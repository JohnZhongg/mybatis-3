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

import ognl.MemberAccess;
import org.apache.ibatis.reflection.Reflector;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Member;
import java.util.Map;

/**
 * The {@link MemberAccess} class that based on <a href=
 * 'https://github.com/jkuhnert/ognl/blob/OGNL_3_2_1/src/java/ognl/DefaultMemberAccess.java'>DefaultMemberAccess</a>. <br>
 *
 * OGNL 成员访问器{@link MemberAccess}实现类
 *
 * @author Kazuki Shimizu
 * @since 3.5.0
 *
 * @see <a href=
 *      'https://github.com/jkuhnert/ognl/blob/OGNL_3_2_1/src/java/ognl/DefaultMemberAccess.java'>DefaultMemberAccess</a>
 * @see <a href='https://github.com/jkuhnert/ognl/issues/47'>#47 of ognl</a>
 */
class OgnlMemberAccess implements MemberAccess {

    /**
     * 是否可以修改成员的可访问，参考无参构造{@link OgnlMemberAccess#OgnlMemberAccess()}
     */
    private final boolean canControlMemberAccessible;

    /**
     * 调用{@link Reflector#canControlMemberAccessible()}检测当前jvm是否配置了限制通过反射访问private对象的权限，并赋值到{@link #canControlMemberAccessible}
     */
    OgnlMemberAccess() {
        this.canControlMemberAccessible = Reflector.canControlMemberAccessible();
    }

    /**
     * Sets the member up for accessibility <br>
     * 设置成员{@code member}为可访问的：
     * <ul>
     *     调用{@link #isAccessible(Map, Object, Member, String)}传入{@code context}、{@code target}、{@code member}、{@code propertyName}判断结果是否为true
     *     <li>
     *         为true：强转{@code member}为{@link AccessibleObject}使其可以调用相关api
     *         <ul>
     *             调用{@link AccessibleObject#isAccessible()}判断{@code member}本来是否是可访问的：
     *             <li>
     *                 是则：什么也不做，直接return null，本方法结束
     *             </li>
     *             <li>
     *                 否则：调用{@code member}的{@link AccessibleObject#setAccessible(boolean)}传入true设置其为可访问的，返回{@link Boolean#FALSE}，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         否则：return null
     *     </li>
     * </ul>
     *
     * @param context ognl上下文
     * @param target 要获取其属性值的对象
     * @param member 该属性名称对应的成员（{@link java.lang.reflect.Field}或者{@link java.lang.reflect.Method}）
     * @param propertyName 要获取的属性名
     * @return 该返回值将在调用{@link #restore(Map, Object, Member, String, Object)}的时候传递给的最后一个参数
     */
    @Override
    public Object setup(Map context, Object target, Member member, String propertyName) {
        Object result = null;
        if (isAccessible(context, target, member, propertyName)) {
            AccessibleObject accessible = (AccessibleObject) member;
            if (!accessible.isAccessible()) {
                result = Boolean.FALSE;
                accessible.setAccessible(true);
            }
        }
        return result;
    }

    /**
     * Restores the member from the previous setup call. （重置最近一次{@link #setup(Map, Object, Member, String)}的操作）<br>
     * <pre>
     *     if (state != null) {
     *             ((AccessibleObject) member).setAccessible(((Boolean) state));
     *     }
     * </pre>
     *
     * @param context ognl上下文
     * @param target 要获取其属性值的对象
     * @param member 该属性名称对应的成员（{@link java.lang.reflect.Field}或者{@link java.lang.reflect.Method}）
     * @param propertyName 要获取的属性名
     * @param state 最近一次调用{@link #setup(Map, Object, Member, String)}的返回值
     */
    @Override
    public void restore(Map context, Object target, Member member, String propertyName,
                        Object state) {
        if (state != null) {
            ((AccessibleObject) member).setAccessible(((Boolean) state));
        }
    }

    /**
     * Returns true if the given member is accessible or can be made accessible by this object.：<br>
     * return {@link #canControlMemberAccessible}
     * @param context ognl上下文
     * @param target 要获取其属性值的对象
     * @param member 该属性名称对应的成员（{@link java.lang.reflect.Field}或者{@link java.lang.reflect.Method}）
     * @param propertyName 要获取的属性名
     * @return {@link #canControlMemberAccessible}
     */
    @Override
    public boolean isAccessible(Map context, Object target, Member member, String propertyName) {
        return canControlMemberAccessible;
    }

}
