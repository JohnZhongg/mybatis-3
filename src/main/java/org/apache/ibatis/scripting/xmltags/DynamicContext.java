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

import ognl.OgnlContext;
import ognl.OgnlException;
import ognl.OgnlRuntime;
import ognl.PropertyAccessor;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.session.Configuration;

import java.util.HashMap;
import java.util.Map;

/**
 * SQL的动态的上下文，主要用来在每一次对于sql的节点【各sql节点主要分三大类：1、mybatis中的各种xml标签节点（如{@code <forEach/>}这些）；2、携带"${}"token的文本节点；3、不含"${}"token的普通文本。】进行解析之后得到的当前sql字符串（{@link DynamicContext#sqlBuilder}）<br>
 * 另外就是记录当前sql的参数上下文（{@link DynamicContext#bindings}）
 *
 * @author Clinton Begin
 */
public class DynamicContext {


    static {
        //给所有ContextMap.class类型的对象注册一个全局属性访问器
        OgnlRuntime.setPropertyAccessor(ContextMap.class, new ContextAccessor());
    }

    /**
     * {@link #bindings} 的一个固定键值对的key："_parameter"；value是一个承载了所有参数值的对象（一个JavaBean或者Map，也可能是null。不是{@link MetaObject}对象）
     */
    public static final String PARAMETER_OBJECT_KEY = "_parameter";
    /**
     * {@link #bindings} 的一个固定键值对的key："_databaseId"；value是{@link Configuration#getDatabaseId()}
     */
    public static final String DATABASE_ID_KEY = "_databaseId";

    /**
     * 当前sql参数的上下文：
     * <ol>
     *     property:
     *     <li>
     *         parameterMetaObject::承载了sql参数的对象的{@link MetaObject}对象（当_parameter是JavaBan的时候有值；_parameter是Map、null则为null）
     *     </li>
     * </ol>
     * <ol>
     *     key-value
     *     <li>
     *         _parameter::承载了sql参数的对象（可以是JavaBean、Map、null）
     *     </li>
     *     <li>
     *         value::key"_parameter"对应的value（当"_parameter"对应的value是null或者是简单的类型的时候才有当前键值对{@link org.apache.ibatis.type.SimpleTypeRegistry#SIMPLE_TYPE_SET}）
     *     </li>
     *     <li>
     *         _databaseId::{@link Configuration#getDatabaseId()}
     *     </li>
     * </ol>
     *
     */
    private final ContextMap bindings;
    /**
     * 最近一次解析之后得到的sql
     */
    private final StringBuilder sqlBuilder = new StringBuilder();
    /**
     * 唯一编号。在 {@link org.apache.ibatis.scripting.xmltags.XMLScriptBuilder.ForEachHandler} 使用。在{@code <foreach/>}标签的循环过程中，
     * 每一次迭代拼接sql都有自己的sql上下文空间，但是同时可以使用比自己大的上下文空间
     */
    private int uniqueNumber = 0;

    /**
     * <ol>
     *     <li>
     *         判断 {@code parameterObject} != null 并且  !({@code parameterObject} instanceof {@link Map})
     *         <ul>
     *             <li>
     *                 为true：调用{@code configuration}的{@link Configuration#newMetaObject(Object)}传入{@code parameterObject}获取承载参数的对象的{@link MetaObject}对象；然后调用{@link ContextMap#ContextMap(MetaObject)}传入前面获得的
     *                 {@link MetaObject}对象实例化一个{@link ContextMap}对象并赋值到{@link #bindings}
     *             </li>
     *             <li>
     *                 为false：直接调用{@link ContextMap#ContextMap(MetaObject)}传入null实例化一个{@link ContextMap}对象并赋值到{@link #bindings}
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link #bindings}的{@link ContextMap#put(Object, Object)}传入{@link #PARAMETER_OBJECT_KEY}和{@code parameterObject}
     *     </li>
     *     <li>
     *         调用{@link #bindings}的{@link ContextMap#put(Object, Object)}传入{@link #DATABASE_ID_KEY}和{@link Configuration#getDatabaseId()}
     *     </li>
     * </ol>
     *
     * @param configuration 全局{@link Configuration}对象
     * @param parameterObject 一个承载了当前sql参数的对象
     */
    public DynamicContext(Configuration configuration, Object parameterObject) {
        if (parameterObject != null && !(parameterObject instanceof Map)) {
            MetaObject metaObject = configuration.newMetaObject(parameterObject);
            bindings = new ContextMap(metaObject);
        } else {
            bindings = new ContextMap(null);
        }
        bindings.put(PARAMETER_OBJECT_KEY, parameterObject);
        bindings.put(DATABASE_ID_KEY, configuration.getDatabaseId());
    }

    /**
     * @return {@link #bindings}
     */
    public Map<String, Object> getBindings() {
        return bindings;
    }

    /**
     * 调用{@link #bindings}.put({@code name}, {@code value})
     *
     * @param name
     * @param value
     */
    public void bind(String name, Object value) {
        bindings.put(name, value);
    }

    /**
     * <pre>
     *     {@link #sqlBuilder}.append({@code sql});
     *     {@link #sqlBuilder}.append(" ");</pre>
     * @param sql
     */
    public void appendSql(String sql) {
        sqlBuilder.append(sql);
        sqlBuilder.append(" ");
    }

    /**
     * @return {@link #sqlBuilder}.toString().trim()
     */
    public String getSql() {
        return sqlBuilder.toString().trim();
    }

    /**
     * @return {@link #uniqueNumber}++ （返回当前{@link #uniqueNumber}的值然后自增1）
     */
    public int getUniqueNumber() {
        return uniqueNumber++;
    }

    /**
     * 具有缓存能力的参数上下文
     */
    static class ContextMap extends HashMap<String, Object> {

        private static final long serialVersionUID = 2977601501966151582L;

        /**
         * parameter 对应的 {@link MetaObject} 对象
         */
        private MetaObject parameterMetaObject;

        public ContextMap(MetaObject parameterMetaObject) {
            this.parameterMetaObject = parameterMetaObject;
        }

        /**
         * <ol>
         *     <li>
         *         将入参{@code key}强转为{@link String}类型并赋值引用给一个新的变量"strKey"
         *     </li>
         *     <li>
         *         判断super.containsKey("strKey")：如果为true，则直接返回super.get("strKey")，本方法结束；如果为false，在什么也不做继续往下走。（缓存）
         *     </li>
         *     <li>
         *         判断{@link #parameterMetaObject}是否为null：为null在什么也不做，直接返回null，本方法结束；不为null则调用 {@link #parameterMetaObject}的{@link MetaObject#getValue(String)}传入"strKey"并直接return 其结果，本方法结束。
         *     </li>
         * </ol>
         *
         * @param key
         * @return
         */
        @Override
        public Object get(Object key) {
            String strKey = (String) key;
            if (super.containsKey(strKey)) {
                return super.get(strKey);
            }
            if (parameterMetaObject != null) {
                // issue #61 do not modify the context when reading
                return parameterMetaObject.getValue(strKey);
            }

            return null;
        }
    }

    static class ContextAccessor implements PropertyAccessor {


        /**
         * Extracts and returns the property of the given name from the given target object. <br>
         * <ol>
         *     <li>
         *         将{@code target}（都是{@link ContextMap}对象）强转成{@link Map}
         *     </li>
         *     <li>
         *         调用{@code target}的{@link ContextMap#get(Object)}传入{@code name}得到返回结果；判断 {@code target}.containsKey({@code name}) 或者 返回的结果 != null：
         *         <ul>
         *             <li>
         *                 如果为true：直接return 返回的结果，本方法结束
         *             </li>
         *             <li>
         *                 如果为false：什么也不做，继续往下走
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         来到这里，说明当前{@code target}（{@link ContextMap}对象）封装的对象有可能不是一个JavaBean，对应的{@link ContextMap#parameterMetaObject}是null。尝试调用{@code target}的{@link ContextMap#get(Object)}传入{@link #PARAMETER_OBJECT_KEY}
         *         获取对应的value（参考构造方法{@link DynamicContext#DynamicContext(Configuration, Object)}第二步）获取一个{@link Map}对象
         *     </li>
         *     <li>
         *         判断上一步获得的对象 instanceof {@link Map}：如果为true则直接 return 上一步获得的对象.get({@code name})；否则 return null。本方法结束
         *     </li>
         * </ol>
         *
         * @param context
         *          The current execution context. （当前OGNL解析过程的命名上下文，参考{@link OgnlCache#getValue(String, Object)}第一步）
         * @param target
         *            the object to get the property from （要取属性值的对象，这里都认为其只会是{@link ContextMap}对象，参考{@link OgnlCache#getValue(String, Object)}第二步）
         * @param name
         *            the name of the property to get. （要获取对应值的属性名称）
         *
         * @return the current value of the given property in the given object
         * @exception OgnlException
         *                if there is an error locating the property in the given object
         */
        @Override
        public Object getProperty(Map context, Object target, Object name) throws OgnlException {
            Map map = (Map) target;

            Object result = map.get(name);
            if (map.containsKey(name) || result != null) {
                return result;
            }

            Object parameterObject = map.get(PARAMETER_OBJECT_KEY);
            if (parameterObject instanceof Map) {
                return ((Map) parameterObject).get(name);
            }

            return null;
        }

        /**
         * Sets the value of the property of the given name in the given target object. <br>
         * 强转{@code target}（{@link ContextMap}对象）为{@link Map}类型，然后调用其对应的{@link Map#put(Object, Object)}传入{@code name}和{@code value}进行设置缓存
         *
         * @param context
         *          The current execution context.
         * @param target
         *            the object to set the property in
         * @param name
         *            the name of the property to set
         * @param value
         *            the new value for the property.
         *
         * @exception OgnlException
         *                if there is an error setting the property in the given object
         */
        @Override
        public void setProperty(Map context, Object target, Object name, Object value)
                throws OgnlException {
            Map<Object, Object> map = (Map<Object, Object>) target;
            map.put(name, value);
        }

        @Override
        public String getSourceAccessor(OgnlContext arg0, Object arg1, Object arg2) {
            return null;
        }

        @Override
        public String getSourceSetter(OgnlContext arg0, Object arg1, Object arg2) {
            return null;
        }
    }

}