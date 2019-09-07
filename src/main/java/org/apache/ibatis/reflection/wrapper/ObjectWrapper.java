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
package org.apache.ibatis.reflection.wrapper;

import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

import java.util.List;

/**
 * 对象包装器接口
 * @author Clinton Begin
 */
public interface ObjectWrapper {

    /**
     * 传入一个表达式编译器对象，本方法认为传入的表达式编译器对象只有一层，没有children，所以只会取表达式第一层值（含index解析集合）即仅对当前包装器包装的对象的对应的属性值进行取值。
     * 调用{@link BaseWrapper#resolveCollection(org.apache.ibatis.reflection.property.PropertyTokenizer, java.lang.Object)}和
     * {@link BaseWrapper#getCollectionValue(org.apache.ibatis.reflection.property.PropertyTokenizer, java.lang.Object)} 解析可能存在的[]
     * @param prop 表达式编译器对象
     * @return 取到的属性值
     */
    Object get(PropertyTokenizer prop);

    /**
     * 传入一个表达式编译器对象，本方法认为传入的表达式编译器对象只有一层，没有children，所以只会取表达式第一层值（含index解析集合）即仅对当前包装器包装的对象的对应的属性值进行设值。
     * 调用{@link BaseWrapper#resolveCollection(org.apache.ibatis.reflection.property.PropertyTokenizer, java.lang.Object)}和
     * {@link BaseWrapper#setCollectionValue(PropertyTokenizer, Object, Object)} 解析可能存在的[]
     * @param prop 表达式编译器对象
     * @param value 要设置的值
     */
    void set(PropertyTokenizer prop, Object value);

    /**
     * <ul>
     *     <li>
     *         {@link BeanWrapper}:直接调用{@link MetaClass#findProperty(String, boolean)}
     *     </li>
     *     <li>
     *         {@link MapWrapper}:直接返回了传入的name
     *     </li>
     *     <li>
     *         {@link CollectionWrapper}: throw {@link UnsupportedOperationException}
     *     </li>
     * </ul>
     * @param name 大小写不敏感的属性表达式（还可以带下划线）
     * @param useCamelCaseMapping 是否使用驼峰式命名
     * @return 精准正确的属性表达式
     */
    String findProperty(String name, boolean useCamelCaseMapping);

    /**
     * <ul>
     *     <li>
     *         {@link MapWrapper} : 直接返回keyset构建的字符串数组
     *     </li>
     *     <li>
     *         {@link BeanWrapper} : 直接调用{@link MetaClass#getGetterNames()}返回当前对象可读属性名称集合
     *     </li>
     *     <li>
     *         {@link CollectionWrapper} : 抛出 {@link UnsupportedOperationException}
     *     </li>
     * </ul>
     * @return 返回可读属性名称集合
     */
    String[] getGetterNames();

    /**
     * <ul>
     *     <li>
     *         {@link MapWrapper} : 直接返回keyset构建的字符串数组
     *     </li>
     *     <li>
     *         {@link BeanWrapper} : 直接调用{@link MetaClass#getSetterNames()} ()}返回当前对象可读属性名称集合
     *     </li>
     *     <li>
     *         {@link CollectionWrapper} : 抛出 {@link UnsupportedOperationException}
     *     </li>
     * </ul>
     * @return 返回可写属性名称集合
     */
    String[] getSetterNames();

    /**
     * 传入属性表达式，获取表达式末端对应的writable属性的类型，{@link MapWrapper} 获取的是属性对应的值对象的类型，{@link BeanWrapper} 获取的是属性对应的class定义的成员（Field或者Method的参数及返回值）的类型
     *
     * @param name 属性表达式
     * @return 属性表达式末端属性的类型
     */
    Class<?> getSetterType(String name);

    /**
     * 逻辑同{@link ObjectWrapper#getSetterType(String)}
     * @param name 属性表达式
     * @return 属性表达式末端属性的类型
     */
    Class<?> getGetterType(String name);

    /**
     *传入属性表达式，判断该对象是否可以对该属性表达式进行设值
     * <ul>
     *     <li>
     *         {@link MapWrapper#hasSetter(String)} : 直接返回true，相当于传入的属性表达式都被当做key，因为mybatis所有Map的对象都被替换成了HashMap，所以null是可以设置的
     *     </li>
     *     <li>
     *         {@link BeanWrapper#hasSetter(String)} : 如果属性表达式有多层，会递归构建{@link MetaObject}对象然后再调用{@link MetaObject#hasSetter(String)}（伪递归），
     *         直到到达最后一层属性表达式则调用{@link MetaClass#hasSetter(String)}，具体方法逻辑请看{@link BeanWrapper#hasSetter(String)}方法内部注释及代码
     *     </li>
     * </ul>
     * @param name 属性表达式
     * @return
     */
    boolean hasSetter(String name);

    /**
     * 传入属性表达式，判断该对象是否已经被设置了对应该属性表达式的值，可以根据该属性表达式获取到该值
     * <ul>
     *     <li>
     *         {@link MapWrapper#hasGetter(String)} : 详情看方法内部注释和代码
     *     </li>
     *     <li>
     *         {@link BeanWrapper#hasGetter(String)} : 详情看方法内部注释和代码
     *     </li>
     * </ul>
     *
     * @param name 属性表达式
     * @return
     */
    boolean hasGetter(String name);

    /**
     * {@link BeanWrapper}：实例化当前对象包装器所包装对象的属性（取属性表达式的name即第一层对应属性的无参构造），并构建该属性对象的MetaObject对象返回<br>
     * {@link MapWrapper}：写死的实例化一个HashMap<String, Object>作为当前Map对象的第一层属性值
     *
     * @param name 传入的属性表达式
     * @param prop 传入的属性表达式的编译器对象
     * @param objectFactory 对象工厂（生产属性的类对象）
     * @return 属性对应的元数据对象
     */
    MetaObject instantiatePropertyValue(String name, PropertyTokenizer prop, ObjectFactory objectFactory);

    /**
     * 对象包装器包装的对象是否是集合{@link java.util.Collection}
     * @return
     */
    boolean isCollection();

    /**
     * 调用当前元数据对象对应对象的add方法添加元素（仅当当前对象包装器包装的对象是集合的时候该方法才可用，否则抛出{@link UnsupportedOperationException}）
     * @param element
     */
    void add(Object element);

    /**
     * 调用当前元数据对象对应对象的addAll方法添加一个元素集合（仅当当前元数据对应对象是集合的时候该方法才可用，否则抛出{@link UnsupportedOperationException}）
     * @param element 要添加的元素集合（{@link List}）
     * @param <E> 元素集合的泛型
     */
    <E> void addAll(List<E> element);

}
