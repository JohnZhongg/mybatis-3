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
package org.apache.ibatis.reflection;

import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.property.PropertyTokenizer;
import org.apache.ibatis.reflection.wrapper.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 对象元数据（包含原对象，对象包装器，对象工厂，对象包装器工厂，对象对应的类反射信息容器工厂）<br>
 * 使用了策略模式，所有操作由对象包装器完成，不同对象使用不同的对象包装器<br>
 *
 * @author Clinton Begin
 */
public class MetaObject {

    /**
     * 原始对象
     */
    private final Object originalObject;
    /**
     * 对象包装器
     */
    private final ObjectWrapper objectWrapper;
    /**
     * 对象工厂
     */
    private final ObjectFactory objectFactory;
    /**
     * 对象包装器工厂
     */
    private final ObjectWrapperFactory objectWrapperFactory;
    /**
     * 反射信息容器工厂
     */
    private final ReflectorFactory reflectorFactory;

    private MetaObject(Object object, ObjectFactory objectFactory, ObjectWrapperFactory objectWrapperFactory, ReflectorFactory reflectorFactory) {
        this.originalObject = object;
        this.objectFactory = objectFactory;
        this.objectWrapperFactory = objectWrapperFactory;
        this.reflectorFactory = reflectorFactory;

        if (object instanceof ObjectWrapper) {
            this.objectWrapper = (ObjectWrapper) object;
        } else if (objectWrapperFactory.hasWrapperFor(object)) {
            //如果当前包装器工厂支持生产当前对象的包装器，就对对象进行包装然后返回该包装器
            // （默认的包装器工厂不支持产生任何对象的包装器，包装器工厂仅作为一个接口被自定义实现，当前MetaObject默认使用默认包装器工厂）
            this.objectWrapper = objectWrapperFactory.getWrapperFor(this, object);
        } else if (object instanceof Map) {
            //如果该对象是Map容器对象，就使用Map包装器进行包装
            this.objectWrapper = new MapWrapper(this, (Map) object);
        } else if (object instanceof Collection) {
            //如果该对象是Collection容器对象，就使用Collection包装器进行包装
            this.objectWrapper = new CollectionWrapper(this, (Collection) object);
        } else {
            //如果不是Map或者Collection对象就使用Bean包装器进行包装
            this.objectWrapper = new BeanWrapper(this, object);
        }
    }

    /**
     * 传入一个对象，封装对象元数据到MetaObject（如果传入的对象是null，就返回{@link SystemMetaObject.NULL_META_OBJECT}）
     *
     * @param object               传入的对象
     * @param objectFactory        对象工厂
     * @param objectWrapperFactory 对象包装器工厂
     * @param reflectorFactory     反射信息容器工厂
     * @return 构建好的元对象
     */
    public static MetaObject forObject(Object object, ObjectFactory objectFactory, ObjectWrapperFactory objectWrapperFactory, ReflectorFactory reflectorFactory) {
        if (object == null) {
            //如果对象是null，返回一个空对象的元对象
            return SystemMetaObject.NULL_META_OBJECT;
        } else {
            return new MetaObject(object, objectFactory, objectWrapperFactory, reflectorFactory);
        }
    }

    public ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    public ObjectWrapperFactory getObjectWrapperFactory() {
        return objectWrapperFactory;
    }

    public ReflectorFactory getReflectorFactory() {
        return reflectorFactory;
    }

    public Object getOriginalObject() {
        return originalObject;
    }

    /**
     * 解析大小写不敏感且可能带有下划线的属性名称或者属性表达式（可选择下划线转驼峰式）<br>
     * 如果当前的MetaObject封装的Object是：<br>
     * 1. Map：直接返回传入的值<br>
     * 2. Collection：不支持，会报（{@link UnsupportedOperationException}）<br>
     * 3. Bean：支持传入属性表达式，调用MetaClass的findProperty<br>
     *
     * @param propName            大小写不敏感且可能带有下划线的属性名称或者属性表达式
     * @param useCamelCaseMapping 是否使用驼峰式命名
     * @return 返回正确的属性名称或者属性表达式
     */
    public String findProperty(String propName, boolean useCamelCaseMapping) {
        return objectWrapper.findProperty(propName, useCamelCaseMapping);
    }

    /**
     * 直接调用{@link ObjectWrapper#getGetterNames()}
     * @return 返回可读属性名称集合
     */
    public String[] getGetterNames() {
        return objectWrapper.getGetterNames();
    }

    /**
     * 直接调用{@link ObjectWrapper#getSetterNames()}
     * @return 返回可写属性名称集合
     */
    public String[] getSetterNames() {
        return objectWrapper.getSetterNames();
    }

    /**
     * 直接调用{@link ObjectWrapper#getSetterType(String)}
     * @param name 准确的属性名称或者属性表达式
     * @return 返回属性名称或者属性表达式末端对应的类型
     */
    public Class<?> getSetterType(String name) {
        return objectWrapper.getSetterType(name);
    }

    /**
     * 直接调用{@link ObjectWrapper#getGetterType(String)}
     * @param name 准确的属性名称或者属性表达式
     * @return 返回属性名称或者属性表达式末端对应的类型
     */
    public Class<?> getGetterType(String name) {
        return objectWrapper.getGetterType(name);
    }

    /**
     * 本对象是否有该属性表达式对应的setter，调用{@link ObjectWrapper#hasSetter(String)}
     *
     * @param name 属性表达式
     * @return
     */
    public boolean hasSetter(String name) {
        return objectWrapper.hasSetter(name);
    }

    public boolean hasGetter(String name) {
        return objectWrapper.hasGetter(name);
    }

    /**
     * 传入属性表达式，获取当前元数据对象对应的对象对应该属性表达式末层属性值：
     * <ol>
     *     <li>
     *         如果属性表达式有多层，方法内部会递归调用{@link MetaObject#metaObjectForProperty(String)}构建子属性的{@link MetaObject}对象，然后再调用该对象的本方法{@link MetaObject#getValue(String)}，
     *         这样一层层递归下去，直到达到最后一层属性表达式。
     *     </li>
     *     <li>
     *         当传入的{@code name}是一层属性表达会调用{@link ObjectWrapper#get(PropertyTokenizer)}方法解析IndexedName：
     *         <ul>
     *             <li>
     *                 第一次调用本方法传入的就是只有一层的属性表达式
     *             </li>
     *             <li>
     *                 第一次调用本方法传入的是多层属性表达式，其中每一层属性构建成{@link MetaObject}然后递归下去直到最后一个{@link MetaObject}调用本方法并传入最后一层属性表达式（末端）
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param name 属性表达式
     * @return 获取的属性表达式对应的值
     */
    public Object getValue(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        //传入的属性表达式是否有两层以上
        if (prop.hasNext()) {
            // 1、构建当前层的属性值（包含index"[]"）对应的MetaObject对象
            MetaObject metaValue = metaObjectForProperty(prop.getIndexedName());
            // 2、如果该对象是SystemMetaObject.NULL_META_OBJECT（为了节省内存，在setValue那里只要任何属性是null，都会设置该值），返回null
            if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                return null;
            } else {
            // 3、如果不是null，则将属性表达式的子层交给新构建的MetaObject进行解析，达到一个递归的效果。直到有一个MetaObject进入步骤4（递归出口，即到达了属性表达式末端）
                return metaValue.getValue(prop.getChildren());
            }
        } else {
        // 4、这里是属性表达式末端（即name只有一层），剩下的就是解析indexedName中可能携带的index"[]"中的内容了
            return objectWrapper.get(prop);
        }
    }

    /**
     * 传入属性表达式，对当前元数据对象的对应对象对应属性表达式末层的属性进行赋值
     * <ol>
     *     <li>
     *         如果传入的属性表达式有多层，就截取当前第一层的表达式（含index解析集合），然后调用{@link MetaObject#metaObjectForProperty(String)}
     *         方法传入截取的表达式将第一层的属性转成MetaObject对象返回，然后调用该属性的MetaObject对象的{@link MetaObject#setValue(String, Object)}，
     *         传入属性表达式的子表达式（除去第一层）（即本方法，相当于一个类递归，因为不是同一个对象内的方法调用）。
     *         <ul>
     *             期间如果有获取的属性的MetaObject对象是{@link SystemMetaObject#NULL_META_OBJECT}，说明当前属性值为NULL，此时会对要设置的属性值进行判断：
     *             <li>
     *                 如果是NULL并且子属性表达式不为NULL，就什么都不做，直接结束方法。
     *             </li>
     *             <li>
     *                 如果不是NULL，就会调用属性对应的类的无参构造对属性值进行实例化，并构建MetaObject对象返回，继续调用其setValue方法
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         如果传入的属性表达式只有一层，直接调用{@link ObjectWrapper#set(PropertyTokenizer, Object)} 赋值
     *     </li>
     * </ol>
     * @param name 属性表达式
     * @param value 要设置的值
     */
    public void setValue(String name, Object value) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        // 1、传入的属性表达式是否有多层
        if (prop.hasNext()) {
            // 1.1、先构建当前层的属性值对象的MetaObject对象
            MetaObject metaValue = metaObjectForProperty(prop.getIndexedName());
            // 1.2、如果当前属性值是null
            if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                // 1.2.1、如果传入的value是null，而属性表达式的子表达式不为空（表示将该子属性表达式设置为null），豪无意义（当前层属性获得的值已经是null了，其子属性就更加不用说了，本身就是null），什么也不做，直接返回，如下情况：
                /*
                    @Test
                    public void test03() {
                        Map<String, A> map = new HashMap<>();
                        MetaObject metaObject = MetaObject.forObject(map, SystemMetaObject.DEFAULT_OBJECT_FACTORY, SystemMetaObject.DEFAULT_OBJECT_WRAPPER_FACTORY, new DefaultReflectorFactory());
                        metaObject.setValue("a.b", null);
                        System.out.println();
                    }
                    class A{
                        public B b = new B();
                    }
                    class B{
                        public String bField = "B Field";
                    }
                 */
                //prop.getChildren() != nul多此一举？prop.hasNext()才进来这里的，也就是children不是null
                if (value == null && prop.getChildren() != null) {
                    // don't instantiate child path if value is null
                    return;
                } else {
                // 要设的value不是null，子属性不是null
                // 则：
                // 1.2.2、初始化当前属性值（取该属性的默认值设置进去）并构建该默认属性值的MetaObject对象
                    metaValue = objectWrapper.instantiatePropertyValue(name, prop, objectFactory);
                }
            }
            // 递归调用，直到进入1.2.1和2
            metaValue.setValue(prop.getChildren(), value);
        // 2、调用ObjectWrapper.set方法对末端属性进行设值
        } else {
            objectWrapper.set(prop, value);
        }
    }

    /**
     * 本方法认为接收的属性表达式都是只有一层的，调用{@link MetaObject#getValue(String)}获取属性表达式对应的在当前对象中
     * 的对应的属性的值，会直接进入{@link MetaObject#getValue(String)}方法的步骤4。
     * <ol>
     *     <li>
     *          如果传入的属性表达式有多层，{@link MetaObject#getValue(String)}内部会有类递归的逻辑（调用本方法获取第一层
     *          属性表达式对应的属性值然后调用本方法构建MetaObject对象，再将子属性表达式交给构建的MetaObject对象进行解析），获取表达式末层对应的属性值返回。
     *     </li>
     *     <li>
     *         如果传入的属性表达式只有一层，{@link MetaObject#getValue(String)}，直接调用相应{@link ObjectWrapper#get(PropertyTokenizer)}获取属性值返回
     *     </li>
     * </ol>
     * 然后调用{@link MetaObject#forObject}构建MetaObject对象返回。
     * @param name 传入的属性表达式
     * @return 传入属性表达式末层属性的MetaObject对象
     */
    public MetaObject metaObjectForProperty(String name) {
        Object value = getValue(name);
        return MetaObject.forObject(value, objectFactory, objectWrapperFactory, reflectorFactory);
    }

    public ObjectWrapper getObjectWrapper() {
        return objectWrapper;
    }

    /**
     * 当前元数据对象对应的对象是否是集合{@link Collection}
     * @return
     */
    public boolean isCollection() {
        return objectWrapper.isCollection();
    }

    /**
     * 调用当前元数据对象对应对象的add方法添加元素（仅当当前元数据对应对象是集合的时候该方法才可用，否则抛出{@link UnsupportedOperationException}）
     * @param element 要添加的元素
     */
    public void add(Object element) {
        objectWrapper.add(element);
    }

    /**
     * 调用当前元数据对象对应对象的addAll方法添加一个元素集合（仅当当前元数据对应对象是集合的时候该方法才可用，否则抛出{@link UnsupportedOperationException}）
     * @param list 要添加的元素集合（{@link List}）
     * @param <E> 元素集合的泛型
     */
    public <E> void addAll(List<E> list) {
        objectWrapper.addAll(list);
    }

}
