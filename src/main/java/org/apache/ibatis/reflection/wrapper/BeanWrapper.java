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

import org.apache.ibatis.reflection.*;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.invoker.Invoker;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

import java.util.List;

/**
 * @author Clinton Begin
 */
public class BeanWrapper extends BaseWrapper {

    private final Object object;
    private final MetaClass metaClass;

    public BeanWrapper(MetaObject metaObject, Object object) {
        super(metaObject);
        this.object = object;
        this.metaClass = MetaClass.forClass(object.getClass(), metaObject.getReflectorFactory());
    }

    @Override
    public Object get(PropertyTokenizer prop) {
        // 如果当前层属性携带[]，说明当前层属性是容器类型（Map、Collection、数组）的
        if (prop.getIndex() != null) {
            // 先获得当前层属性对应的对象值
            Object collection = resolveCollection(prop, object);
            // 处理[]中的内容
            return getCollectionValue(prop, collection);
        } else {
            // 如果没有[]，直接根据key获取map的value进行返回
            return getBeanProperty(prop, object);
        }
    }

    @Override
    public void set(PropertyTokenizer prop, Object value) {
        // 如果当前prop带有index，说明当前层属性表达式name部分对应的是一个容器
        if (prop.getIndex() != null) {
            // 使用getValue获取当前层属性表达式中name对应的BeanWrapper所Wrap的Bean的容器属性对象
            Object collection = resolveCollection(prop, object);
            // 然后将value设置给该容器对应的index
            setCollectionValue(prop, collection, value);
        } else {
            // 如果没有[]，直接获取当前BeanWrapper所Wrap的Bean的对应属性的Invoker进行赋值
            setBeanProperty(prop, object, value);
        }
    }

    @Override
    public String findProperty(String name, boolean useCamelCaseMapping) {
        return metaClass.findProperty(name, useCamelCaseMapping);
    }

    @Override
    public String[] getGetterNames() {
        return metaClass.getGetterNames();
    }

    @Override
    public String[] getSetterNames() {
        return metaClass.getSetterNames();
    }

    @Override
    public Class<?> getSetterType(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        //1、传入的属性表达式是否有两层以上
        if (prop.hasNext()) {
            // 1.1、构建当前层的属性值（包含index"[]"）对应的MetaObject对象
            MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
            // 1.2、如果当前map的该属性（key）对应的对象（value）是SystemMetaObject.NULL_META_OBJECT
            // （为了节省内存，在setValue那里只要任何属性是null，都会设置该值），说明该属性对应的值是null，则取class上定义的类型
            if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                return metaClass.getSetterType(name);
            } else {
            // 1.3、如果不是null，则将属性表达式的子层交给新构建的MetaObject进行解析，达到一个递归的效果。直到有一个进入MapWrapper或者BeanWrapper的步骤2（递归出口，即到达了属性表达式末端）
                return metaValue.getSetterType(prop.getChildren());
            }
        } else {
            // 2、这里是来到了属性表达式的末端，直接获取当前object对应class上定义的该属性的类型
            return metaClass.getSetterType(name);
        }
    }

    @Override
    public Class<?> getGetterType(String name) {
        //逻辑和getSetterType一样
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
            if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                return metaClass.getGetterType(name);
            } else {
                return metaValue.getGetterType(prop.getChildren());
            }
        } else {
            return metaClass.getGetterType(name);
        }
    }

    @Override
    public boolean hasSetter(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        // 1、传入的属性表达式是否有两层以上
        if (prop.hasNext()) {
            // 1.1、有两层以上的属性，则判断当前层属性是否存在setter（虽然这里传入给MetaClass的hasSetter方法的是含有[]的indexedName但是该方法只会解析name，不解析[]中的内容）
            if (metaClass.hasSetter(prop.getIndexedName())) {
                // 1.1.1、当前层属性的name在当前class存在setter，则继续，上面没有传入给MetaClass的hasSetter方法没有解析[]，它只负责解析当前一层的name是否有setter即可，剩下的交由MetaObject和ObjectWrapper负责，
                // 获取当前层indexedName对应的属性值对象，如果获取的对象是null，那就无法从对象层级获取是否存在setter了
                MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
                // 1.1.2、如果获取到的属性值对象是null，则尽力了，无法从Object层级判断，交由metaClass进行class层级的判断，但是依然存在无法解析属性表达式中非末端携带[]的问题
                if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                    return metaClass.hasSetter(name);
                } else {
                // 1.1.3、如果获取到了属性值对象，递归调用，直到到达MapWrapper的hasSeter直接返回true或者到达BeanWrapper的hasSetter的步骤2
                    return metaValue.hasSetter(prop.getChildren());
                }
            } else {
            //1.2、如果当前层属性的name不存在setter，则直接返回false
                return false;
            }
        // 2、如果传入的是属性表达式末端，则直接获取当前对象的class是否存在该属性对应的setter（来到末端直接进行class层级判断即可，即使带有[]也无所谓，只有name存在，[]的hasSetter永远是true）
        } else {
            return metaClass.hasSetter(name);
        }
    }

    @Override
    public boolean hasGetter(String name) {
        // 逻辑同hasSetter，只不过递归出口改为BeanWrapper或者MapWrapper的hasGetter方法的步骤2
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            if (metaClass.hasGetter(prop.getIndexedName())) {
                MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
                if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                    return metaClass.hasGetter(name);
                } else {
                    return metaValue.hasGetter(prop.getChildren());
                }
            } else {
                return false;
            }
        } else {
            return metaClass.hasGetter(name);
        }
    }

    @Override
    public MetaObject instantiatePropertyValue(String name, PropertyTokenizer prop, ObjectFactory objectFactory) {
        MetaObject metaValue;
        //拿到属性表达式的第一层值（不带index），即当前被包装对象的属性的类型
        Class<?> type = getSetterType(prop.getName());
        try {
            //调用该类型的无参构造器实例化属性对象
            Object newObject = objectFactory.create(type);
            //构建该属性对象的元数据对象
            metaValue = MetaObject.forObject(newObject, metaObject.getObjectFactory(), metaObject.getObjectWrapperFactory(), metaObject.getReflectorFactory());
            //将新构建的属性对象赋值给当前对象包装器包装对象的属性
            set(prop, newObject);
        } catch (Exception e) {
            throw new ReflectionException("Cannot set value of property '" + name + "' because '" + name + "' is null and cannot be instantiated on instance of " + type.getName() + ". Cause:" + e.toString(), e);
        }
        return metaValue;
    }

    /**
     * 传入一个表达式编译器对象，本方法认为传入属性表达式只有一层所以只会取表达式第一层值（直接获取无index的name）作为属性名称进行取值（调用{@link MetaClass#getGetInvoker(java.lang.String)}方法获得Invoker并invoke）
     * @param prop 属性表达式编译器对象
     * @param object 被包装的对象（也就是要被取属性值的对象）
     * @return 取出的属性值
     */
    private Object getBeanProperty(PropertyTokenizer prop, Object object) {
        try {
            Invoker method = metaClass.getGetInvoker(prop.getName());
            try {
                return method.invoke(object, NO_ARGUMENTS);
            } catch (Throwable t) {
                throw ExceptionUtil.unwrapThrowable(t);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Throwable t) {
            throw new ReflectionException("Could not get property '" + prop.getName() + "' from " + object.getClass() + ".  Cause: " + t.toString(), t);
        }
    }

    /**
     * 传入一个表达式编译器对象，本方法认为传入属性表达式只有一层所以只会取表达式第一层值即仅对当前包装器包装的对象的对应的属性值进行设值
     * @param prop 属性表达式编译器对象
     * @param object 被包装的对象（也就是要被设置属性的对象）
     * @param value 该属性要被设置的值
     */
    private void setBeanProperty(PropertyTokenizer prop, Object object, Object value) {
        try {
            Invoker method = metaClass.getSetInvoker(prop.getName());
            Object[] params = {value};
            try {
                method.invoke(object, params);
            } catch (Throwable t) {
                throw ExceptionUtil.unwrapThrowable(t);
            }
        } catch (Throwable t) {
            throw new ReflectionException("Could not set property '" + prop.getName() + "' of '" + object.getClass() + "' with value '" + value + "' Cause: " + t.toString(), t);
        }
    }

    @Override
    public boolean isCollection() {
        return false;
    }

    @Override
    public void add(Object element) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> void addAll(List<E> list) {
        throw new UnsupportedOperationException();
    }

}
