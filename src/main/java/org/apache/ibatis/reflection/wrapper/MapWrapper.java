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

import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.SystemMetaObject;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Map类对象包装器
 *
 * @author Clinton Begin
 */
public class MapWrapper extends BaseWrapper {

    private final Map<String, Object> map;

    public MapWrapper(MetaObject metaObject, Map<String, Object> map) {
        super(metaObject);
        this.map = map;
    }

    @Override
    public Object get(PropertyTokenizer prop) {
        // 如果当前层属性携带[]，说明当前层属性是容器类型（Map、Collection、数组）的
        if (prop.getIndex() != null) {
            // 先获得当前层属性对应的对象值
            Object collection = resolveCollection(prop, map);
            // 处理[]中的内容
            return getCollectionValue(prop, collection);
        } else {
            // 如果没有[]，直接根据key获取map的value进行返回
            return map.get(prop.getName());
        }
    }

    @Override
    public void set(PropertyTokenizer prop, Object value) {
        // 如果当前prop带有index，说明当前层属性表达式name部分对应的是一个容器
        if (prop.getIndex() != null) {
            // 则将name作为key获得当前MapWrapper所Wrapd的Map的对应的value（容器）
            Object collection = resolveCollection(prop, map);
            // 然后将value设置给该容器对应的index
            setCollectionValue(prop, collection, value);
        } else {
            // 如果不含index，直接将prop的name作为key，并设置对应value到当前MapWrapper所Wrapd的Map
            map.put(prop.getName(), value);
        }
    }

    @Override
    public String findProperty(String name, boolean useCamelCaseMapping) {
        return name;
    }

    @Override
    public String[] getGetterNames() {
        return map.keySet().toArray(new String[map.keySet().size()]);
    }

    @Override
    public String[] getSetterNames() {
        return map.keySet().toArray(new String[map.keySet().size()]);
    }

    @Override
    public Class<?> getSetterType(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        //1、传入的属性表达式是否有两层以上
        if (prop.hasNext()) {
            // 1.1、构建当前层的属性值（包含index"[]"）对应的MetaObject对象
            MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
            // 1.2、如果当前map的该属性（key）对应的对象（value）是SystemMetaObject.NULL_META_OBJECT
            // （为了节省内存，在setValue那里只要任何属性是null，都会设置该值），说明该key对应的值是null，
            // 这里返回了Object.class，不知道是为什么
            if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                return Object.class;
            } else {
            // 1.3、如果不是null，则将属性表达式的子层交给新构建的MetaObject进行解析，达到一个递归的效果。直到有一个进入MapWrapper或者BeanWrapper的步骤2（递归出口，即到达了属性表达式末端）
                return metaValue.getSetterType(prop.getChildren());
            }
        } else {
            // 2、如果prop没有children，说明name就是末端的属性表达式，没有点号了，直接将传入的属性表达式作为key获取map的value，不为null就获取其class，否则返回Object.class
            // 这里有个问题，没有考虑到最后一层还可能携带了[]的情况：
            /*
            Map internalMap = new HashMap();
            internalMap.put("a", "String");
            Map externalMap = new HashMap();
            externalMap.put("a", internalMap);
            */
            // 以上代码使用"[a]"和"a[a]"都会进入if而得到Object.class，实际上却应该是HashMap.class和String.class
            if (map.get(name) != null) {
                return map.get(name).getClass();
            } else {
                return Object.class;
            }
        }
    }

    @Override
    public Class<?> getGetterType(String name) {
        //逻辑和getSetterType一样
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
            if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                return Object.class;
            } else {
                return metaValue.getGetterType(prop.getChildren());
            }
        } else {
            if (map.get(name) != null) {
                return map.get(name).getClass();
            } else {
                return Object.class;
            }
        }
    }

    @Override
    public boolean hasSetter(String name) {
        return true;
    }

    @Override
    public boolean hasGetter(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        // 1、属性表达式是否存在多层
        if (prop.hasNext()) {
            // 1.1、将indexedName整个作为key判断key是否存在，存在则继续
            if (map.containsKey(prop.getIndexedName())) {
                // 1.1.1、递归构建MetaaObject
                MetaObject metaValue = metaObject.metaObjectForProperty(prop.getIndexedName());
                // 1.1.2、如果构建出来的是Null Object，说明该IndexedName对应的value是null，这里直接返回了true，看不懂是为什么，可能是没有考虑像下面的情况，返回的是true：
                /*
                Map map = new HashMap();
                map.put("a", null);
                MetaObject metaObject = MetaObject.forObject(map, SystemMetaObject.DEFAULT_OBJECT_FACTORY, SystemMetaObject.DEFAULT_OBJECT_WRAPPER_FACTORY, new DefaultReflectorFactory());
                boolean getterType = metaObject.hasGetter("a.b.c");
                 */
                if (metaValue == SystemMetaObject.NULL_META_OBJECT) {
                    return true;
                //1.1.3、如果构建出来的对象不是null，则再调用该MetaObject对象的hasGetter方法进行伪递归直到进入MapWrapper或者BeanWrapper的hasGetter方法的步骤2
                } else {
                    return metaValue.hasGetter(prop.getChildren());
                }
            // 1.2、将indexedName整个作为key判断key是否存在，不存在直接返回false
            } else {
                return false;
            }
        // 2、当传入的属性表达式只有一层的时候，如果map中存在了name对应的key就返回true，否则false。
        //这里貌似有点问题，上面用的是map.containsKey(prop.getIndexedName()，这里又是name，像下面的例子得到的结果就是true，按照本方法的逻辑优点说不通，
        //说是将indexedName作为一个key查询是否存在该key的结果返回又不是，说是将name作为key查询是否存在该key并将对应的index作为key对应map中的value的属性名进行解析又没有做，很奇怪
        /*
        Map map = new HashMap();
        map.put("a", "String");
        MetaObject metaObject = MetaObject.forObject(map, SystemMetaObject.DEFAULT_OBJECT_FACTORY, SystemMetaObject.DEFAULT_OBJECT_WRAPPER_FACTORY, new DefaultReflectorFactory());
        boolean getterType = metaObject.hasGetter("a[b]");
        */
        } else {
            return map.containsKey(prop.getName());
        }
    }

    @Override
    public MetaObject instantiatePropertyValue(String name, PropertyTokenizer prop, ObjectFactory objectFactory) {
        HashMap<String, Object> map = new HashMap<>();
        set(prop, map);
        return MetaObject.forObject(map, metaObject.getObjectFactory(), metaObject.getObjectWrapperFactory(), metaObject.getReflectorFactory());
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
    public <E> void addAll(List<E> element) {
        throw new UnsupportedOperationException();
    }

}
