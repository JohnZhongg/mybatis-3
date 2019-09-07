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

import org.apache.ibatis.reflection.invoker.GetFieldInvoker;
import org.apache.ibatis.reflection.invoker.Invoker;
import org.apache.ibatis.reflection.invoker.MethodInvoker;
import org.apache.ibatis.reflection.property.PropertyTokenizer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

/**
 * 存储Class的元数据
 *
 * @author Clinton Begin
 */
public class MetaClass {

    private final ReflectorFactory reflectorFactory;
    private final Reflector reflector;

    private MetaClass(Class<?> type, ReflectorFactory reflectorFactory) {
        this.reflectorFactory = reflectorFactory;
        this.reflector = reflectorFactory.findForClass(type);
    }

    /**
     * 使用ReflectorFactory构建一个Class的Reflector，并构造一个MetaClass对象，将ReflectorFactory和Reflector都存在该MetaClass中
     *
     * @param type             要构建相应MetaClass对象的Class
     * @param reflectorFactory 反射容器工厂
     * @return
     */
    public static MetaClass forClass(Class<?> type, ReflectorFactory reflectorFactory) {
        return new MetaClass(type, reflectorFactory);
    }

    /**
     * 只是从reflector的getTypes中获取getter或者field的返回值类型构建一个Property的MetaClass
     *
     * @param name 属性名称
     * @return
     */
    public MetaClass metaClassForProperty(String name) {
        Class<?> propType = reflector.getGetterType(name);
        return MetaClass.forClass(propType, reflectorFactory);
    }

    /**
     * 传入大小写不敏感的属性表达式，返回大小写准确的属性表达式（不解析下划线）
     * "FIrST.LONG1" -- > "first.long1"
     *
     * @param name 大小写不敏感的属性表达式（不含下划线）
     * @return
     */
    public String findProperty(String name) {
        StringBuilder prop = buildProperty(name, new StringBuilder());
        return prop.length() > 0 ? prop.toString() : null;
    }

    /**
     * 传入大小写不敏感的属性表达式，返回大小写准确的属性表达式，解析下划线
     *
     * @param name                传入大小写不敏感的属性表达式（可能含有下划线）
     * @param useCamelCaseMapping 是否使用驼峰式命名（即是否消除下划线）
     * @return
     */
    public String findProperty(String name, boolean useCamelCaseMapping) {
        if (useCamelCaseMapping) {
            name = name.replace("_", "");
        }
        return findProperty(name);
    }

    /**
     * 调用{@link Reflector#getGetablePropertyNames()}返回可读的属性名称集合
     *
     * @return
     */
    public String[] getGetterNames() {
        return reflector.getGetablePropertyNames();
    }

    /**
     * 调用{@link Reflector#getSetablePropertyNames()}返回可写的属性名称集合
     *
     * @return
     */
    public String[] getSetterNames() {
        return reflector.getSetablePropertyNames();
    }

    /**
     * 解析属性表达式，获取属性表达式最末尾属性的setter类型<br>
     * firstLevelProp.secondLevelProp.thirdLevelProp -> 返回thirdLevelProp的类型
     *
     * @param name 属性表达式
     * @return
     */
    public Class<?> getSetterType(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            MetaClass metaProp = metaClassForProperty(prop.getName());
            return metaProp.getSetterType(prop.getChildren());
        } else {
            return reflector.getSetterType(prop.getName());
        }
    }

    /**
     * 解析属性表达式，获取属性表达式最末尾属性的Getter类型<br>
     * firstLevelProp.secondLevelProp.thirdLevelProp -> 返回thirdLevelProp的类型
     *
     * @param name 属性表达式
     * @return
     */
    public Class<?> getGetterType(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            MetaClass metaProp = metaClassForProperty(prop);
            return metaProp.getGetterType(prop.getChildren());
        }
        // issue #506. Resolve the type inside a Collection Object
        return getGetterType(prop);
    }


    private MetaClass metaClassForProperty(PropertyTokenizer prop) {
        Class<?> propType = getGetterType(prop);
        return MetaClass.forClass(propType, reflectorFactory);
    }

    /**
     * 如果getter是一个Collection，则返回Collection的元素的类型
     * @param prop
     * @return
     */
    private Class<?> getGetterType(PropertyTokenizer prop) {
        Class<?> type = reflector.getGetterType(prop.getName());
        if (prop.getIndex() != null && Collection.class.isAssignableFrom(type)) {
            Type returnType = getGenericGetterType(prop.getName());
            if (returnType instanceof ParameterizedType) {
                Type[] actualTypeArguments = ((ParameterizedType) returnType).getActualTypeArguments();
                if (actualTypeArguments != null && actualTypeArguments.length == 1) {
                    returnType = actualTypeArguments[0];
                    if (returnType instanceof Class) {
                        type = (Class<?>) returnType;
                    } else if (returnType instanceof ParameterizedType) {
                        type = (Class<?>) ((ParameterizedType) returnType).getRawType();
                    }
                }
            }
        }
        return type;
    }

    /**
     * 可以获取Collection类型的属性中元素的属性
     * @param propertyName Collection类型的属性名称
     * @return 返回对应的类型
     */
    private Type getGenericGetterType(String propertyName) {
        try {
            Invoker invoker = reflector.getGetInvoker(propertyName);
            if (invoker instanceof MethodInvoker) {
                Field _method = MethodInvoker.class.getDeclaredField("method");
                _method.setAccessible(true);
                Method method = (Method) _method.get(invoker);
                return TypeParameterResolver.resolveReturnType(method, reflector.getType());
            } else if (invoker instanceof GetFieldInvoker) {
                Field _field = GetFieldInvoker.class.getDeclaredField("field");
                _field.setAccessible(true);
                Field field = (Field) _field.get(invoker);
                return TypeParameterResolver.resolveFieldType(field, reflector.getType());
            }
        } catch (NoSuchFieldException | IllegalAccessException ignored) {
        }
        return null;
    }

    /**
     * 解析属性表达式，检查表达式最末尾属性是否有setter，本方法没有解析index[]中的内容：
     * <ol>
     *     <li>
     *         如果属性表达式只有一层，完全没问题，因为携带[]代表name对应的是一个容器（Map、Collection、数组），它们对于任何名称的setter都没问题
     *     </li>
     *     <li>
     *         如果属性表达式有多层，且最后一层之前的indexedName是一个携带了index的（说明其中一层属性是一个容器），那么就有问题了，参考下面例子：
     *     </li>
     * </ol>
     * <pre>
     *     class A{
     *         public B b = new B();
     *     }
     *     class B{
     *         public String bField = "B Field";
     *     }
     *     class C{
     *         public Map<String, Map<String, A>> map = new HashMap<String, Map<String, A>>(){
     *             private static final long serialVersionUID = -5868006606988149708L;
     *
     *             {
     *                 put("map", new HashMap<String, A> () {
     *                     private static final long serialVersionUID = 6647253973519127766L;
     *
     *                     {
     *                         put("a", new A());
     *                     }
     *                 });
     *             }
     *         };
     *     }
     *
     *     MetaClass metaClass = MetaClass.forClass(C.class, new DefaultReflectorFactory());
     *     boolean b = metaClass.hasSetter("map[map].a");
     *     boolean b = metaClass.hasSetter("map.map.a");
     * </pre>
     * <b>所以本方法解析不了中间有容器的属性表达式</b>（可能是考虑到对于容器这一类来说，其只有在真正的运行时可以动态的设置其元素，即相当于bean中动态的属性，无法在class中判断是否存在setter，
     * 或许可以从容器的泛型来判断，但是如果没有明确赋值给泛型呢，例如把Map的泛型去掉，就完全无法在class层级解析类似"map[map].a"的表达式了，所以优先交由MetaObject和ObjectWrapper去解析？？？？）
     * @param name 属性表达式
     * @return
     */
    public boolean hasSetter(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            if (reflector.hasSetter(prop.getName())) {
                MetaClass metaProp = metaClassForProperty(prop.getName());
                return metaProp.hasSetter(prop.getChildren());
            } else {
                return false;
            }
        } else {
            return reflector.hasSetter(prop.getName());
        }
    }

    /**
     * 解析属性表达式，检查表达式最末尾属性是否有getter，还有更多解析参考{@link MetaClass#hasSetter(String)}
     *
     * @param name 属性表达式
     * @return
     */
    public boolean hasGetter(String name) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            if (reflector.hasGetter(prop.getName())) {
                MetaClass metaProp = metaClassForProperty(prop);
                return metaProp.hasGetter(prop.getChildren());
            } else {
                return false;
            }
        } else {
            return reflector.hasGetter(prop.getName());
        }
    }

    /**
     * 传入属性名称，获取属性名称对应的getterInvoker
     *
     * @param name 属性名称
     * @return
     */
    public Invoker getGetInvoker(String name) {
        return reflector.getGetInvoker(name);
    }

    /**
     * 传入属性名称，获取属性名称对应的setterInvoker
     *
     * @param name 属性名称
     * @return
     */
    public Invoker getSetInvoker(String name) {
        return reflector.getSetInvoker(name);
    }

    /**
     * 传入大小写不敏感的属性表达式，构建准确大小写的属性表达式(本方法会自己递归调用自己)：
     * "FIrST.LONG1" -- > "first.long1"
     *
     * @param name    大小写不敏感的属性表达式
     * @param builder 构建的StringBuilder
     * @return 构建的StringBuilder
     */
    private StringBuilder buildProperty(String name, StringBuilder builder) {
        PropertyTokenizer prop = new PropertyTokenizer(name);
        if (prop.hasNext()) {
            String propertyName = reflector.findPropertyName(prop.getName());
            if (propertyName != null) {
                builder.append(propertyName);
                builder.append(".");
                MetaClass metaProp = metaClassForProperty(propertyName);
                metaProp.buildProperty(prop.getChildren(), builder);
            }
        } else {
            String propertyName = reflector.findPropertyName(name);
            if (propertyName != null) {
                builder.append(propertyName);
            }
        }
        return builder;
    }

    /**
     * 查询当前MetaClass对应的Class是否有无参构造
     *
     * @return
     */
    public boolean hasDefaultConstructor() {
        return reflector.hasDefaultConstructor();
    }

}
