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
package org.apache.ibatis.type;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * References a generic type.
 *
 * 引用泛型抽象类
 *
 * 目的很简单，就是解析类上定义的泛型
 *
 * @param <T> the referenced type
 * @since 3.1.0
 * @author Simone Tripodi
 */
public abstract class TypeReference<T> {

    /**
     * 泛型
     */
    private final Type rawType;

    protected TypeReference() {
        rawType = getSuperclassTypeParameter(getClass());
    }

    /**
     * 传入一个继承了当前类的class，从该class的父Type开始一直往上推，找到给当前TypeReference赋值的地方
     * （一直往上找，直到找到的父Type是ParameterizedType为止或者是当前类：如果最终找到了当前类说明当前类的<T>
     * 一直都没有被赋值，抛出异常；如果找到了第一个ParameterizedType，就获取其第一个类型参数，这里认为，当前类
     * 的某个父类一定是ParameterizedType并且typeParameter是class且这个父类的子类到该父类的继承路径不会有TypeVariable的ParameterizedType的父Type）
     *
     * 默认规范：currentClass extends fatherClass<String> -> fatherClass<T> extends TypeReference<T>
     * 不规范1：currentClass<T> extends fatherClass<T> -> fatherClass<T> extends TypeReference<T>
     * 不规范2：currentClass<T> extends childClass<T> -> childClass<T> extends fatherClass<String> ->  fatherClass<T> extends TypeReference<T>
     *
     * @param clazz
     * @return
     */
    Type getSuperclassTypeParameter(Class<?> clazz) {

        Type genericSuperclass = clazz.getGenericSuperclass();
        if (genericSuperclass instanceof Class) {
            // 能满足这个条件的，例如 GenericTypeSupportedInHierarchiesTestCase.CustomStringTypeHandler 这个类
            // try to climb up the hierarchy until meet something useful
            if (TypeReference.class != genericSuperclass) { // 排除 TypeReference 类
                return getSuperclassTypeParameter(clazz.getSuperclass());
            }

            throw new TypeException("'" + getClass() + "' extends TypeReference but misses the type parameter. "
                    + "Remove the extension or add a type parameter to it.");
        }

        // 不是class就是ParameterizedType，直接获取第一个参数
        Type rawType = ((ParameterizedType) genericSuperclass).getActualTypeArguments()[0];
        // TODO remove this when Reflector is fixed to return Types
        // 如果第一个参数是ParameterizedType就获取rawType，没有做递归处理，也没有处理TypeVariable的情况
        if (rawType instanceof ParameterizedType) {
            rawType = ((ParameterizedType) rawType).getRawType();
        }

        return rawType;
    }

    public final Type getRawType() {
        return rawType;
    }

    @Override
    public String toString() {
        return rawType.toString();
    }

}