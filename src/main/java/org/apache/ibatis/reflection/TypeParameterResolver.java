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


import java.lang.reflect.*;
import java.util.Arrays;

/**
 * 类型参数解析器：java中使用泛型，对于泛型的填充是在编译其通过泛型檫除，桥方法等进行填充的，
 * 但是如下情况我们通过反射获取带有泛型的父类的一些成员信息的时候，获得的成员泛型是没有被填充的:
 * public interface Father<E>{
 *     E e;
 * }
 * public class Son implements Father<Integer>{
 *
 * }
 * 我们直接通过Son.class.getField(e)得到的从父类继承的属性的类型还是E，TypeVariables，但是
 * 实际上应该是Integer，{@link TypeParameterResolver} 就是来解决这种泛型填充问题。
 *
 * Type的子类有：Class、ParameterizedType、GenericArrayType、TypeVariable、WildcardType<br>
 * 关于Type的Example看：{@link TypeTest}<br>
 * 当在父类中使用了泛型的时候，在子类中可能会对父类的泛型参数进行赋值，这样在正常对子类进行使用
 * 的情况下（即非反射），子类中对应位置的父类的泛型中的类型参数值就确定了，该泛型在子类中就有了
 * 一个确定的类型值。但是在反射中就不行了，由于子类是不能直接获取父类的Fields或者Methods的，所以
 * 需要调用子类的getSuperClass获取父类的Class对象，然后再获取父类的相关Fields或者Methods，之后
 * 获取子类中进行赋值了的泛型参数，然后根据该参数位置对应到父类的泛型参数，然后再对应到获取到的
 * 父类的相关Fields或者Methods上，如果位置能对应上，就将该子类的泛型参数值赋值给父类中对应Fields
 * 或者Methods的泛型参数。
 *
 * @author Iwao AVE!
 */
public class TypeParameterResolver {

    /**
     * 解析Field中的泛型
     * @param field 要解析的Field
     * @param srcType 解析Field的起始点（可能填充了泛型）
     * @return The field type as {@link Type}. If it has type parameters in the declaration,<br>
     * they will be resolved to the actual runtime {@link Type}s.
     */
    public static Type resolveFieldType(Field field, Type srcType) {
        Type fieldType = field.getGenericType();
        Class<?> declaringClass = field.getDeclaringClass();
        return resolveType(fieldType, srcType, declaringClass);
    }

    /**
     * 解析方法返回值中的泛型
     * @param method  要解析的的方法类对象
     * @param srcType 解析方法的起始点（可能填充了泛型）
     * @return The return type of the method as {@link Type}. If it has type parameters in the declaration,<br>
     * they will be resolved to the actual runtime {@link Type}s.
     */
    public static Type resolveReturnType(Method method, Type srcType) {
        Type returnType = method.getGenericReturnType();
        Class<?> declaringClass = method.getDeclaringClass();
        return resolveType(returnType, srcType, declaringClass);
    }

    /**
     * 解析方法参数的泛型
     * @param method 要解析的方法
     * @param srcType 解析方法的起始点（可能填充了泛型）
     * @return The parameter types of the method as an array of {@link Type}s. If they have type parameters in the declaration,<br>
     * they will be resolved to the actual runtime {@link Type}s.
     */
    public static Type[] resolveParamTypes(Method method, Type srcType) {
        Type[] paramTypes = method.getGenericParameterTypes();
        Class<?> declaringClass = method.getDeclaringClass();
        Type[] result = new Type[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            result[i] = resolveType(paramTypes[i], srcType, declaringClass);
        }
        return result;
    }

    /**
     * 解析Type中的泛型
     * @param type          要解析的Type对象（Field的类型、Method的参数类型，返回值类型）
     * @param srcType       解析Type的起始点（可能填充了泛型）
     * @param declaringClass 该Type对象声明的位置（类）
     * @return 返回解析之后的类型
     */
    private static Type resolveType(Type type, Type srcType, Class<?> declaringClass) {
        if (type instanceof TypeVariable) {
            return resolveTypeVar((TypeVariable<?>) type, srcType, declaringClass);
        } else if (type instanceof ParameterizedType) {
            return resolveParameterizedType((ParameterizedType) type, srcType, declaringClass);
        } else if (type instanceof GenericArrayType) {
            return resolveGenericArrayType((GenericArrayType) type, srcType, declaringClass);
        } else {
            return type;
        }
    }

    /**
     * 解析泛型数组类型
     * @param genericArrayType 要解析的泛型数组类型
     * @param srcType 解析该泛型数组类型的起始点（可能填充了泛型）
     * @param declaringClass 该泛型数组类型声明的位置
     * @return 返回解析之后的类型
     */
    private static Type resolveGenericArrayType(GenericArrayType genericArrayType, Type srcType, Class<?> declaringClass) {
        Type componentType = genericArrayType.getGenericComponentType();
        Type resolvedComponentType = null;
        if (componentType instanceof TypeVariable) {
            resolvedComponentType = resolveTypeVar((TypeVariable<?>) componentType, srcType, declaringClass);
        } else if (componentType instanceof GenericArrayType) {
            resolvedComponentType = resolveGenericArrayType((GenericArrayType) componentType, srcType, declaringClass);
        } else if (componentType instanceof ParameterizedType) {
            resolvedComponentType = resolveParameterizedType((ParameterizedType) componentType, srcType, declaringClass);
        }
        if (resolvedComponentType instanceof Class) {
            return Array.newInstance((Class<?>) resolvedComponentType, 0).getClass();
        } else {
            return new GenericArrayTypeImpl(resolvedComponentType);
        }
    }

    /**
     * 解析参数化类型
     * @param parameterizedType 要解析的参数化类型对象
     * @param srcType 解析该参数化类型对象的起始点
     * @param declaringClass 该参数化类型对象声明的地方
     * @return 返回解析之后的参数化类型
     */
    private static ParameterizedType resolveParameterizedType(ParameterizedType parameterizedType, Type srcType, Class<?> declaringClass) {
        Class<?> rawType = (Class<?>) parameterizedType.getRawType();
        //rawType不可能带有类型参数，只会是Class，不会是ParameterizedType、GenericArrayType、TypeVariable或者WildcardType，只解决actualTypeArguments即可
        Type[] typeArgs = parameterizedType.getActualTypeArguments();
        Type[] args = new Type[typeArgs.length];
        for (int i = 0; i < typeArgs.length; i++) {
            if (typeArgs[i] instanceof TypeVariable) {
                args[i] = resolveTypeVar((TypeVariable<?>) typeArgs[i], srcType, declaringClass);
            } else if (typeArgs[i] instanceof ParameterizedType) {
                args[i] = resolveParameterizedType((ParameterizedType) typeArgs[i], srcType, declaringClass);
            } else if (typeArgs[i] instanceof WildcardType) {
                args[i] = resolveWildcardType((WildcardType) typeArgs[i], srcType, declaringClass);
            } else {
                //给泛型赋值有两种方式，一种是在声明带泛型类型的变量的时候对泛型进行赋值（这种类型是直接确定了）；
                // 另一种就是在继承或者实现带泛型父类或者父接口的时候进行赋值，这里是第一种（这种就需要解析了，应为在子类是直接获取不到父类的成员的，
                // 所以在子类的.java文件中对父类进行赋值，在父类中的成员泛型是不会直接接收这个值的）。
                //这里是第一种，泛型已经确定，不用进行解析
                args[i] = typeArgs[i];
            }
        }
        return new ParameterizedTypeImpl(rawType, null, args);
    }

    /**
     * 解析通配符类型
     * @param wildcardType 要解析的通配符类型
     * @param srcType 解析该通配符类型的起始点
     * @param declaringClass 该通配符类型声明的地方
     * @return 解析之后的类型
     */
    private static Type resolveWildcardType(WildcardType wildcardType, Type srcType, Class<?> declaringClass) {
        Type[] lowerBounds = resolveWildcardTypeBounds(wildcardType.getLowerBounds(), srcType, declaringClass);
        Type[] upperBounds = resolveWildcardTypeBounds(wildcardType.getUpperBounds(), srcType, declaringClass);
        return new WildcardTypeImpl(lowerBounds, upperBounds);
    }

    /**
     * 解析通配符类型的边界
     * @param bounds 边界类型数组
     * @param srcType 该通配符类型的解析起始点
     * @param declaringClass 该通配符类型的声明点
     * @return 返回解析之后的通配符类型边界
     */
    private static Type[] resolveWildcardTypeBounds(Type[] bounds, Type srcType, Class<?> declaringClass) {
        Type[] result = new Type[bounds.length];
        for (int i = 0; i < bounds.length; i++) {
            if (bounds[i] instanceof TypeVariable) {
                result[i] = resolveTypeVar((TypeVariable<?>) bounds[i], srcType, declaringClass);
            } else if (bounds[i] instanceof ParameterizedType) {
                result[i] = resolveParameterizedType((ParameterizedType) bounds[i], srcType, declaringClass);
            } else if (bounds[i] instanceof WildcardType) {
                result[i] = resolveWildcardType((WildcardType) bounds[i], srcType, declaringClass);
            } else {
                result[i] = bounds[i];
            }
        }
        return result;
    }

    /**
     * 填充泛型参数：<br>
     * 来到这个方法，涉及的类只会是由Class和Class组成的ParameterizedType、Class和TypeVariable组成的ParameterizedType、Class<br>
     * 1.  如果调用当前泛型参数的类就是声明该泛型参数的类，那么就返回该泛型参数的上边界对应的Class，没有上边界则返回Object.class<br>
     * 2.  如果当前泛型参数的类不是声明该泛型参数的类，就往上一层一层（resolveTypeVar中调用scanSuperTypes，然后scanSuperTypes再调用resolveTYpeVar）检查父类或者父接口，直到找到是ParameterizedType的父类<br>
     * 2.1 逐个检查ParameterizedType父类中的所有actual type arguments，如果全部检查完了还是没有匹配到与当前TypeVariable对应的，就检查当前类或者接口的父类或者父接口（回到2步骤）<br>
     * 2.1.1   当前argument是TypeVariable类型：<br>
     * 2.1.1.1   检查要解决的TypeVariable是否是同一个<br>
     * 2.1.1.1.1   如果是，说明该TypeVariable还未赋值，直接返回它的上边界或者Object.class<br>
     * 2.1.1.1.2   如果不是，就检查下一个actual type argument（回到2.1步骤）<br>
     * 2.1.1.2   如果不是同一个TypeVariable，检查下一个actual type argument（回到2.1步骤）<br>
     * 2.1.2   当前argument是Class类型：<br>
     * 2.1.2.1     检查要解决的TypeVariable是否对应该Class<br>
     * 2.1.2.1.1   如果是，就返回该Class<br>
     * 2.1.2.1.2   如果不是，就检查下一个actual type argument（回到2.1步骤）<br>
     * 2.1.2.2     如果当前argument不对应该TypeVariable，就检查下一个actual type argument（回到2.1步骤）<br>
     *
     * @param typeVar        要填充泛型参数的TypeVariable对象
     * @param srcType        检查该TypeVariable对象的起始类
     * @param declaringClass 声明该TypeVariable的地方（类）
     * @return 返回一个具体的Class（填充泛型参数完毕）
     */
    private static Type resolveTypeVar(TypeVariable<?> typeVar, Type srcType, Class<?> declaringClass) {
        Type result = null;
        Class<?> clazz = null;
        if (srcType instanceof Class) {
            clazz = (Class<?>) srcType;
        } else if (srcType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) srcType;
            clazz = (Class<?>) parameterizedType.getRawType();
        } else {
            throw new IllegalArgumentException("The 2nd arg must be Class or ParameterizedType, but was: " + srcType.getClass());
        }

        //该情况是typeVar当前所处的Type就是它所被声明的Type处，没有任何继承关系，返回Object.class
        if (clazz == declaringClass) {
            Type[] bounds = typeVar.getBounds();
            if (bounds.length > 0) {
                return bounds[0];
            }
            return Object.class;
        }

        //来到这里，说明调用typeVar和声明typeVar不是同一个类，那么声明typeVar的类只会是调用类的父类
        Type superclass = clazz.getGenericSuperclass();
        result = scanSuperTypes(typeVar, srcType, declaringClass, clazz, superclass);
        if (result != null) {
            return result;
        }

        Type[] superInterfaces = clazz.getGenericInterfaces();
        for (Type superInterface : superInterfaces) {
            result = scanSuperTypes(typeVar, srcType, declaringClass, clazz, superInterface);
            if (result != null) {
                return result;
            }
        }
        return Object.class;
    }

    /**
     * @param typeVar        需要解决的泛型
     * @param srcType        检查该TypeVariable的起始类
     * @param declaringClass 该TypeVariable声明的Type处
     * @param clazz          如果检查该泛型的起始Type是Class，那么就是该Class；否则该Type就应该是ParameterizedType，对应的这里的clazz变量就是parameterizedType.getRawType()
     * @param superclass     当前父类的Type
     * @return 返回一个具体的Class
     */
    private static Type scanSuperTypes(TypeVariable<?> typeVar, Type srcType, Class<?> declaringClass, Class<?> clazz, Type superclass) {
        //类的泛型只会是ParameterizedType、TypeVariable、WildcardType和Class的组合。而含有WildcardType是不可能来到这里的
        //当前父类是否是ParameterizedType，如果是：就进行检查；如果不是，说明该类型变量不是在当前父类中进行声明的
        if (superclass instanceof ParameterizedType) {
            ParameterizedType parentAsType = (ParameterizedType) superclass;
            Class<?> parentAsClass = (Class<?>) parentAsType.getRawType();
            TypeVariable<?>[] parentTypeVars = parentAsClass.getTypeParameters();
            if (srcType instanceof ParameterizedType) {
                //这里是为了解析那种子类在继承父类的时候，子类在类名位置声明了泛型，可能其在给后面继承的父类泛型赋值的时候引用了它自己声明的泛型
                parentAsType = translateParentTypeVars((ParameterizedType) srcType, clazz, parentAsType);
            }
            //给泛型赋值有两种方式，一种是在声明带泛型类型的变量的时候对泛型进行赋值（这种类型是直接确定了）；
            // 另一种就是在继承或者实现带泛型父类或者父接口的时候进行赋值，这里是第一种（这种就需要解析了，应为在子类是直接获取不到父类的成员的，
            // 所以在子类的.java文件中对父类进行赋值，在父类中的成员泛型是不会直接接收这个值的）。
            //这里是第二种，需要对父类进行解析，先是检查当前类型变量是和父类中声明的哪个类型变量相等的，然后取它的索引位置，再按照这个索引位置取子类中给父类赋值的相应的值
            if (declaringClass == parentAsClass) {
                for (int i = 0; i < parentTypeVars.length; i++) {
                    if (typeVar == parentTypeVars[i]) {
                        return parentAsType.getActualTypeArguments()[i];
                    }
                }
            }
            if (declaringClass.isAssignableFrom(parentAsClass)) {
                return resolveTypeVar(typeVar, parentAsType, declaringClass);
            }
            /*
             * 来到这个else if，检查当前父类是否是Class：
             *      1.如果是，再看当前父类是否是声明该VariableType的类的子孙类：
             *          1.1. 如果是，就将当前父类作为检查的起始类传入resolveTypeVar()。（递归）
             *          1.2. 如果不是，说明该TypeVariable，不是所继承的父类体系中进行声明的
             *      2. 如果不是，说明该TypeVariable，不是所继承的父类体系中进行声明的
             */
        } else if (superclass instanceof Class && declaringClass.isAssignableFrom((Class<?>) superclass)) {
            return resolveTypeVar(typeVar, superclass, declaringClass);
        }
        //来到这里，说明该TypeVariable并不在父类的体系中进行声明的，返回null
        return null;
    }

    /**
     * 解析子类继承或者实现父类父接口的时候将自己声明类型参数进行父类的类型参数赋值的情况
     * @param srcType    检查TypeVariable的起始类
     * @param srcClass   检查TypeVariable的起始Class（Class本身或者ParameterizedType的getRawType()）
     * @param parentType 当前检查起始类的父类（ParameterizedType）
     * @return
     */
    private static ParameterizedType translateParentTypeVars(ParameterizedType srcType, Class<?> srcClass, ParameterizedType parentType) {
        Type[] parentTypeArgs = parentType.getActualTypeArguments();
        Type[] srcTypeArgs = srcType.getActualTypeArguments();
        //getTypeParameters，通过Class对象获取ParameterizedType的actual type arguments，相当于调用ParameterizedType的getActualTypeArguments，如果没有就返回空
        TypeVariable<?>[] srcTypeVars = srcClass.getTypeParameters();
        Type[] newParentArgs = new Type[parentTypeArgs.length];
        boolean noChange = true;
        for (int i = 0; i < parentTypeArgs.length; i++) {
            if (parentTypeArgs[i] instanceof TypeVariable) {
                for (int j = 0; j < srcTypeVars.length; j++) {
                    if (srcTypeVars[j] == parentTypeArgs[i]) {
                        noChange = false;
                        newParentArgs[i] = srcTypeArgs[j];
                    }
                }
            } else {
                newParentArgs[i] = parentTypeArgs[i];
            }
        }
        return noChange ? parentType : new ParameterizedTypeImpl((Class<?>) parentType.getRawType(), null, newParentArgs);
    }

    private TypeParameterResolver() {
        super();
    }

    static class ParameterizedTypeImpl implements ParameterizedType {
        private Class<?> rawType;

        private Type ownerType;

        private Type[] actualTypeArguments;

        public ParameterizedTypeImpl(Class<?> rawType, Type ownerType, Type[] actualTypeArguments) {
            super();
            this.rawType = rawType;
            this.ownerType = ownerType;
            this.actualTypeArguments = actualTypeArguments;
        }

        @Override
        public Type[] getActualTypeArguments() {
            return actualTypeArguments;
        }

        @Override
        public Type getOwnerType() {
            return ownerType;
        }

        @Override
        public Type getRawType() {
            return rawType;
        }

        @Override
        public String toString() {
            return "ParameterizedTypeImpl [rawType=" + rawType + ", ownerType=" + ownerType + ", actualTypeArguments=" + Arrays.toString(actualTypeArguments) + "]";
        }
    }

    static class WildcardTypeImpl implements WildcardType {
        private Type[] lowerBounds;

        private Type[] upperBounds;

        WildcardTypeImpl(Type[] lowerBounds, Type[] upperBounds) {
            super();
            this.lowerBounds = lowerBounds;
            this.upperBounds = upperBounds;
        }

        @Override
        public Type[] getLowerBounds() {
            return lowerBounds;
        }

        @Override
        public Type[] getUpperBounds() {
            return upperBounds;
        }
    }

    static class GenericArrayTypeImpl implements GenericArrayType {
        private Type genericComponentType;

        GenericArrayTypeImpl(Type genericComponentType) {
            super();
            this.genericComponentType = genericComponentType;
        }

        @Override
        public Type getGenericComponentType() {
            return genericComponentType;
        }
    }
}
