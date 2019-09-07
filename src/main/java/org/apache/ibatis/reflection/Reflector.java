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
import org.apache.ibatis.reflection.invoker.SetFieldInvoker;
import org.apache.ibatis.reflection.property.PropertyNamer;

import java.lang.reflect.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * This class represents a cached set of class definition information that
 * allows for easy mapping between property names and getter/setter methods<br>
 * <b>反射信息缓存容器</b><br>
 * TODO：MyBatis反射将父类的私有成员也进行读取到缓存中，表示为当前类的可访问对象，不知是否合理
 * @author Clinton Begin
 */
public class Reflector {

    private final Class<?> type;
    /**
     * 可读属性数组（properties或者fields）
     */
    private final String[] readablePropertyNames;
    /**
     * 可写属性数组（properties或者fields）
     */
    private final String[] writeablePropertyNames;
    /**
     * Key：属性名称（通过getter或者setter转换）或者field名称（成员变量）
     * Value：{@link MethodInvoker} || {@link SetFieldInvoker}
     */
    private final Map<String, Invoker> setMethods = new HashMap<>();
    /**
     * Key：属性名称（通过getter或者setter转换）或者field名称（成员变量）
     * Value：{@link MethodInvoker} || {@link GetFieldInvoker}
     */
    private final Map<String, Invoker> getMethods = new HashMap<>();
    /**
     * Key：属性（通过getter或者setter转换）或者field名称（成员变量）
     * Value：属性Setter的参数类型对应的Class || Field的Class
     */
    private final Map<String, Class<?>> setTypes = new HashMap<>();
    /**
     * Key：属性名称（通过getter或者setter转换）或者field名称（成员变量）
     * Value：属性对应Getter的返回值对应的Class || Field的Class
     */
    private final Map<String, Class<?>> getTypes = new HashMap<>();
    private Constructor<?> defaultConstructor;

    private Map<String, String> caseInsensitivePropertyMap = new HashMap<>();

    public Reflector(Class<?> clazz) {
        type = clazz;
        addDefaultConstructor(clazz);
        addGetMethods(clazz);
        addSetMethods(clazz);
        addFields(clazz);
        //添加可读的属性名称到readablePropertyNames
        readablePropertyNames = getMethods.keySet().toArray(new String[getMethods.keySet().size()]);
        //添加可写的属性名称到readablePropertyNames
        writeablePropertyNames = setMethods.keySet().toArray(new String[setMethods.keySet().size()]);
        //添加所有可读可写的属性到大小写名称不敏感的map中
        for (String propName : readablePropertyNames) {
            caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
        }
        for (String propName : writeablePropertyNames) {
            caseInsensitivePropertyMap.put(propName.toUpperCase(Locale.ENGLISH), propName);
        }
    }

    /**
     * 获取无参构造器作为一个默认的构造器赋值给当前Reflector对象的一个变量作为缓存
     * @param clazz
     */
    private void addDefaultConstructor(Class<?> clazz) {
        Constructor<?>[] consts = clazz.getDeclaredConstructors();
        for (Constructor<?> constructor : consts) {
            if (constructor.getParameterTypes().length == 0) {
                if (canControlMemberAccessible()) {
                    try {
                        constructor.setAccessible(true);
                    } catch (Exception e) {
                        // Ignored. This is only a final precaution, nothing we can do.
                    }
                }
                if (constructor.isAccessible()) {
                    this.defaultConstructor = constructor;
                }
            }
        }
    }

    /**
     * 添加传入类型的所有getter方法执行器和返回值类型到缓存（map）
     * @param cls 传入的类型
     */
    private void addGetMethods(Class<?> cls) {
        Map<String, List<Method>> conflictingGetters = new HashMap<>();
        Method[] methods = getClassMethods(cls);
        for (Method method : methods) {
            if (method.getParameterTypes().length > 0) {
                continue;
            }
            String name = method.getName();
            if ((name.startsWith("get") && name.length() > 3)
                    || (name.startsWith("is") && name.length() > 2)) {
                name = PropertyNamer.methodToProperty(name);
                addMethodConflict(conflictingGetters, name, method);
            }
        }
        resolveGetterConflicts(conflictingGetters);
    }

    /**
     * 当存在isProperty和getProperty的时候，解决冲突<br>
     *    Example：{@link org.apache.ibatis.test.TestEveryThing.ReflectorTest.TestMethodResolveGetterConflicts}
     * @param conflictingGetters key->属性名；value->getter方法类对象集合
     */
    private void resolveGetterConflicts(Map<String, List<Method>> conflictingGetters) {
        for (Entry<String, List<Method>> entry : conflictingGetters.entrySet()) {
            Method winner = null;
            String propName = entry.getKey();
            for (Method candidate : entry.getValue()) {
                if (winner == null) {
                    winner = candidate;
                    continue;
                }
                Class<?> winnerType = winner.getReturnType();
                Class<?> candidateType = candidate.getReturnType();
                //如果两个方法的返回值类型相同
                if (candidateType.equals(winnerType)) {
                    if (!boolean.class.equals(candidateType)) {
                        //如果两个方法的返回值类型相同且不是boolean类型的时候抛出异常
                        throw new ReflectionException(
                                "Illegal overloaded getter method with ambiguous type for property "
                                        + propName + " in class " + winner.getDeclaringClass()
                                        + ". This breaks the JavaBeans specification and can cause unpredictable results.");
                    } else if (candidate.getName().startsWith("is")) {
                        //如果两个方法的返回值类型相同且两者的类型是boolean的时候，取方法名称是is开头的那一个，如果不是candidate就是winner
                        winner = candidate;
                    }
                    //以下的else if都是getter返回值不同的时候，取两者中可以代表(isAssignableFrom)另一者的一者（即更加具体的，继承体系中的子孙辈），否则抛出异常
                } else if (candidateType.isAssignableFrom(winnerType)) {
                    // OK getter type is descendant
                } else if (winnerType.isAssignableFrom(candidateType)) {
                    winner = candidate;
                } else {
                    throw new ReflectionException(
                            "Illegal overloaded getter method with ambiguous type for property "
                                    + propName + " in class " + winner.getDeclaringClass()
                                    + ". This breaks the JavaBeans specification and can cause unpredictable results.");
                }
            }
            addGetMethod(propName, winner);
        }
    }

    /**
     * 添加单个getter执行器到缓存
     * @param name 属性名称
     * @param method getter方法类对象
     */
    private void addGetMethod(String name, Method method) {
        if (isValidPropertyName(name)) {
            getMethods.put(name, new MethodInvoker(method));
            //因为在前面获取getter的步骤是获取所有当前类及父辈的getter，所以可能会存在从父辈继承的getter带有泛型的情况，需要解决
            Type returnType = TypeParameterResolver.resolveReturnType(method, type);
            getTypes.put(name, typeToClass(returnType));
        }
    }

    /**
     * 添加Setter缓存
     * @param cls
     */
    private void addSetMethods(Class<?> cls) {
        Map<String, List<Method>> conflictingSetters = new HashMap<>();
        Method[] methods = getClassMethods(cls);
        for (Method method : methods) {
            String name = method.getName();
            if (name.startsWith("set") && name.length() > 3) {
                //方法参数列表必须有且仅有一个参数
                if (method.getParameterTypes().length == 1) {
                    name = PropertyNamer.methodToProperty(name);
                    addMethodConflict(conflictingSetters, name, method);
                }
            }
        }
        resolveSetterConflicts(conflictingSetters);
    }

    /**
     * 将getter/setter对应的属性名称作为key，getter/setter对应的方法类对象封装到集合作为value（因为可能有getA和isA）
     * @param conflictingMethods 属性名->方法类对象集合（getA/isA）
     * @param name 属性名
     * @param method 方法类对象
     */
    private void addMethodConflict(Map<String, List<Method>> conflictingMethods, String name, Method method) {
        // TODO：k->new ArrayList<>() 没有就新建
        List<Method> list = conflictingMethods.computeIfAbsent(name, k -> new ArrayList<>());
        list.add(method);
    }

    /**
     * 解决setter冲突
     * @param conflictingSetters
     */
    private void resolveSetterConflicts(Map<String, List<Method>> conflictingSetters) {
        for (String propName : conflictingSetters.keySet()) {
            List<Method> setters = conflictingSetters.get(propName);
            //从getTypes缓存中获取该属性的类型
            Class<?> getterType = getTypes.get(propName);
            Method match = null;
            ReflectionException exception = null;
            for (Method setter : setters) {
                Class<?> paramType = setter.getParameterTypes()[0];
                if (paramType.equals(getterType)) {
                    //如果当前setter唯一的方法参数类型与该属性的getter返回值类型一致，那么当前setter就是对应属性的最佳匹配，直接中断循环返回
                    // should be the best match
                    match = setter;
                    break;
                }
                if (exception == null) {
                    try {
                        match = pickBetterSetter(match, setter, propName);
                    } catch (ReflectionException e) {
                        // there could still be the 'best match'
                        match = null;
                        //这里抓住异常，先不抛出，因为可能有不止两个setter对应同一个property，循环处理完所有property名称相同的setter之后，如果match还是null的，再抛出这个异常
                        exception = e;
                    }
                }
            }
            if (match == null) {
                throw exception;
            } else {
                addSetMethod(propName, match);
            }
        }
    }

    /**
     * 选择最佳的setter
     * @param setter1
     * @param setter2
     * @param property
     * @return
     */
    private Method pickBetterSetter(Method setter1, Method setter2, String property) {
        if (setter1 == null) {
            return setter2;
        }
        Class<?> paramType1 = setter1.getParameterTypes()[0];
        Class<?> paramType2 = setter2.getParameterTypes()[0];
        //如果两个setter的参数有继承关系，返回参数类型最具体的那个setter
        if (paramType1.isAssignableFrom(paramType2)) {
            return setter2;
        } else if (paramType2.isAssignableFrom(paramType1)) {
            return setter1;
        }
        //如果两个setter的参数没有继承关系，直接抛出异常
        throw new ReflectionException("Ambiguous setters defined for property '" + property + "' in class '"
                + setter2.getDeclaringClass() + "' with types '" + paramType1.getName() + "' and '"
                + paramType2.getName() + "'.");
    }

    /**
     * 添加setter到缓存
     * @param name 属性名称
     * @param method setter
     */
    private void addSetMethod(String name, Method method) {
        if (isValidPropertyName(name)) {
            //添加setter invoker到缓存
            setMethods.put(name, new MethodInvoker(method));
            Type[] paramTypes = TypeParameterResolver.resolveParamTypes(method, type);
            //添加setter的参数类型到缓存
            setTypes.put(name, typeToClass(paramTypes[0]));
        }
    }

    /**
     * 转换Type类型位Class：<br>
     * 1. 如果是Class类型，直接强转<br>
     * 2. 如果是ParameterizedType，或者rawType<br>
     * 3. 如果是genericArrayType，递归获取componentType进行数组创建<br>
     * 4. 如果以上情况有漏网之鱼，转成Object.class<br>
     * @param src  要转换的Type
     * @return 转换之后的Class
     */
    private Class<?> typeToClass(Type src) {
        Class<?> result = null;
        if (src instanceof Class) {
            result = (Class<?>) src;
        } else if (src instanceof ParameterizedType) {
            result = (Class<?>) ((ParameterizedType) src).getRawType();
        } else if (src instanceof GenericArrayType) {
            Type componentType = ((GenericArrayType) src).getGenericComponentType();
            if (componentType instanceof Class) {
                result = Array.newInstance((Class<?>) componentType, 0).getClass();
            } else {
                Class<?> componentClass = typeToClass(componentType);
                result = Array.newInstance((Class<?>) componentClass, 0).getClass();
            }
        }
        if (result == null) {
            result = Object.class;
        }
        return result;
    }

    /**
     * 添加clazz的fields到缓存
     * @param clazz
     */
    private void addFields(Class<?> clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (canControlMemberAccessible()) {
                try {
                    field.setAccessible(true);
                } catch (Exception e) {
                    // Ignored. This is only a final precaution, nothing we can do.
                }
            }
            if (field.isAccessible()) {
                //如果setter map已经有了这个field的setter（MethodInvoker），就不用添加FieldInvoker了，有限通过Setter访问
                if (!setMethods.containsKey(field.getName())) {
                    // issue #379 - removed the check for final because JDK 1.5 allows
                    // modification of final fields through reflection (JSR-133). (JGB)
                    // pr #16 - final static can only be set by the classloader
                    //返回这个field的修饰符编码
                    int modifiers = field.getModifiers();
                    if (!(Modifier.isFinal(modifiers) && Modifier.isStatic(modifiers))) {
                        //不能同时是static或者final的（即编译期就需要确定值的field，如果编译器没有赋值，运行期就会报错），所以不用添加field的setInvoker了
                        addSetField(field);
                    }
                }
                //如果getter map已经有了这个field的getter（MethodInvoker），就不用添加FieldInvoker了，有限通过Getter访问
                if (!getMethods.containsKey(field.getName())) {
                    addGetField(field);
                }
            }
        }
        if (clazz.getSuperclass() != null) {
            //添加父类所有FIeld到缓存
            addFields(clazz.getSuperclass());
        }
    }

    /**
     * 添加field的SetInvoker和类型到缓存
     * @param field
     */
    private void addSetField(Field field) {
        if (isValidPropertyName(field.getName())) {
            setMethods.put(field.getName(), new SetFieldInvoker(field));
            Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
            setTypes.put(field.getName(), typeToClass(fieldType));
        }
    }

    /**
     * 添加field的GetInvoker和类型到缓存
     * @param field
     */
    private void addGetField(Field field) {
        if (isValidPropertyName(field.getName())) {
            getMethods.put(field.getName(), new GetFieldInvoker(field));
            Type fieldType = TypeParameterResolver.resolveFieldType(field, type);
            getTypes.put(field.getName(), typeToClass(fieldType));
        }
    }

    /**
     * 检测属性名是否合法：<br>
     *     非法值："$"；"serialVersionUID"；"class"
     * @param name 属性名称（有getter转换来的）
     * @return
     */
    private boolean isValidPropertyName(String name) {
        return !(name.startsWith("$") || "serialVersionUID".equals(name) || "class".equals(name));
    }

    /**
     *
     * This method returns an array containing all methods
     * declared in this class and any superclass.
     * We use this method, instead of the simpler Class.getMethods(),
     * because we want to look for private methods as well.<br>
     * <span><b>获取当前类及其所有父类及实现接口的方法（父类和实现接口的方法是直接获取不到的）（包括不公开方法，不知是否合理）</b></span>
     * @param cls The class
     * @return An array containing all methods in this class
     *
     */
    private Method[] getClassMethods(Class<?> cls) {
        Map<String, Method> uniqueMethods = new HashMap<>();
        Class<?> currentClass = cls;
        while (currentClass != null && currentClass != Object.class) {
            addUniqueMethods(uniqueMethods, currentClass.getDeclaredMethods());

            // we also need to look for interface methods -
            // because the class may be abstract
            //获取所有接口的方法
            Class<?>[] interfaces = currentClass.getInterfaces();
            for (Class<?> anInterface : interfaces) {
                addUniqueMethods(uniqueMethods, anInterface.getMethods());
            }
            //获取当前类的父类，循环获取原始类的所有可用方法，因为存在方法重写，所以要根据一个方法签名进行父类被重写方法的去重
            currentClass = currentClass.getSuperclass();
        }

        Collection<Method> methods = uniqueMethods.values();

        return methods.toArray(new Method[methods.size()]);
    }

    /**
     * 传入一个map和方法数组，然后将数组中所有方法加入到map，key是方法的签名，value是方法类对象本身
     * @param uniqueMethods 传入的map
     * @param methods 传入的方法类对象集合
     */
    private void addUniqueMethods(Map<String, Method> uniqueMethods, Method[] methods) {
        for (Method currentMethod : methods) {
            //bridge方法（桥接）：当父类某个泛型方法被子类重写并赋值到泛型的时候由编译器在子类中自动生成的该父类方法，起到连接作用，我们不需要
            //see : https://www.cnblogs.com/zsg88/p/7588929.html
            if (!currentMethod.isBridge()) {
                String signature = getSignature(currentMethod);
                // check to see if the method is already known
                // if it is known, then an extended class must have
                // overridden a method
                //这里的意思是：这里可能会被循环获取父类的所有方法，那么就会存在方法被重写的情况，此时进行缓存的而时候需要筛掉父类被重写方法，使用一个拥有方法签名key的map来进行去重也正是此意。
                if (!uniqueMethods.containsKey(signature)) {
                    if (canControlMemberAccessible()) {
                        try {
                            currentMethod.setAccessible(true);
                        } catch (Exception e) {
                            // Ignored. This is only a final precaution, nothing we can do.
                        }
                    }

                    uniqueMethods.put(signature, currentMethod);
                }
            }
        }
    }

    /**
     * 获取方法签名
     * @param method 方法
     * @return 签名
     */
    private String getSignature(Method method) {
        StringBuilder sb = new StringBuilder();
        Class<?> returnType = method.getReturnType();
        if (returnType != null) {
            sb.append(returnType.getName()).append('#');
        }
        sb.append(method.getName());
        Class<?>[] parameters = method.getParameterTypes();
        for (int i = 0; i < parameters.length; i++) {
            if (i == 0) {
                sb.append(':');
            } else {
                sb.append(',');
            }
            sb.append(parameters[i].getName());
        }
        return sb.toString();
    }

    /**
     * Checks whether can control member accessible.
     *
     * @return If can control member accessible, it return {@literal true}
     * @since 3.5.0
     */
    public static boolean canControlMemberAccessible() {
        try {
            SecurityManager securityManager = System.getSecurityManager();
            if (null != securityManager) {
                // 暂时理解，检测是否开启了抑制检测（private不能访问） TODO：需要进一步了解关于java的security manager和一些权限相关
                securityManager.checkPermission(new ReflectPermission("suppressAccessChecks"));
            }
        } catch (SecurityException e) {
            return false;
        }
        return true;
    }

    /**
     * Gets the name of the class the instance provides information for
     *
     * @return The class name
     */
    public Class<?> getType() {
        return type;
    }

    public Constructor<?> getDefaultConstructor() {
        if (defaultConstructor != null) {
            return defaultConstructor;
        } else {
            throw new ReflectionException("There is no default constructor for " + type);
        }
    }

    public boolean hasDefaultConstructor() {
        return defaultConstructor != null;
    }

    public Invoker getSetInvoker(String propertyName) {
        Invoker method = setMethods.get(propertyName);
        if (method == null) {
            throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
        }
        return method;
    }

    public Invoker getGetInvoker(String propertyName) {
        Invoker method = getMethods.get(propertyName);
        if (method == null) {
            throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
        }
        return method;
    }

    /**
     * Gets the type for a property setter
     *
     * @param propertyName - the name of the property
     * @return The Class of the propery setter
     */
    public Class<?> getSetterType(String propertyName) {
        Class<?> clazz = setTypes.get(propertyName);
        if (clazz == null) {
            throw new ReflectionException("There is no setter for property named '" + propertyName + "' in '" + type + "'");
        }
        return clazz;
    }

    /**
     * Gets the type for a property getter
     *
     * @param propertyName - the name of the property
     * @return The Class of the propery getter
     */
    public Class<?> getGetterType(String propertyName) {
        Class<?> clazz = getTypes.get(propertyName);
        if (clazz == null) {
            throw new ReflectionException("There is no getter for property named '" + propertyName + "' in '" + type + "'");
        }
        return clazz;
    }

    /**
     * Gets an array of the readable properties for an object
     *
     * @return The array
     */
    public String[] getGetablePropertyNames() {
        return readablePropertyNames;
    }

    /**
     * Gets an array of the writeable properties for an object
     *
     * @return The array
     */
    public String[] getSetablePropertyNames() {
        return writeablePropertyNames;
    }

    /**
     * Check to see if a class has a writeable property by name
     *
     * @param propertyName - the name of the property to check
     * @return True if the object has a writeable property by the name
     */
    public boolean hasSetter(String propertyName) {
        return setMethods.keySet().contains(propertyName);
    }

    /**
     * Check to see if a class has a readable property by name
     *
     * @param propertyName - the name of the property to check
     * @return True if the object has a readable property by the name
     */
    public boolean hasGetter(String propertyName) {
        return getMethods.keySet().contains(propertyName);
    }

    /**
     * 大小写不敏感查找属性名称
     * @param name 属性名称（忽略大小写）
     * @return 准确的属性名称
     */
    public String findPropertyName(String name) {
        return caseInsensitivePropertyMap.get(name.toUpperCase(Locale.ENGLISH));
    }
}
