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
package org.apache.ibatis.binding;

import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.MapKey;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.mapping.MappedStatement;
import org.apache.ibatis.mapping.SqlCommandType;
import org.apache.ibatis.mapping.StatementType;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ParamNameResolver;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.session.SqlSession;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Mapper 方法。在 Mapper 接口中，每个定义的方法，对应一个 MapperMethod 对象
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 * @author Lasse Voss
 * @author Kazuki Shimizu
 */
public class MapperMethod {

    /**
     * {@link SqlCommand} 对象
     */
    private final SqlCommand command;
    /**
     * {@link MethodSignature} 对象
     */
    private final MethodSignature method;

    /**
     * <ol>
     *     <li>调用构造器{@link SqlCommand#SqlCommand(Configuration, Class, Method)}传入{@code config}、{@code mapperInterface}、{@code method} 三个参数，构造一个{@link SqlCommand}对象并赋值到{@link #command}</li>
     * </ol>
     *
     * @param mapperInterface mapper接口类对象{@link Class}
     * @param method 方法对象{@link Method}
     * @param config 全局{@link Configuration}对象
     */
    public MapperMethod(Class<?> mapperInterface, Method method, Configuration config) {
        this.command = new SqlCommand(config, mapperInterface, method);
        this.method = new MethodSignature(config, mapperInterface, method);
    }

    public Object execute(SqlSession sqlSession, Object[] args) {
        Object result;
        switch (command.getType()) {
            case INSERT: {
                // 转换参数
                Object param = method.convertArgsToSqlCommandParam(args);
                // 执行 INSERT 操作
                // 转换 rowCount
                result = rowCountResult(sqlSession.insert(command.getName(), param));
                break;
            }
            case UPDATE: {
                // 转换参数
                Object param = method.convertArgsToSqlCommandParam(args);
                // 转换 rowCount
                result = rowCountResult(sqlSession.update(command.getName(), param));
                break;
            }
            case DELETE: {
                // 转换参数
                Object param = method.convertArgsToSqlCommandParam(args);
                // 转换 rowCount
                result = rowCountResult(sqlSession.delete(command.getName(), param));
                break;
            }
            case SELECT:
                // 无返回，并且有 ResultHandler 方法参数，则将查询的结果，提交给 ResultHandler 进行处理
                if (method.returnsVoid() && method.hasResultHandler()) {
                    executeWithResultHandler(sqlSession, args);
                    result = null;
                // 执行查询，返回列表
                } else if (method.returnsMany()) {
                    result = executeForMany(sqlSession, args);
                // 执行查询，返回 Map
                } else if (method.returnsMap()) {
                    result = executeForMap(sqlSession, args);
                // 执行查询，返回 Cursor
                } else if (method.returnsCursor()) {
                    result = executeForCursor(sqlSession, args);
                // 执行查询，返回单个对象
                } else {
                    // 转换参数
                    Object param = method.convertArgsToSqlCommandParam(args);
                    // 查询单条
                    result = sqlSession.selectOne(command.getName(), param);
                    if (method.returnsOptional() &&
                            (result == null || !method.getReturnType().equals(result.getClass()))) {
                        result = Optional.ofNullable(result);
                    }
                }
                break;
            case FLUSH:
                result = sqlSession.flushStatements();
                break;
            default:
                throw new BindingException("Unknown execution method for: " + command.getName());
        }
        // 返回结果为 null ，并且返回类型为基本类型，则抛出 BindingException 异常
        if (result == null && method.getReturnType().isPrimitive() && !method.returnsVoid()) {
            throw new BindingException("Mapper method '" + command.getName()
                    + " attempted to return null from a method with a primitive return type (" + method.getReturnType() + ").");
        }
        // 返回结果
        return result;
    }

    private Object rowCountResult(int rowCount) {
        final Object result;
        if (method.returnsVoid()) { // Void 情况，不用返回
            result = null;
        } else if (Integer.class.equals(method.getReturnType()) || Integer.TYPE.equals(method.getReturnType())) { // Int
            result = rowCount;
        } else if (Long.class.equals(method.getReturnType()) || Long.TYPE.equals(method.getReturnType())) { // Long
            result = (long) rowCount;
        } else if (Boolean.class.equals(method.getReturnType()) || Boolean.TYPE.equals(method.getReturnType())) { // Boolean
            result = rowCount > 0;
        } else {
            throw new BindingException("Mapper method '" + command.getName() + "' has an unsupported return type: " + method.getReturnType());
        }
        return result;
    }

    private void executeWithResultHandler(SqlSession sqlSession, Object[] args) {
        // 获得 MappedStatement 对象
        MappedStatement ms = sqlSession.getConfiguration().getMappedStatement(command.getName());
        if (!StatementType.CALLABLE.equals(ms.getStatementType()) // 校验存储过程的情况。不符合，抛出 BindingException 异常
                && void.class.equals(ms.getResultMaps().get(0).getType())) {
            throw new BindingException("method " + command.getName()
                    + " needs either a @ResultMap annotation, a @ResultType annotation,"
                    + " or a resultType attribute in XML so a ResultHandler can be used as a parameter.");
        }
        // 转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行 SELECT 操作
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            sqlSession.select(command.getName(), param, rowBounds, method.extractResultHandler(args));
        } else {
            sqlSession.select(command.getName(), param, method.extractResultHandler(args));
        }
    }

    private <E> Object executeForMany(SqlSession sqlSession, Object[] args) {
        List<E> result;
        // 转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行 SELECT 操作
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            result = sqlSession.selectList(command.getName(), param, rowBounds);
        } else {
            result = sqlSession.selectList(command.getName(), param);
        }
        // issue #510 Collections & arrays support
        // 封装 Array 或 Collection 结果
        if (!method.getReturnType().isAssignableFrom(result.getClass())) {
            if (method.getReturnType().isArray()) { // 情况一，Array
                return convertToArray(result);
            } else {
                return convertToDeclaredCollection(sqlSession.getConfiguration(), result); // 情况二，Collection
            }
        }
        // 直接返回的结果
        return result; // 情况三，默认
    }

    private <T> Cursor<T> executeForCursor(SqlSession sqlSession, Object[] args) {
        Cursor<T> result;
        // 转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行 SELECT 操作
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            result = sqlSession.selectCursor(command.getName(), param, rowBounds);
        } else {
            result = sqlSession.selectCursor(command.getName(), param);
        }
        return result;
    }

    private <E> Object convertToDeclaredCollection(Configuration config, List<E> list) {
        Object collection = config.getObjectFactory().create(method.getReturnType());
        MetaObject metaObject = config.newMetaObject(collection);
        metaObject.addAll(list);
        return collection;
    }

    @SuppressWarnings("unchecked")
    private <E> Object convertToArray(List<E> list) {
        Class<?> arrayComponentType = method.getReturnType().getComponentType();
        Object array = Array.newInstance(arrayComponentType, list.size());
        if (arrayComponentType.isPrimitive()) {
            for (int i = 0; i < list.size(); i++) {
                Array.set(array, i, list.get(i));
            }
            return array;
        } else {
            return list.toArray((E[]) array);
        }
    }

    private <K, V> Map<K, V> executeForMap(SqlSession sqlSession, Object[] args) {
        Map<K, V> result;
        // 转换参数
        Object param = method.convertArgsToSqlCommandParam(args);
        // 执行 SELECT 操作
        if (method.hasRowBounds()) {
            RowBounds rowBounds = method.extractRowBounds(args);
            result = sqlSession.<K, V>selectMap(command.getName(), param, method.getMapKey(), rowBounds);
        } else {
            result = sqlSession.<K, V>selectMap(command.getName(), param, method.getMapKey());
        }
        return result;
    }

    public static class ParamMap<V> extends HashMap<String, V> {

        private static final long serialVersionUID = -2212268410512043556L;

        @Override
        public V get(Object key) {
            if (!super.containsKey(key)) {
                throw new BindingException("Parameter '" + key + "' not found. Available parameters are " + keySet());
            }
            return super.get(key);
        }

    }

    /**
     * SQL命令（类型{@link SqlCommand#type}）
     */
    public static class SqlCommand {

        /**
         * {@link MappedStatement#getId()}
         */
        private final String name;
        /**
         * SQL 命令类型 {@link MappedStatement#getSqlCommandType()}
         */
        private final SqlCommandType type;

        /**
         * <ol>
         *     <li>调用{@code method}的{@link Method#getName()}获取方法名称，然后调用{@code method}的{@link Method#getDeclaringClass()}获取方法的声明类对象</li>
         *     <li>调用{@link #resolveMappedStatement(Class, String, Class, Configuration)}传入 {@code mapperInterface}、前一步获取的方法名称、前一步获取的方法声明类对象、{@code configuration}尝试获取该方法的{@link MappedStatement}对象</li>
         *     <li>
         *         判断前一步获取的{@link MappedStatement}对象 == null
         *         <ul>
         *             <li>
         *                 true：
         *                 <ul>
         *                     调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link Flush}.class尝试获取方法上的{@link Flush}注解对象，然后判断获取导的对象 != null
         *                     <li>true：将null赋值导{@link #name}、将{@link SqlCommandType#FLUSH}赋值到{@link #type}</li>
         *                     <li>false：直接抛出异常{@link BindingException}</li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 false：调用获取到的{@link MappedStatement}对象的{@link MappedStatement#getId()}并赋值到{@link #name}，调用{@link MappedStatement#getSqlCommandType()}并赋值到{@link #type}，
         *                 如果此时{@link #type} == {@link SqlCommandType#UNKNOWN}：直接抛出异常{@link BindingException}；否则什么也不做
         *             </li>
         *         </ul>
         *     </li>
         * </ol>
         *
         * @param configuration 全局{@link Configuration}对象
         * @param mapperInterface mapper接口类对象
         * @param method 方法对象
         */
        public SqlCommand(Configuration configuration, Class<?> mapperInterface, Method method) {
            final String methodName = method.getName();
            final Class<?> declaringClass = method.getDeclaringClass();
            MappedStatement ms = resolveMappedStatement(mapperInterface, methodName, declaringClass, configuration);
            if (ms == null) {
                if (method.getAnnotation(Flush.class) != null) {
                    name = null;
                    type = SqlCommandType.FLUSH;
                } else {
                    throw new BindingException("Invalid bound statement (not found): "
                            + mapperInterface.getName() + "." + methodName);
                }
            } else {
                name = ms.getId();
                type = ms.getSqlCommandType();
                if (type == SqlCommandType.UNKNOWN) {
                    throw new BindingException("Unknown execution method for: " + name);
                }
            }
        }

        /**
         * @return {@link #name}
         */
        public String getName() {
            return name;
        }

        /**
         * @return {@link #type}
         */
        public SqlCommandType getType() {
            return type;
        }

        /**
         * <ol>
         *     <li>
         *         调用{@code mapperInterfac}的{@link Class#getName()} 拼接 "." 拼接 {@code methodName}获得当前mapper方法对应的{@link MappedStatement}id
         *     </li>
         *     <li>
         *         调用{@code configuration}的{@link Configuration#hasStatement(String)}传入前一步拼接的{@link MappedStatement}id，判断该{@link MappedStatement}对象是否存在：
         *         <ul>
         *             <li>
         *                 存在：直接return {@code configuration}的{@link Configuration#getMappedStatement(String)}传入该id，方法结束
         *             </li>
         *             <li>
         *                 不存在：通过{@code mapperInterface}.equals({@code declaringClass})判断当前mapper接口类对象{@code mapperInterface}已经是声明该方法的类对象{@code declaringClass}（递归出口）
         *                 <ul>
         *                     <li>
         *                         是：直接return null，方法结束
         *                     </li>
         *                     <li>
         *                         否：什么也不做，继续往下走
         *                     </li>
         *                 </ul>
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         调用{@code mapperInterface}的{@link Class#getInterfaces()}获取当前mapper接口类的所有继承的接口对象数组，然后遍历迭代该数组，对于每一个迭代的{@link Class}对象：
         *         <ul>
         *             通过"{@code declaringClass}.isAssignableFrom(当前迭代的{@link Class}对象)"判断当前迭代的{@link Class}对象是否为声明了该方法的{@link Class}对象的子孙类
         *             <li>
         *                 是：
         *                 <ol>
         *                     <li>
         *                         调用本方法{@link #resolveMappedStatement(Class, String, Class, Configuration)}传入 "当前迭代的接口{@link Class}对象"、{@code methodName}、{@code declaringClass}、{@code configuration} 四个参数，递归获取{@link MappedStatement}对象。
         *                     </li>
         *                     <li>
         *                         判断前面对于本方法自身递归调用返回的接口 != null
         *                         <ul>
         *                             <li>true：直接return 该接口，方法结束</li>
         *                             <li>false：什么也不做，进入循环下一轮迭代，或者循环结束</li>
         *                         </ul>
         *                     </li>
         *                 </ol>
         *             </li>
         *             <li>
         *                 否：什么也不做，进入循环下一轮迭代，或者循环结束
         *             </li>
         *         </ul>
         *     </li>
         *     <li>当前前一步的循环结束了还没结束方法，来到了这里，直接return null，方法结束</li>
         * </ol>
         *
         * @param mapperInterface mapper接口类对象
         * @param methodName 方法名{@link Method#getName()}
         * @param declaringClass 声明了该方法的类{@link Class}对象（{@link Method#getDeclaringClass()}）（注意：不一定mapper的方法就是在其接口类中声明的，可能是在父接口中声明，也有可能它对父接口中声明的方法进行了重写）
         * @param configuration 全局{@link Configuration}
         * @return
         */
        private MappedStatement resolveMappedStatement(Class<?> mapperInterface, String methodName, Class<?> declaringClass, Configuration configuration) {
            String statementId = mapperInterface.getName() + "." + methodName;
            if (configuration.hasStatement(statementId)) {
                return configuration.getMappedStatement(statementId);
            } else if (mapperInterface.equals(declaringClass)) {
                return null;
            }
            for (Class<?> superInterface : mapperInterface.getInterfaces()) {
                if (declaringClass.isAssignableFrom(superInterface)) {
                    MappedStatement ms = resolveMappedStatement(superInterface, methodName, declaringClass, configuration);
                    if (ms != null) {
                        return ms;
                    }
                }
            }
            return null;
        }

    }

    /**
     * 方法签名
     */
    public static class MethodSignature {

        /**
         * 返回类型是否为集合
         */
        private final boolean returnsMany;
        /**
         * 返回类型是否为 Map
         */
        private final boolean returnsMap;
        /**
         * 返回类型是否为 void
         */
        private final boolean returnsVoid;
        /**
         * 返回类型是否为 {@link org.apache.ibatis.cursor.Cursor}
         */
        private final boolean returnsCursor;
        /**
         * 返回类型是否为 {@link java.util.Optional}
         */
        private final boolean returnsOptional;
        /**
         * 返回类型
         */
        private final Class<?> returnType;
        /**
         * 返回方法上的 {@link MapKey#value()} ，前提是返回类型为 Map
         */
        private final String mapKey;
        /**
         * 获得 {@link ResultHandler} 在方法参数中的位置。
         *
         * 如果为 null ，说明不存在这个类型
         */
        private final Integer resultHandlerIndex;
        /**
         * 获得 {@link RowBounds} 在方法参数中的位置。
         *
         * 如果为 null ，说明不存在这个类型
         */
        private final Integer rowBoundsIndex;
        /**
         * ParamNameResolver 对象
         */
        private final ParamNameResolver paramNameResolver;

        public MethodSignature(Configuration configuration, Class<?> mapperInterface, Method method) {
            // 初始化 returnType 属性
            Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, mapperInterface);
            if (resolvedReturnType instanceof Class<?>) { // 普通类
                this.returnType = (Class<?>) resolvedReturnType;
            } else if (resolvedReturnType instanceof ParameterizedType) { // 泛型
                this.returnType = (Class<?>) ((ParameterizedType) resolvedReturnType).getRawType();
            } else { // 内部类等等
                this.returnType = method.getReturnType();
            }
            // 初始化 returnsVoid 属性
            this.returnsVoid = void.class.equals(this.returnType);
            // 初始化 returnsMany 属性
            this.returnsMany = configuration.getObjectFactory().isCollection(this.returnType) || this.returnType.isArray();
            // 初始化 returnsCursor 属性
            this.returnsCursor = Cursor.class.equals(this.returnType);
            // 初始化 returnsOptional 属性
            this.returnsOptional = Optional.class.equals(this.returnType);
            // 初始化 mapKey
            this.mapKey = getMapKey(method);
            // 初始化 returnsMap
            this.returnsMap = this.mapKey != null;
            // 初始化 rowBoundsIndex、resultHandlerIndex
            this.rowBoundsIndex = getUniqueParamIndex(method, RowBounds.class);
            this.resultHandlerIndex = getUniqueParamIndex(method, ResultHandler.class);
            // 初始化 paramNameResolver
            this.paramNameResolver = new ParamNameResolver(configuration, method);
        }

        public Object convertArgsToSqlCommandParam(Object[] args) {
            return paramNameResolver.getNamedParams(args);
        }

        public boolean hasRowBounds() {
            return rowBoundsIndex != null;
        }

        public RowBounds extractRowBounds(Object[] args) {
            return hasRowBounds() ? (RowBounds) args[rowBoundsIndex] : null;
        }

        public boolean hasResultHandler() {
            return resultHandlerIndex != null;
        }

        public ResultHandler extractResultHandler(Object[] args) {
            return hasResultHandler() ? (ResultHandler) args[resultHandlerIndex] : null;
        }

        public String getMapKey() {
            return mapKey;
        }

        public Class<?> getReturnType() {
            return returnType;
        }

        public boolean returnsMany() {
            return returnsMany;
        }

        public boolean returnsMap() {
            return returnsMap;
        }

        public boolean returnsVoid() {
            return returnsVoid;
        }

        public boolean returnsCursor() {
            return returnsCursor;
        }

        /**
         * return whether return type is {@code java.util.Optional}
         * @return return {@code true}, if return type is {@code java.util.Optional}
         * @since 3.5.0
         */
        public boolean returnsOptional() {
            return returnsOptional;
        }

        /**
         * 获得指定参数类型在方法参数中的位置
         *
         * @param method 指定方法
         * @param paramType 参数类型
         * @return 位置
         */
        private Integer getUniqueParamIndex(Method method, Class<?> paramType) {
            Integer index = null;
            // 遍历方法参数
            final Class<?>[] argTypes = method.getParameterTypes();
            for (int i = 0; i < argTypes.length; i++) {
                if (paramType.isAssignableFrom(argTypes[i])) { // 类型符合
                    // 获得第一次的位置
                    if (index == null) {
                        index = i;
                    // 如果重复类型了，则抛出 BindingException 异常
                    } else {
                        throw new BindingException(method.getName() + " cannot have multiple " + paramType.getSimpleName() + " parameters");
                    }
                }
            }
            return index;
        }

        /**
         * @param method 方法
         * @return 获得注解的 {@link MapKey#value()}
         */
        private String getMapKey(Method method) {
            String mapKey = null;
            // 返回类型为 Map
            if (Map.class.isAssignableFrom(method.getReturnType())) {
                // 使用 @MapKey 注解
                final MapKey mapKeyAnnotation = method.getAnnotation(MapKey.class);
                // 获得 @MapKey 注解的键
                if (mapKeyAnnotation != null) {
                    mapKey = mapKeyAnnotation.value();
                }
            }
            return mapKey;
        }
    }

}
