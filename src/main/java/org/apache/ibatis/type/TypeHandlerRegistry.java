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

import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.io.ResolverUtil;
import org.apache.ibatis.io.Resources;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.*;
import java.time.chrono.JapaneseDate;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link TypeHandler} 注册表 <br>
 * <ol>
 *     <li>
 *         {@link #TYPE_HANDLER_MAP}中定义的是<1>jdbcType的通用handler
 *     </li>
 *     <li>
 *         {@link #ALL_TYPE_HANDLERS_MAP}中定义的是<2>javaType和jdbcType的精确handler（key1和key2都有值），<3>如果key2（也就是jdbcType）是null，则为该javaType的通用handler
 *     </li>
 *     <li>
 *         <2>的优先级最高，最先找从它这里找；然后是<3>；再是<1>。参考{@link UnknownTypeHandler#resolveTypeHandler(java.sql.ResultSetMetaData, java.lang.Integer)}
 *     </li>
 * </ol>
 *
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public final class TypeHandlerRegistry {

    /**
     * 空 TypeHandler 集合的标识，即使 {@link #TYPE_HANDLER_MAP} 中，某个 KEY1 对应的 Map<JdbcType, TypeHandler<?>> 为空。
     *
     * @see #getJdbcHandlerMap(Type)
     */
    private static final Map<JdbcType, TypeHandler<?>> NULL_TYPE_HANDLER_MAP = Collections.emptyMap();

    /**
     * JDBC Type 和 {@link TypeHandler} 的映射
     *
     * {@link #register(JdbcType, TypeHandler)}
     */
    private final Map<JdbcType, TypeHandler<?>> JDBC_TYPE_HANDLER_MAP = new EnumMap<>(JdbcType.class);
    /**
     * {@link TypeHandler} 的映射
     *
     * KEY1：JDBC Type
     * KEY2：Java Type
     * VALUE：{@link TypeHandler} 对象
     */
    private final Map<Type, Map<JdbcType, TypeHandler<?>>> TYPE_HANDLER_MAP = new ConcurrentHashMap<>();
    /**
     * 所有 TypeHandler 的“集合”
     *
     * KEY：{@link TypeHandler#getClass()}
     * VALUE：{@link TypeHandler} 对象
     */
    private final Map<Class<?>, TypeHandler<?>> ALL_TYPE_HANDLERS_MAP = new HashMap<>();

    /**
     * {@link UnknownTypeHandler} 对象
     */
    private final TypeHandler<Object> UNKNOWN_TYPE_HANDLER = new UnknownTypeHandler(this);
    /**
     * 默认的枚举类型的 TypeHandler 对象
     */
    private Class<? extends TypeHandler> defaultEnumTypeHandler = EnumTypeHandler.class;

    public TypeHandlerRegistry() {
        register(Boolean.class, new BooleanTypeHandler());
        register(boolean.class, new BooleanTypeHandler());
        register(JdbcType.BOOLEAN, new BooleanTypeHandler());
        register(JdbcType.BIT, new BooleanTypeHandler());

        register(Byte.class, new ByteTypeHandler());
        register(byte.class, new ByteTypeHandler());
        register(JdbcType.TINYINT, new ByteTypeHandler());

        register(Short.class, new ShortTypeHandler());
        register(short.class, new ShortTypeHandler());
        register(JdbcType.SMALLINT, new ShortTypeHandler());

        register(Integer.class, new IntegerTypeHandler());
        register(int.class, new IntegerTypeHandler());
        register(JdbcType.INTEGER, new IntegerTypeHandler());

        register(Long.class, new LongTypeHandler());
        register(long.class, new LongTypeHandler());

        register(Float.class, new FloatTypeHandler());
        register(float.class, new FloatTypeHandler());
        register(JdbcType.FLOAT, new FloatTypeHandler());

        register(Double.class, new DoubleTypeHandler());
        register(double.class, new DoubleTypeHandler());
        register(JdbcType.DOUBLE, new DoubleTypeHandler());

        register(Reader.class, new ClobReaderTypeHandler());
        register(String.class, new StringTypeHandler());
        register(String.class, JdbcType.CHAR, new StringTypeHandler());
        register(String.class, JdbcType.CLOB, new ClobTypeHandler());
        register(String.class, JdbcType.VARCHAR, new StringTypeHandler());
        register(String.class, JdbcType.LONGVARCHAR, new ClobTypeHandler());
        register(String.class, JdbcType.NVARCHAR, new NStringTypeHandler());
        register(String.class, JdbcType.NCHAR, new NStringTypeHandler());
        register(String.class, JdbcType.NCLOB, new NClobTypeHandler());
        register(JdbcType.CHAR, new StringTypeHandler());
        register(JdbcType.VARCHAR, new StringTypeHandler());
        register(JdbcType.CLOB, new ClobTypeHandler());
        register(JdbcType.LONGVARCHAR, new ClobTypeHandler());
        register(JdbcType.NVARCHAR, new NStringTypeHandler());
        register(JdbcType.NCHAR, new NStringTypeHandler());
        register(JdbcType.NCLOB, new NClobTypeHandler());

        register(Object.class, JdbcType.ARRAY, new ArrayTypeHandler());
        register(JdbcType.ARRAY, new ArrayTypeHandler());

        register(BigInteger.class, new BigIntegerTypeHandler());
        register(JdbcType.BIGINT, new LongTypeHandler());

        register(BigDecimal.class, new BigDecimalTypeHandler());
        register(JdbcType.REAL, new BigDecimalTypeHandler());
        register(JdbcType.DECIMAL, new BigDecimalTypeHandler());
        register(JdbcType.NUMERIC, new BigDecimalTypeHandler());

        register(InputStream.class, new BlobInputStreamTypeHandler());
        register(Byte[].class, new ByteObjectArrayTypeHandler());
        register(Byte[].class, JdbcType.BLOB, new BlobByteObjectArrayTypeHandler());
        register(Byte[].class, JdbcType.LONGVARBINARY, new BlobByteObjectArrayTypeHandler());
        register(byte[].class, new ByteArrayTypeHandler());
        register(byte[].class, JdbcType.BLOB, new BlobTypeHandler());
        register(byte[].class, JdbcType.LONGVARBINARY, new BlobTypeHandler());
        register(JdbcType.LONGVARBINARY, new BlobTypeHandler());
        register(JdbcType.BLOB, new BlobTypeHandler());

        register(Object.class, UNKNOWN_TYPE_HANDLER);
        register(Object.class, JdbcType.OTHER, UNKNOWN_TYPE_HANDLER);
        register(JdbcType.OTHER, UNKNOWN_TYPE_HANDLER);

        register(Date.class, new DateTypeHandler());
        register(Date.class, JdbcType.DATE, new DateOnlyTypeHandler());
        register(Date.class, JdbcType.TIME, new TimeOnlyTypeHandler());
        register(JdbcType.TIMESTAMP, new DateTypeHandler());
        register(JdbcType.DATE, new DateOnlyTypeHandler());
        register(JdbcType.TIME, new TimeOnlyTypeHandler());

        register(java.sql.Date.class, new SqlDateTypeHandler());
        register(java.sql.Time.class, new SqlTimeTypeHandler());
        register(java.sql.Timestamp.class, new SqlTimestampTypeHandler());

        register(String.class, JdbcType.SQLXML, new SqlxmlTypeHandler());

        register(Instant.class, InstantTypeHandler.class);
        register(LocalDateTime.class, LocalDateTimeTypeHandler.class);
        register(LocalDate.class, LocalDateTypeHandler.class);
        register(LocalTime.class, LocalTimeTypeHandler.class);
        register(OffsetDateTime.class, OffsetDateTimeTypeHandler.class);
        register(OffsetTime.class, OffsetTimeTypeHandler.class);
        register(ZonedDateTime.class, ZonedDateTimeTypeHandler.class);
        register(Month.class, MonthTypeHandler.class);
        register(Year.class, YearTypeHandler.class);
        register(YearMonth.class, YearMonthTypeHandler.class);
        register(JapaneseDate.class, JapaneseDateTypeHandler.class);

        // issue #273
        register(Character.class, new CharacterTypeHandler());
        register(char.class, new CharacterTypeHandler());
    }

    /**
     * Set a default {@link TypeHandler} class for {@link Enum}.
     * A default {@link TypeHandler} is {@link org.apache.ibatis.type.EnumTypeHandler}.
     * @param typeHandler a type handler class for {@link Enum}
     * @since 3.4.5
     */
    public void setDefaultEnumTypeHandler(Class<? extends TypeHandler> typeHandler) {
        this.defaultEnumTypeHandler = typeHandler;
    }

    /**
     * 直接调用{@link #hasTypeHandler(Class, JdbcType)}，jdbcType传入null
     *
     * @param javaType
     * @return
     */
    public boolean hasTypeHandler(Class<?> javaType) {
        return hasTypeHandler(javaType, null);
    }

    /**
     * 直接调用{@link #hasTypeHandler(TypeReference, JdbcType)}，jdbcType传入null
     *
     * @param javaTypeReference
     * @return
     */
    public boolean hasTypeHandler(TypeReference<?> javaTypeReference) {
        return hasTypeHandler(javaTypeReference, null);
    }

    /**
     * {@code javaType}不为null并且{@link #getTypeHandler(Type, JdbcType)}不为null
     *
     * @param javaType
     * @param jdbcType
     * @return
     */
    public boolean hasTypeHandler(Class<?> javaType, JdbcType jdbcType) {
        return javaType != null && getTypeHandler((Type) javaType, jdbcType) != null;
    }

    /**
     * {@code javaTypeReference}不为null并且{@link #getTypeHandler(TypeReference, JdbcType)}不为null
     *
     * @param javaTypeReference
     * @param jdbcType
     * @return
     */
    public boolean hasTypeHandler(TypeReference<?> javaTypeReference, JdbcType jdbcType) {
        return javaTypeReference != null && getTypeHandler(javaTypeReference, jdbcType) != null;
    }

    /**
     * 将{@link TypeHandler}的{@link Class}对象作为key从{@link #ALL_TYPE_HANDLERS_MAP}中获取其对象
     *
     * @param handlerType
     * @return
     */
    public TypeHandler<?> getMappingTypeHandler(Class<? extends TypeHandler<?>> handlerType) {
        return ALL_TYPE_HANDLERS_MAP.get(handlerType);
    }

    /**
     * 直接调用{@link #getTypeHandler(Type, JdbcType)}传入null作为jdbcType获取『默认的』{@link TypeHandler}
     *
     * @param type
     * @param <T>
     * @return
     */
    public <T> TypeHandler<T> getTypeHandler(Class<T> type) {
        return getTypeHandler((Type) type, null);
    }

    /**
     * 调用{@code javaTypeReference}的{@link TypeReference#getRawType()}方法获取其泛型作为javaType，调用{@link #getTypeHandler(Type, JdbcType)}传入null作为jdbcType获取『默认的』{@link TypeHandler}
     *
     * @param javaTypeReference
     * @param <T>
     * @return
     */
    public <T> TypeHandler<T> getTypeHandler(TypeReference<T> javaTypeReference) {
        return getTypeHandler(javaTypeReference, null);
    }

    /**
     * 从{@link #JDBC_TYPE_HANDLER_MAP}获取{@code jdbcType}对应的handler
     *
     * @param jdbcType
     * @return
     */
    public TypeHandler<?> getTypeHandler(JdbcType jdbcType) {
        return JDBC_TYPE_HANDLER_MAP.get(jdbcType);
    }

    /**
     * 直接调用{@link #getTypeHandler(Type, JdbcType)}获取{@link TypeHandler}
     *
     * @param type
     * @param jdbcType
     * @param <T>
     * @return
     */
    public <T> TypeHandler<T> getTypeHandler(Class<T> type, JdbcType jdbcType) {
        return getTypeHandler((Type) type, jdbcType);
    }

    /**
     * 调用{@code javaTypeReference}的{@link TypeReference#getRawType()}方法获取其泛型作为javaType，调用{@link #getTypeHandler(Type, JdbcType)}获取{@link TypeHandler}
     *
     * @param javaTypeReference
     * @param jdbcType
     * @param <T>
     * @return
     */
    public <T> TypeHandler<T> getTypeHandler(TypeReference<T> javaTypeReference, JdbcType jdbcType) {
        return getTypeHandler(javaTypeReference.getRawType(), jdbcType);
    }

    /**
     * 如果传入的{@code type}是{@link ParamMap}，直接返回null；否则继续
     * <ol>
     *     先从{@link #TYPE_HANDLER_MAP}获取当前{@code type}获取其对应的jdbcType handler Map
     *     <li>
     *         如果获取的handler Map是null，则直接返回null，表示找不到该{@code type}该{@code jdbcType}的{@link TypeHandler}
     *     </li>
     *     <li>
     *         如果获取到的handler Map非空，则将{@code jdbcType}作为key获取其对应的{@link TypeHandler}，如果获取不到则下一步
     *     </li>
     *     <li>
     *         将null作为{@link #JDBC_TYPE_HANDLER_MAP}的key传入获取『默认的』{@link TypeHandler}，如果还获取不到则下一步
     *     </li>
     *     <li>
     *         如果jdbcType handler Map里面的handler对象类型都一样，则返回第一个handler对象，否则返回null，表示找不到该{@code type}该{@code jdbcType}的{@link TypeHandler}
     *     </li>
     * </ol>
     *
     * @param type
     * @param jdbcType
     * @param <T>
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T> TypeHandler<T> getTypeHandler(Type type, JdbcType jdbcType) {
        if (ParamMap.class.equals(type)) {
            return null;
        }
        Map<JdbcType, TypeHandler<?>> jdbcHandlerMap = getJdbcHandlerMap(type);
        TypeHandler<?> handler = null;
        if (jdbcHandlerMap != null) {
            handler = jdbcHandlerMap.get(jdbcType);
            if (handler == null) {
                handler = jdbcHandlerMap.get(null);
            }
            if (handler == null) {
                // #591
                handler = pickSoleHandler(jdbcHandlerMap);
            }
        }
        // type drives generics here
        return (TypeHandler<T>) handler;
    }

    /**
     * <ol>
     *     先从{@link #TYPE_HANDLER_MAP}获取{@code type}的jdbcType handler Map
     *     <li>
     *         如果获取到的是{@link #NULL_TYPE_HANDLER_MAP}，该单例对象就是用来说明传入的当前{@code type}之前已经寻找过handler了，并且不存在，所以以此为标识，下次再找该{@code type}的时候就不会再重复一遍操作了，直接返回null
     *     </li>
     *     <li>
     *         如果获取到的值是null，并且type是Class实例，则尝试从其父接口（{@code type}是Enum）或者父类（{@code type}是普通class）寻找handler
     *         <ul>
     *             <li>
     *                 如果找到了，就直接put找到的handlerMap到{@link #TYPE_HANDLER_MAP}给当前的{@code type}，并返回找到的handlerMap
     *             </li>
     *             <li>
     *                 如果没找到，{@code type}如果是Enum：则使用{@link #defaultEnumTypeHandler}注册给当前{@code type}；否则put{@link #NULL_TYPE_HANDLER_MAP}到{@link #TYPE_HANDLER_MAP}给当前{@code type}，防止下次再重复这样的寻找handler的步骤
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param type
     * @return
     */
    private Map<JdbcType, TypeHandler<?>> getJdbcHandlerMap(Type type) {
        Map<JdbcType, TypeHandler<?>> jdbcHandlerMap = TYPE_HANDLER_MAP.get(type);
        if (NULL_TYPE_HANDLER_MAP.equals(jdbcHandlerMap)) {
            return null;
        }
        if (jdbcHandlerMap == null && type instanceof Class) {
            Class<?> clazz = (Class<?>) type;
            // 只允许在聚合Enum的handler的时候使用interface（Enum也只能实现接口，不能继承class），所以如果当前type找不到处理器并且是Enum，只能往inteface找
            if (clazz.isEnum()) {
                jdbcHandlerMap = getJdbcHandlerMapForEnumInterfaces(clazz, clazz);
                if (jdbcHandlerMap == null) {
                    register(clazz, getInstance(clazz, defaultEnumTypeHandler));
                    return TYPE_HANDLER_MAP.get(clazz);
                }
            // 普通的class只能使用普通class来聚合handler，如果当前type找不到处理器，并且是非Enum，只能往父class找
            } else {
                jdbcHandlerMap = getJdbcHandlerMapForSuperclass(clazz);
            }
        }
        TYPE_HANDLER_MAP.put(type, jdbcHandlerMap == null ? NULL_TYPE_HANDLER_MAP : jdbcHandlerMap);
        return jdbcHandlerMap;
    }

    /**
     * 获取Enum类的所有接口（Enum不能继承类，包括抽象类，只能实现接口），按照获取的接口顺序遍历所有接口，然后寻找每个接口是否在{@link #TYPE_HANDLER_MAP}中拥有类型处理器Map，
     * 有就使用{@code enumClazz}构建Enum处理器，并返回，结束本方法；如果当前遍历的接口如果没有handler，则将当前接口作为{@code clazz}，{@code enumClazz}保持不变调用本方法达到循环递归的效果，
     * 直到遍历递归完所有的接口都找不到handler为止。
     *
     * 单元测试：{@link TypeHandlerRegistryTest#demoTypeHandlerForSuperInterface()}
     * @param clazz 起始点enum或者遍历的当前interface
     * @param enumClazz Enum类，方便循环递归到有处理器的父接口的时候使用{@link #getInstance(Class, Class)}直接构建该Enum的handler（从逻辑上讲，Enum的处理器是肯定需要确定Enum的实际类型的）
     * @return
     */
    private Map<JdbcType, TypeHandler<?>> getJdbcHandlerMapForEnumInterfaces(Class<?> clazz, Class<?> enumClazz) {
        // 遍历枚举类的所有接口
        for (Class<?> iface : clazz.getInterfaces()) {
            // 获得该接口对应的 jdbcHandlerMap 集合
            Map<JdbcType, TypeHandler<?>> jdbcHandlerMap = TYPE_HANDLER_MAP.get(iface);
            // 为空，递归 getJdbcHandlerMapForEnumInterfaces 方法，继续从父类对应的 TypeHandler 集合
            if (jdbcHandlerMap == null) {
                jdbcHandlerMap = getJdbcHandlerMapForEnumInterfaces(iface, enumClazz);
                }
                // 这里好像有点问题，如果要递归两层以上接口找到TypeHandler，那么最里面的一层已经经过了下面创建Handler实例的步骤，返回出来之后又做了一次一模一样的动作
                if (jdbcHandlerMap != null) {
                    // Found a type handler regsiterd to a super interface
                    HashMap<JdbcType, TypeHandler<?>> newMap = new HashMap<>();
                    for (Entry<JdbcType, TypeHandler<?>> entry : jdbcHandlerMap.entrySet()) {
                        // Create a type handler instance with enum type as a constructor arg
                        newMap.put(entry.getKey(), getInstance(enumClazz, entry.getValue().getClass()));
                }
                return newMap;
            }
        }
        // 找不到，则返回 null
        return null;
    }

    /**
     * 从传入的{@code clazz}开始一直往上找其父class作为{@link #TYPE_HANDLER_MAP}的key寻找其相应的jdbcType类型处理器Map：只要一找到就返回；找到顶端(Object.class)都找不到则返回null
     *
     * @param clazz
     * @return
     */
    private Map<JdbcType, TypeHandler<?>> getJdbcHandlerMapForSuperclass(Class<?> clazz) {
        // 获得父类
        Class<?> superclass = clazz.getSuperclass();
        // 不存在非 Object 的父类，返回 null
        if (superclass == null || Object.class.equals(superclass)) {
            return null;
        }
        // 获得父类对应的 TypeHandler 集合
        Map<JdbcType, TypeHandler<?>> jdbcHandlerMap = TYPE_HANDLER_MAP.get(superclass);
        // 找到，则直接返回
        if (jdbcHandlerMap != null) {
            return jdbcHandlerMap;
        // 找不到，则递归 getJdbcHandlerMapForSuperclass 方法，继续获得父类对应的 TypeHandler 集合
        } else {
            return getJdbcHandlerMapForSuperclass(superclass);
        }
    }

    /**
     * 如果{@code jdbcHandlerMap}中有值并且所有{@link TypeHandler}对象（value）都是同一Class的，则返回第一个handler对象；否则返回null
     *
     * @param jdbcHandlerMap
     * @return
     */
    private TypeHandler<?> pickSoleHandler(Map<JdbcType, TypeHandler<?>> jdbcHandlerMap) {
        TypeHandler<?> soleHandler = null;
        for (TypeHandler<?> handler : jdbcHandlerMap.values()) {
            if (soleHandler == null) {
                soleHandler = handler;
            // 如果还有，并且不同类，那么不好选择，所以返回 null
            } else if (!handler.getClass().equals(soleHandler.getClass())) {
                // More than one type handlers registered.
                return null;
            }
        }
        return soleHandler;
    }

    public TypeHandler<Object> getUnknownTypeHandler() {
        return UNKNOWN_TYPE_HANDLER;
    }

    /**
     * 为{@code jdbcType}注册处理器{@code handler}，容器为:{@link #JDBC_TYPE_HANDLER_MAP}
     *
     * @param jdbcType
     * @param handler
     */
    public void register(JdbcType jdbcType, TypeHandler<?> handler) {
        JDBC_TYPE_HANDLER_MAP.put(jdbcType, handler);
    }

    //
    // REGISTER INSTANCE
    //

    // Only handler

    /**
     *
     *  <ol>
     *      <li>
     *          尝试获取{@code typeHandler}上的{@link MappedTypes}注解，如果<b><i><u>获取到了该注解</u></i></b>并且<b><i><u>注解中定义了一个及以上的javaType</u></i></b>，则循环调用{@link #register(Type, TypeHandler)}为每一个定义的javaType注册该类型处理器，结束本方法；否则下一步
     *      </li>
     *      <li>
     *          如果{@code typeHandler}是{@link TypeReference}的子类，则通过其{@link TypeReference#getRawType()}获得javaType，并调用{@link #register(Type, TypeHandler)}为该javaType注册该处理器，并结束方法；否则下一步
     *      </li>
     *      <li>
     *          如果上面的步骤都没有成功，则将null作为javaType的值调用{@link #register(Type, TypeHandler)}进行注册，即{@link #TYPE_HANDLER_MAP}的key1将是null
     *      </li>
     *  </ol>
     *
     *
     * @param typeHandler
     * @param <T>
     */
    @SuppressWarnings("unchecked")
    public <T> void register(TypeHandler<T> typeHandler) {
        boolean mappedTypeFound = false;
        MappedTypes mappedTypes = typeHandler.getClass().getAnnotation(MappedTypes.class);
        if (mappedTypes != null) {
            for (Class<?> handledType : mappedTypes.value()) {
                register(handledType, typeHandler);
                mappedTypeFound = true;
            }
        }
        // @since 3.1.0 - try to auto-discover the mapped type
        if (!mappedTypeFound && typeHandler instanceof TypeReference) {
            try {
                TypeReference<T> typeReference = (TypeReference<T>) typeHandler;
                register(typeReference.getRawType(), typeHandler); // Java Type 为 <T> 泛型
                mappedTypeFound = true;
            } catch (Throwable t) {
                // maybe users define the TypeReference with a different type and are not assignable, so just ignore it
            }
        }
        if (!mappedTypeFound) {
            register((Class<T>) null, typeHandler);
        }
    }

    // java type + handler


    /**
     * 直接调用{@link #register(Type, TypeHandler)}
     * @param javaType
     * @param typeHandler
     * @param <T>
     */
    public <T> void register(Class<T> javaType, TypeHandler<? extends T> typeHandler) {
        register((Type) javaType, typeHandler);
    }


    /**
     * 给{@code javaType} 注册类型处理器，尝试获取{@code typeHandler}上可能存在的{@link MappedJdbcTypes}注解
     * <ol>
     *     <li>
     *         如果获取到，则获取注解中定义了的{@link JdbcType}数组，循环调用{@link #register(Type, JdbcType, TypeHandler)}进行注册。然后再判断该注解是否声明了{@link MappedJdbcTypes#includeNullJdbcType()}为True，是就将jdbcType设置为null再进行一次注册（表示该javaType的默认处理器）
     *     </li>
     *     <li>
     *         如果获取不到注解，则将{@code jdbcType}设置为null调用{@link #register(Type, JdbcType, TypeHandler)}进行注册
     *     </li>
     * </ol>
     * @param javaType
     * @param typeHandler
     * @param <T>
     */
    private <T> void register(Type javaType, TypeHandler<? extends T> typeHandler) {
        MappedJdbcTypes mappedJdbcTypes = typeHandler.getClass().getAnnotation(MappedJdbcTypes.class);
        if (mappedJdbcTypes != null) {
            for (JdbcType handledJdbcType : mappedJdbcTypes.value()) {
                register(javaType, handledJdbcType, typeHandler);
            }
            if (mappedJdbcTypes.includeNullJdbcType()) {
                register(javaType, null, typeHandler);
            }
        } else {
            register(javaType, null, typeHandler);
        }
    }

    /**
     * 通过{@link TypeReference#getRawType()}获取{@code javaTypeReference}（通常是处理器对象本身，{@link BaseTypeHandler}的子类）的泛型作为要处理的javaType，通过调用{@link #register(Type, TypeHandler)}为其注册处理器{@code handler}
     *
     * @param javaTypeReference
     * @param handler
     * @param <T>
     */
    public <T> void register(TypeReference<T> javaTypeReference, TypeHandler<? extends T> handler) {
        register(javaTypeReference.getRawType(), handler);
    }

    // java type + jdbc type + handler

    /**
     * 直接调用{@link #register(Type, JdbcType, TypeHandler)}
     *
     * @param type
     * @param jdbcType
     * @param handler
     * @param <T>
     */
    public <T> void register(Class<T> type, JdbcType jdbcType, TypeHandler<? extends T> handler) {
        register((Type) type, jdbcType, handler);
    }

    /**
     * 给{@code javaType}注册一个{@code jdbcType}的类型处理器{@code handler}
     * <ol>
     *     <li>
     *         传进来的{@code javaType}如果是null，则将{@code handler}注册到{@link #ALL_TYPE_HANDLERS_MAP}，key为{@code handler.getClass()}
     *     </li>
     *     <li>
     *         如果{@code javaType}不是null，从{@link #TYPE_HANDLER_MAP}获取其所有的处理器Map（如果没有就新建一个Map），然后将{@code jdbcType}和{@code handler}直接put进去，没有对这两个值判空
     *         <p>
     *             设置jdbcType是null，相当于该{@code javaType}的一个默认处理器，参考{@link #getTypeHandler(Type, JdbcType)}方法
     *         </p>
     *     </li>
     * </ol>
     *
     * @param javaType Java Type
     * @param jdbcType JDBC Type
     * @param handler TypeHandler 对象
     */
    private void register(Type javaType, JdbcType jdbcType, TypeHandler<?> handler) {
        if (javaType != null) {
            Map<JdbcType, TypeHandler<?>> map = TYPE_HANDLER_MAP.get(javaType);
            if (map == null || map == NULL_TYPE_HANDLER_MAP) {
                map = new HashMap<>();
                TYPE_HANDLER_MAP.put(javaType, map);
            }
            map.put(jdbcType, handler);
        }
        ALL_TYPE_HANDLERS_MAP.put(handler.getClass(), handler);
    }

    //
    // REGISTER CLASS
    //

    // Only handler type

    /**
     * <ol>
     *     <li>
     *         尝试获取{@code typeHandlerClass}上的{@link MappedTypes}注解，如果<b><i><u>获取到了该注解</u></i></b>并且<b><i><u>注解中定义了一个及以上的javaType</u></i></b>，则循环调用{@link #register(Type, TypeHandler)}为每一个定义的javaType注册该类型处理器，结束本方法；否则下一步
     *     </li>
     *     <li>
     *         获取不到注解就调用{@link #getInstance(Class, Class)}通过无参构造实例化{@code typeHandlerClass}然后调用{@link #register(TypeHandler)}进行注册
     *     </li>
     * </ol>
     *
     *
     * @param typeHandlerClass 处理器的Class对象
     */
    public void register(Class<?> typeHandlerClass) {
        boolean mappedTypeFound = false;
        MappedTypes mappedTypes = typeHandlerClass.getAnnotation(MappedTypes.class);
        if (mappedTypes != null) {
            for (Class<?> javaTypeClass : mappedTypes.value()) {
                register(javaTypeClass, typeHandlerClass);
                mappedTypeFound = true;
            }
        }
        if (!mappedTypeFound) {
            register(getInstance(null, typeHandlerClass));
        }
    }

    // java type + handler type

    /**
     * 调用{@link Resources#classForName(String)}分别获得{@code javaTypeClassName}和{@code typeHandlerClassName}的Class对象，然后调用{@link #register(Class, Class)}
     *
     * @param javaTypeClassName
     * @param typeHandlerClassName
     * @throws ClassNotFoundException
     */
    public void register(String javaTypeClassName, String typeHandlerClassName) throws ClassNotFoundException {
        register(Resources.classForName(javaTypeClassName),
                Resources.classForName(typeHandlerClassName));
    }

    /**
     * 调用{@link #getInstance(Class, Class)}实例化{@code typeHandlerClass}之后调用{@link #register(Class, TypeHandler)}进行注册
     *
     * @param javaTypeClass
     * @param typeHandlerClass
     */
    public void register(Class<?> javaTypeClass, Class<?> typeHandlerClass) {
        register(javaTypeClass, getInstance(javaTypeClass, typeHandlerClass));
    }

    // java type + jdbc type + handler type

    /**
     * 调用{@link #getInstance(Class, Class)}实例化{@code typeHandlerClass}之后调用{@link #register(Class, JdbcType, TypeHandler)}进行注册
     *
     * @param javaTypeClass
     * @param jdbcType
     * @param typeHandlerClass
     */
    public void register(Class<?> javaTypeClass, JdbcType jdbcType, Class<?> typeHandlerClass) {
        register(javaTypeClass, jdbcType, getInstance(javaTypeClass, typeHandlerClass));
    }

    // Construct a handler (used also from Builders)

    /**
     * 先尝试获取{@code typeHandlerClass}的携带{@link Class}类型参数的构造函数({@link EnumTypeHandler})，没有就获取无参构造，然后构建一个{@code typeHandlerClass}实例并返回
     *
     * @param javaTypeClass Java Type 类
     * @param typeHandlerClass TypeHandler 类
     * @param <T> 泛型
     * @return TypeHandler 对象
     */
    @SuppressWarnings("unchecked")
    public <T> TypeHandler<T> getInstance(Class<?> javaTypeClass, Class<?> typeHandlerClass) {
        // 获得 Class 类型的构造方法
        if (javaTypeClass != null) {
            try {
                Constructor<?> c = typeHandlerClass.getConstructor(Class.class);
                return (TypeHandler<T>) c.newInstance(javaTypeClass); // 符合这个条件的，例如 EnumTypeHandler
            } catch (NoSuchMethodException ignored) {
                // ignored 忽略该异常，继续向下
            } catch (Exception e) {
                throw new TypeException("Failed invoking constructor for handler " + typeHandlerClass, e);
            }
        }
        // 获得空参的构造方法
        try {
            Constructor<?> c = typeHandlerClass.getConstructor();
            return (TypeHandler<T>) c.newInstance(); // 符合这个条件的，例如 IntegerTypeHandler
        } catch (Exception e) {
            throw new TypeException("Unable to find a usable constructor for " + typeHandlerClass, e);
        }
    }

    // scan

    /**
     * 调用{@link ResolverUtil#find(ResolverUtil.Test, String)}和{@link ResolverUtil#getClasses()}获得指定{@code packageName}下所有的{@link TypeHandler}的子类并调用{@link #register(Class)}进行注册
     *
     * @param packageName 指定包
     */
    public void register(String packageName) {
        // 扫描指定包下的所有 TypeHandler 类
        ResolverUtil<Class<?>> resolverUtil = new ResolverUtil<>();
        resolverUtil.find(new ResolverUtil.IsA(TypeHandler.class), packageName);
        Set<Class<? extends Class<?>>> handlerSet = resolverUtil.getClasses();
        // 遍历 TypeHandler 数组，发起注册
        for (Class<?> type : handlerSet) {
            //Ignore inner classes and interfaces (including package-info.java) and abstract classes
            // 排除匿名类、接口、抽象类
            if (!type.isAnonymousClass() && !type.isInterface() && !Modifier.isAbstract(type.getModifiers())) {
                register(type);
            }
        }
    }

    // get information

    /**
     * {@link #ALL_TYPE_HANDLERS_MAP}.values()
     *
     * @since 3.2.2
     * @return
     */
    public Collection<TypeHandler<?>> getTypeHandlers() {
        return Collections.unmodifiableCollection(ALL_TYPE_HANDLERS_MAP.values());
    }

}
