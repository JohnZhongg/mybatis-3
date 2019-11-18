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
package org.apache.ibatis.builder;

import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.decorators.LruCache;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.util.*;

/**
 * Mapper 构造器的小助手，提供了一些公用的方法，例如创建 ParameterMap、MappedStatement 对象等等。
 *
 * @author Clinton Begin
 */
public class MapperBuilderAssistant extends BaseBuilder {

    /**
     * 当前 Mapper 命名空间 （一般是mapper全限定名）
     */
    private String currentNamespace;
    /**
     * 资源引用的地址 （一般为java文件路径名：xxx.java (best guess)）
     */
    private final String resource;
    /**
     * 当前 Cache 对象
     */
    private Cache currentCache;
    /**
     * 当前mapper在xml在设置了{@code <cache-ref namespace="namespace"/>}标签并且声明了namespace属性，但是并没有根据该namespace解析mapper对应的{@link Cache}对象
     */
    private boolean unresolvedCacheRef; // issue #676

    public MapperBuilderAssistant(Configuration configuration, String resource) {
        super(configuration);
        ErrorContext.instance().resource(resource);
        this.resource = resource;
    }

    public String getCurrentNamespace() {
        return currentNamespace;
    }

    /**
     * 设置当前的namespace，如果传入{@code currentNamespace}为null或者{@link MapperBuilderAssistant#currentNamespace}不为null且和传入{@code currentNamespace}不同都会抛出异常{@link BuilderException}
     *
     * @param currentNamespace
     */
    public void setCurrentNamespace(String currentNamespace) {
        if (currentNamespace == null) {
            throw new BuilderException("The mapper element requires a namespace attribute to be specified.");
        }

        if (this.currentNamespace != null && !this.currentNamespace.equals(currentNamespace)) {
            throw new BuilderException("Wrong namespace. Expected '"
                    + this.currentNamespace + "' but found '" + currentNamespace + "'.");
        }

        this.currentNamespace = currentNamespace;
    }

    /**
     * {@code base}是否为null
     * <ul>
     *     <li>
     *         是则返回null
     *     </li>
     *     <li>
     *         否则判断{@code isReference}是否为true
     *         <ul>
     *             <li>
     *                 为true则看{@code base}是否包含了"."：是则直接返回{@code base}；否则拼接{@link MapperBuilderAssistant#currentNamespace}+{@code base}返回（从是否包含了"."来判断是否是绝对id：如果没有"."就认为是相对id，添加{@link #currentNamespace}为前缀返回；如果有"."则判断是绝对id，直接返回）
     *             </li>
     *             <li>
     *                 为false则（能返回的肯定是当前{@link #currentNamespace}为前缀的id）:
     *                 <ol>
     *                     <li>
     *                         {@code base}是否以{@link MapperBuilderAssistant#currentNamespace}+"."开头：是则直接返回{@code base}，否则继续
     *                     </li>
     *                     <li>
     *                         {@code base}是否包含"."：是则抛出异常{@link BuilderException}；否则拼接{@link MapperBuilderAssistant#currentNamespace}+{@code base}返回
     *                     </li>
     *                 </ol>
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * @param base
     * @param isReference
     * @return
     */
    public String applyCurrentNamespace(String base, boolean isReference) {
        if (base == null) {
            return null;
        }
        if (isReference) {
            // is it qualified with any namespace yet?
            if (base.contains(".")) {
                return base;
            }
        } else {
            // is it qualified with this namespace yet?
            if (base.startsWith(currentNamespace + ".")) {
                return base;
            }
            if (base.contains(".")) {
                throw new BuilderException("Dots are not allowed in element names, please remove it from " + base);
            }
        }
        return currentNamespace + "." + base;
    }

    /**
     * 根据传入的命名空间寻找其指向的{@link Cache}对象：
     * <ol>
     *     <li>
     *         如果传入的{@code namespace}是null，直接抛出异常{@link BuilderException}，否则继续
     *     </li>
     *     <li>
     *         设置{@link #unresolvedCacheRef}为true，并尝试调用{@link Configuration#getCache(String)}获取{@link Cache}对象
     *         <ul>
     *             <li>
     *                 如果获取的{@link Cache}对象是null或者抛出了异常，则抛出异常{@link IncompleteElementException}，<b>注意此时{@link #unresolvedCacheRef}仍为true</b>
     *             </li>
     *             <li>
     *                 如果获取到了{@link Cache}对象，则设置{@link #currentCache}为获取到的{@link Cache}对象，并设置{@link #unresolvedCacheRef}为false，并返回该{@link Cache}对象
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param namespace 指向的命名空间
     * @return Cache 对象
     */
    public Cache useCacheRef(String namespace) {
        if (namespace == null) {
            throw new BuilderException("cache-ref element requires a namespace attribute.");
        }
        try {
            unresolvedCacheRef = true;
            Cache cache = configuration.getCache(namespace);
            if (cache == null) {
                throw new IncompleteElementException("No cache for namespace '" + namespace + "' could be found.");
            }
            currentCache = cache;
            unresolvedCacheRef = false;
            return cache;
        } catch (IllegalArgumentException e) {
            throw new IncompleteElementException("No cache for namespace '" + namespace + "' could be found.", e);
        }
    }

    /**
     * 将{@link #currentNamespace}传入{@link CacheBuilder#CacheBuilder(String)}构建一个{@link CacheBuilder}对象，并调用其各方法将本方法入参对其成员变量implementation、decorators、size、clearInterval、readWrite、properties、blocking
     * 进行设置，然后调用其{@link CacheBuilder#build()}构建一个{@link Cache}对象，然后调用{@link #configuration}的{@link Configuration#addCache(Cache)}添加到{@link Configuration#caches}中，并将其设置到成员变量{@link #currentCache}，
     * 然后返回该{@link Cache}对象
     *
     * @param typeClass 负责存储的 Cache 实现类
     * @param evictionClass 负责过期的 Cache 实现类
     * @param flushInterval 清空缓存的频率。0 代表不清空
     * @param size 缓存容器大小
     * @param readWrite 是否序列化
     * @param blocking 是否阻塞
     * @param props Properties 对象
     * @return Cache 对象
     */
    public Cache useNewCache(Class<? extends Cache> typeClass,
                             Class<? extends Cache> evictionClass,
                             Long flushInterval,
                             Integer size,
                             boolean readWrite,
                             boolean blocking,
                             Properties props) {
        Cache cache = new CacheBuilder(currentNamespace)
                .implementation(valueOrDefault(typeClass, PerpetualCache.class))
                .addDecorator(valueOrDefault(evictionClass, LruCache.class))
                .clearInterval(flushInterval)
                .size(size)
                .readWrite(readWrite)
                .blocking(blocking)
                .properties(props)
                .build();
        configuration.addCache(cache);
        currentCache = cache;
        return cache;
    }


    /**
     * 调用{@link ParameterMap.Builder#Builder(Configuration, String, Class, List)}传入三个入参获得{@link ParameterMap.Builder}对象并调用{@link ParameterMap.Builder#build()}方法构建一个{@link ParameterMap}对象并返回，
     * 然后调用成员变量{@link #configuration}的{@link Configuration#addParameterMap(ParameterMap)}方法添加该{@link ParameterMap}对象，然后return该{@link ParameterMap}对象
     *
     * @param id 当前parameterMap的id
     * @param parameterClass parameterMapping的承载类
     * @param parameterMappings parameterMapping集合
     * @return
     */
    public ParameterMap addParameterMap(String id, Class<?> parameterClass, List<ParameterMapping> parameterMappings) {
        id = applyCurrentNamespace(id, false);
        ParameterMap parameterMap = new ParameterMap.Builder(configuration, id, parameterClass, parameterMappings).build();
        configuration.addParameterMap(parameterMap);
        return parameterMap;
    }


    /**
     * <ol>
     *     <li>
     *         调用{@link #resolveParameterJavaType(Class, String, Class, JdbcType)}传入{@code parameterType}、{@code property}、{@code javaType}、{@code jdbcType}解析当前parameterMapping对应的javaType
     *     </li>
     *     <li>
     *         调用{@link BaseBuilder#resolveTypeHandler(Class, Class)}解析当前parameterMapping对应的{@link TypeHandler}对象
     *     </li>
     *     <li>
     *         调用{@link ParameterMapping.Builder#Builder(Configuration, String, Class)}构建一个{@link ParameterMapping.Builder}对象然后调用其api将{@code jdbcType}、{@code resultMap}、{@code parameterModee}、
     *         {@code numericScale}和解析之后的{@link TypeHandler}对象进行设置然后调用其{@link ParameterMapping.Builder#build()}方法进行{@link ParameterMapping}对象的构建并返回
     *     </li>
     * </ol>
     *
     * @param parameterType parameterMapping的承载类
     * @param property 在parameterMapping承载类中当前parameterMapping对应的property
     * @param javaType 指定当前parameterMapping的javaType
     * @param jdbcType 指定当前parameterMapping的jdbcType
     * @param resultMap {@code javaType}是{@link java.sql.ResultSet}.class或者{@code jdbcType}是{@link JdbcType#CURSOR}的时候当前字段不能为空，它指向一个{@link ResultMap}对象
     * @param parameterMode 指定当前parameterMapping的{@link ParameterMode}类型
     * @param typeHandler 当前parameterMapping的{@link TypeHandler}类对象
     * @param numericScale 当前parameter的精度
     * @return
     */
    public ParameterMapping buildParameterMapping(
            Class<?> parameterType,
            String property,
            Class<?> javaType,
            JdbcType jdbcType,
            String resultMap,
            ParameterMode parameterMode,
            Class<? extends TypeHandler<?>> typeHandler,
            Integer numericScale) {
        resultMap = applyCurrentNamespace(resultMap, true);

        // Class parameterType = parameterMapBuilder.type();
        Class<?> javaTypeClass = resolveParameterJavaType(parameterType, property, javaType, jdbcType);
        TypeHandler<?> typeHandlerInstance = resolveTypeHandler(javaTypeClass, typeHandler);

        return new ParameterMapping.Builder(configuration, property, javaTypeClass)
                .jdbcType(jdbcType)
                .resultMapId(resultMap)
                .mode(parameterMode)
                .numericScale(numericScale)
                .typeHandler(typeHandlerInstance)
                .build();
    }


    /**
     * 创建{@link ResultMap}对象，并添加到{@link Configuration}中：
     * <ol>
     *     <li>
     *         调用{@link #applyCurrentNamespace(String, boolean)}传入{@code id}和false，将{@code id}作为绝对id判断其是否在当前{@link #currentNamespace}下面，是则继续；否则抛出异常
     *     </li>
     *     <li>
     *         调用{@link #applyCurrentNamespace(String, boolean)}传入{@code extend}和true，获取的继承{@link ResultMap}对象的绝对{@link ResultMap#id}
     *     </li>
     *     <li>
     *         如果上述步骤的方法返回的父{@link ResultMap}的绝对id不为null：
     *         <ol>
     *             <li>
     *                 调用成员变量{@link #configuration}的{@link Configuration#hasResultMap(String)}方法传入父{@link ResultMap}的绝对id判断是否存在：不存在则抛出异常{@link IncompleteElementException}；存在则继续
     *             </li>
     *             <li>
     *                 调用成员变量{@link #configuration}的{@link Configuration#getResultMap(String)}方法传入父{@link ResultMap}的绝对id获取对应的{@link ResultMap}对象
     *             </li>
     *             <li>
     *                 获取父{@link ResultMap}的{@link ResultMap#resultMappings}{@link List}，然后调用{@link List#removeAll(Collection)}传入{@code resultMappings}，实现当前{@link ResultMapping}覆盖父{@link ResultMap}也拥有的{@link ResultMapping}
     *             </li>
     *             <li>
     *                 遍历当前{@code resultMappings}调用{@link ResultMapping#getFlags()}获取{@link ResultFlag}{@link List}并判断是否存在{@link ResultMapping}拥有{@link ResultFlag#CONSTRUCTOR}（即声明了{@code <constructor/>}）：
     *                 <ul>
     *                     <li>
     *                         如果存在，则遍历父{@link ResultMapping}集合去重所有带有{@link ResultFlag#CONSTRUCTOR}的{@link ResultMapping}对象（如果当前{@link ResultMap}中声明了{@code <constructor/>}则不继承父{@link ResultMap}中声明的）
     *                     </li>
     *                     <li>
     *                         如果不存在，则什么都不做（如果当前{@link ResultMap}中没有声明{@code <constructor/>}则继承父{@link ResultMap}中声明的）
     *                     </li>
     *                 </ul>
     *             </li>
     *             <li>
     *                 经过以上处理之后的父{@link ResultMap}的{@link ResultMapping}对象通过{@link List#addAll(Collection)}全部加入到当前{@code resultMappings}中，调用{@link ResultMap.Builder#Builder(Configuration, String, Class, List, Boolean)}
     *                 分别传入{@link #configuration}、{@code id}、{@code type}、经过以上继承处理的{@code resultMappings}、{@code autoMapping}实例化一个{@link ResultMap.Builder}对象，然后调用{@link ResultMap.Builder#discriminator}传入{@code discriminator}
     *                 设置{@link ResultMap#discriminator}，最后调用{@link ResultMap.Builder#build()}构建一个{@link ResultMap}对象并返回
     *             </li>
     *             <li>
     *                 调用成员变量{@link #configuration}的{@link Configuration#addResultMap(ResultMap)}方法记录构建好的{@link ResultMap}对象
     *             </li>
     *             <li>
     *                 返回上面构建的{@link ResultMap}对象
     *             </li>
     *         </ol>
     *     </li>
     * </ol>
     *
     * @param id 当前要构建的{@link ResultMap}对象的唯一标志（绝对id，携带namespace）
     * @param type 当前{@link ResultMap}对应的{@link Class}对象，也就是{@link ResultMapping}的承接类
     * @param extend 当前{@link ResultMap}对象继承的{@link ResultMap}对象id（可以是相对id，不携带namespace）
     * @param discriminator 当前{@link ResultMap}的{@link Discriminator}对象（如果在xml中声明了多个则最后一个会进去）
     * @param resultMappings 当前{@link ResultMap}中声明的{@link ResultMapping}对象
     * @param autoMapping 当前{@link ResultMap}是否开启了自动匹配
     * @return
     */
    public ResultMap addResultMap(
            String id,
            Class<?> type,
            String extend,
            Discriminator discriminator,
            List<ResultMapping> resultMappings,
            Boolean autoMapping) {
        id = applyCurrentNamespace(id, false);
        extend = applyCurrentNamespace(extend, true);

        if (extend != null) {
            if (!configuration.hasResultMap(extend)) {
                throw new IncompleteElementException("Could not find a parent resultmap with id '" + extend + "'");
            }
            ResultMap resultMap = configuration.getResultMap(extend);
            List<ResultMapping> extendedResultMappings = new ArrayList<>(resultMap.getResultMappings());
            extendedResultMappings.removeAll(resultMappings);
            boolean declaresConstructor = false;
            for (ResultMapping resultMapping : resultMappings) {
                if (resultMapping.getFlags().contains(ResultFlag.CONSTRUCTOR)) {
                    declaresConstructor = true;
                    break;
                }
            }
            if (declaresConstructor) {
                extendedResultMappings.removeIf(resultMapping -> resultMapping.getFlags().contains(ResultFlag.CONSTRUCTOR));
            }
            resultMappings.addAll(extendedResultMappings);
        }
        ResultMap resultMap = new ResultMap.Builder(configuration, id, type, resultMappings, autoMapping)
                .discriminator(discriminator)
                .build();
        configuration.addResultMap(resultMap);
        return resultMap;
    }

    /**
     * 构建{@link Discriminator}对象：
     * <ol>
     *     <li>
     *         调用{@link #buildResultMapping(Class, String, String, Class, JdbcType, String, String, String, String, Class, List, String, String, boolean)}传入{@code resultType}、{@code column}、{@code javaType}、{@code jdbcType}、{@code typeHandler}、一个空的{@link ResultFlag}的{@link ArrayList}、lazy为false构建一个{@link ResultMapping}对象
     *     </li>
     *     <li>
     *         将{@code discriminatorMap}的所有value取出调用{@link #applyCurrentNamespace(String, boolean)}方法传入true构建完整的resultMap唯一标志，并结合原有的key组成一个新的{@link HashMap}对象
     *     </li>
     *     <li>
     *         调用{@link Discriminator.Builder#Builder(Configuration, ResultMapping, Map)}传入成员变量{@link #configuration}、上面步骤新构建的{@link ResultMapping}对象、{@link HashMap}对象构造一个{@link Discriminator.Builder}对象然后调用{@link Discriminator.Builder#build()}方法构建一个{@link Discriminator}对象并返回
     *     </li>
     * </ol>
     *
     * @param resultType 当前{@code <discriminator/>}标签的外层标签的映射关系承载类，由"type"、"ofType"、"resultType"、"javaType"属性来指定
     * @param column 当前{@code <discriminator/>}标签的"column"属性
     * @param javaType {@code <discriminator/>}标签的"javaType"属性
     * @param jdbcType {@code <discriminator/>}标签的"jdbcType"属性
     * @param typeHandler {@code <discriminator/>}标签的"typeHandler"属性
     * @param discriminatorMap {@code <discriminator/>}标签的"value"和"ResultMap"的映射
     * @return
     */
    public Discriminator buildDiscriminator(
            Class<?> resultType,
            String column,
            Class<?> javaType,
            JdbcType jdbcType,
            Class<? extends TypeHandler<?>> typeHandler,
            Map<String, String> discriminatorMap) {
        ResultMapping resultMapping = buildResultMapping(
                resultType,
                null,
                column,
                javaType,
                jdbcType,
                null,
                null,
                null,
                null,
                typeHandler,
                new ArrayList<ResultFlag>(),
                null,
                null,
                false);
        Map<String, String> namespaceDiscriminatorMap = new HashMap<>();
        for (Map.Entry<String, String> e : discriminatorMap.entrySet()) {
            String resultMap = e.getValue();
            resultMap = applyCurrentNamespace(resultMap, true);
            namespaceDiscriminatorMap.put(e.getKey(), resultMap);
        }
        return new Discriminator.Builder(configuration, resultMapping, namespaceDiscriminatorMap).build();
    }

    // 构建 MappedStatement 对象
    public MappedStatement addMappedStatement(
            String id,
            SqlSource sqlSource,
            StatementType statementType,
            SqlCommandType sqlCommandType,
            Integer fetchSize,
            Integer timeout,
            String parameterMap,
            Class<?> parameterType,
            String resultMap,
            Class<?> resultType,
            ResultSetType resultSetType,
            boolean flushCache,
            boolean useCache,
            boolean resultOrdered,
            KeyGenerator keyGenerator,
            String keyProperty,
            String keyColumn,
            String databaseId,
            LanguageDriver lang,
            String resultSets) {

        // 如果只想的 Cache 未解析，抛出 IncompleteElementException 异常
        if (unresolvedCacheRef) {
            throw new IncompleteElementException("Cache-ref not yet resolved");
        }

        // 获得 id 编号，格式为 `${namespace}.${id}`
        id = applyCurrentNamespace(id, false);
        boolean isSelect = sqlCommandType == SqlCommandType.SELECT;

        // 创建 MappedStatement.Builder 对象
        MappedStatement.Builder statementBuilder = new MappedStatement.Builder(configuration, id, sqlSource, sqlCommandType)
                .resource(resource)
                .fetchSize(fetchSize)
                .timeout(timeout)
                .statementType(statementType)
                .keyGenerator(keyGenerator)
                .keyProperty(keyProperty)
                .keyColumn(keyColumn)
                .databaseId(databaseId)
                .lang(lang)
                .resultOrdered(resultOrdered)
                .resultSets(resultSets)
                .resultMaps(getStatementResultMaps(resultMap, resultType, id)) // 获得 ResultMap 集合
                .resultSetType(resultSetType)
                .flushCacheRequired(valueOrDefault(flushCache, !isSelect))
                .useCache(valueOrDefault(useCache, isSelect))
                .cache(currentCache);

        // 获得 ParameterMap ，并设置到 MappedStatement.Builder 中
        ParameterMap statementParameterMap = getStatementParameterMap(parameterMap, parameterType, id);
        if (statementParameterMap != null) {
            statementBuilder.parameterMap(statementParameterMap);
        }

        // 创建 MappedStatement 对象
        MappedStatement statement = statementBuilder.build();
        // 添加到 configuration 中
        configuration.addMappedStatement(statement);
        return statement;
    }

    /**
     * 如果{@code value}是null，则返回{@code defaultValue}，否则返回{@code value}
     *
     * @param value
     * @param defaultValue
     * @param <T>
     * @return
     */
    private <T> T valueOrDefault(T value, T defaultValue) {
        return value == null ? defaultValue : value;
    }

    // 获得 ParameterMap
    private ParameterMap getStatementParameterMap(
            String parameterMapName,
            Class<?> parameterTypeClass,
            String statementId) {
        // 获得 ParameterMap 的编号，格式为 `${namespace}.${parameterMapName}`
        parameterMapName = applyCurrentNamespace(parameterMapName, true);
        ParameterMap parameterMap = null;
        // 如果 parameterMapName 非空，则获得 parameterMapName 对应的 ParameterMap 对象
        if (parameterMapName != null) {
            try {
                parameterMap = configuration.getParameterMap(parameterMapName);
            } catch (IllegalArgumentException e) {
                throw new IncompleteElementException("Could not find parameter map " + parameterMapName, e);
            }
            // 如果 parameterTypeClass 非空，则创建 ParameterMap 对象
        } else if (parameterTypeClass != null) {
            List<ParameterMapping> parameterMappings = new ArrayList<>();
            parameterMap = new ParameterMap.Builder(
                    configuration,
                    statementId + "-Inline",
                    parameterTypeClass,
                    parameterMappings).build();
        }
        return parameterMap;
    }

    // 获得 ResultMap 集合
    private List<ResultMap> getStatementResultMaps(
            String resultMap,
            Class<?> resultType,
            String statementId) {
        // 获得 resultMap 的编号
        resultMap = applyCurrentNamespace(resultMap, true);

        // 创建 ResultMap 集合
        List<ResultMap> resultMaps = new ArrayList<>();
        // 如果 resultMap 非空，则获得 resultMap 对应的 ResultMap 对象(们）
        if (resultMap != null) {
            String[] resultMapNames = resultMap.split(",");
            for (String resultMapName : resultMapNames) {
                try {
                    resultMaps.add(configuration.getResultMap(resultMapName.trim())); // 从 configuration 中获得
                } catch (IllegalArgumentException e) {
                    throw new IncompleteElementException("Could not find result map " + resultMapName, e);
                }
            }
        // 如果 resultType 非空，则创建 ResultMap 对象
        } else if (resultType != null) {
            ResultMap inlineResultMap = new ResultMap.Builder(
                    configuration,
                    statementId + "-Inline",
                    resultType,
                    new ArrayList<>(),
                    null).build();
            resultMaps.add(inlineResultMap);
        }
        return resultMaps;
    }


    /**
     * 构造 ResultMapping 对象：
     * <ol>
     *     <li>
     *         调用{@link #resolveResultJavaType(Class, String, Class)}传入{@code resultType}、{@code property}、{@code javaType}尝试解析映射关系的java类型
     *     </li>
     *     <li>
     *         调用{@link BaseBuilder#resolveTypeHandler(Class, Class)}尝试解析处理当前映射的{@link TypeHandler}对象
     *     </li>
     *     <li>
     *         调用{@link #parseCompositeColumnName(String)}尝试解析『复合字段』
     *     </li>
     *     <li>
     *         调用{@link ResultMapping.Builder#Builder(Configuration, String, String, Class)}传入{@link #configuration}、{@code property}、{@code column}、解析之后的{@code javaType} new一个{@link ResultMapping.Builder}对象
     *     </li>
     *     <li>
     *         调用{@link ResultMapping.Builder}的各api将入参给{@link ResultMapping}的属性进行值设置（详情参考方法内容），然后调用{@link ResultMapping.Builder#build()}方法构建一个{@link ResultMapping}对象并返回
     *     </li>
     * </ol>
     *
     * @param resultType 映射关系的承载类，由包裹当前标签的外两层或者外层标签的"type"、"ofType"、"resultType"、"javaType"属性指定
     * @param property 如果当前标签被{@code <constructor/>}标签所包裹，则取的是当前标签的"name"属性，表示当前要build的{@link ResultMapping}对象是{@code resultType}中一个构造器的某个参数到数据库表中某个字段的映射；否则是当前标签的"property"属性，表示{@code resultType}中某个属性到数据库表中某个字段的映射
     * @param column 当前标签中的"column"属性，指明当前映射的数据库表字段
     * @param javaType 当前标签的"javaType"属性，指明当前映射的java类型（也就是property对应的构造器参数类型或者属性类型）
     * @param jdbcType 当前标签的"jdbcType"属性，指明当前映射的jdbc类型（{@link JdbcType}），即数据库字段的类型
     * @param nestedSelect 当前标签的"select"属性
     * @param nestedResultMap 当前标签的"resultMap"属性
     * @param notNullColumn 当前标签的"notNullColumn"属性
     * @param columnPrefix 当前标签的"columnPrefix"属性
     * @param typeHandler 当前标签的"typeHandler"属性
     * @param flags 当前标签的tags（{@link ResultFlag}）
     * @param resultSet 当前标签的"resultSet"属性
     * @param foreignColumn 当前标签的"foreignColumn"属性
     * @param lazy 当前标签的"fetchType"属性
     * @return
     */
    public ResultMapping buildResultMapping(
            Class<?> resultType,
            String property,
            String column,
            Class<?> javaType,
            JdbcType jdbcType,
            String nestedSelect,
            String nestedResultMap,
            String notNullColumn,
            String columnPrefix,
            Class<? extends TypeHandler<?>> typeHandler,
            List<ResultFlag> flags,
            String resultSet,
            String foreignColumn,
            boolean lazy) {
        Class<?> javaTypeClass = resolveResultJavaType(resultType, property, javaType);
        TypeHandler<?> typeHandlerInstance = resolveTypeHandler(javaTypeClass, typeHandler);
        List<ResultMapping> composites = parseCompositeColumnName(column);
        return new ResultMapping.Builder(configuration, property, column, javaTypeClass)
                .jdbcType(jdbcType)
                .nestedQueryId(applyCurrentNamespace(nestedSelect, true))
                .nestedResultMapId(applyCurrentNamespace(nestedResultMap, true))
                .resultSet(resultSet)
                .typeHandler(typeHandlerInstance)
                .flags(flags == null ? new ArrayList<>() : flags)
                .composites(composites)
                .notNullColumns(parseMultipleColumnNames(notNullColumn))
                .columnPrefix(columnPrefix)
                .foreignColumn(foreignColumn)
                .lazy(lazy)
                .build();
    }


    private Set<String> parseMultipleColumnNames(String columnName) {
        Set<String> columns = new HashSet<>();
        if (columnName != null) {
            // 多个字段，使用 ，分隔
            if (columnName.indexOf(',') > -1) {
                StringTokenizer parser = new StringTokenizer(columnName, "{}, ", false);
                while (parser.hasMoreTokens()) {
                    String column = parser.nextToken();
                    columns.add(column);
                }
            } else {
                columns.add(columnName);
            }
        }
        return columns;
    }

    /**
     * 解析『复合的字段名称』(column="{prop1=col1,prop2=col2}")，参考<a href='http://www.mybatis.org/mybatis-3/zh/sqlmap-xml.html'>官方文档</a>的"关联的嵌套 Select 查询"：
     * <ol>
     *     <li>
     *         使用{@link StringTokenizer}使用"{}=, "等字符作为分隔符取出每一对property和column
     *     </li>
     *     <li>
     *         针对每一对property和column调用{@link ResultMapping.Builder#Builder(Configuration, String, String, TypeHandler)}分别传入{@link #configuration}、property、column、{@link Configuration}的{@link org.apache.ibatis.type.TypeHandlerRegistry}的{@link org.apache.ibatis.type.UnknownTypeHandler}4个参数new出一个{@link ResultMapping.Builder}对象并调用{@link ResultMapping.Builder#build()}方法构建一个{@link ResultMapping}对象
     *     </li>
     *     <li>
     *         将每一对property和column转化而来的{@link ResultMapping}对象用一个集合收集起来，返回该集合
     *     </li>
     * </ol>
     * @param columnName
     * @return
     */
    private List<ResultMapping> parseCompositeColumnName(String columnName) {
        List<ResultMapping> composites = new ArrayList<>();
        if (columnName != null && (columnName.indexOf('=') > -1 || columnName.indexOf(',') > -1)) {
            StringTokenizer parser = new StringTokenizer(columnName, "{}=, ", false);
            while (parser.hasMoreTokens()) {
                String property = parser.nextToken();
                String column = parser.nextToken();
                ResultMapping complexResultMapping = new ResultMapping.Builder(
                        configuration, property, column, configuration.getTypeHandlerRegistry().getUnknownTypeHandler()).build();
                composites.add(complexResultMapping);
            }
        }
        return composites;
    }

    /**
     * 当resultMapping中java类型未知时，利用resultMapping的承载类及该resultMapping对应的属性进行java类型解析
     *
     * <ul>
     *     <li>
     *         如果{@code javaType}不为null则直接返回{@code javaType}
     *     </li>
     *     <li>
     *         如果{@code javaType}为null则判断{@code property}是否为null：
     *         <ul>
     *             <li>
     *                 为null则返回{@link Object}.class
     *             </li>
     *             <li>
     *                 否则调用{@link MetaClass#forClass(Class, ReflectorFactory)}获取{@code resultType}对应的{@link MetaClass}对象然后调用{@link MetaClass#getSetterType(String)}获取{@code property}对应的{@link Class}对象并返回
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * @param resultType {@link ResultMapping}的承载类
     * @param property {@link ResultMapping}的承载类中当前字段映射对应的属性
     * @param javaType {@link ResultMapping}的承载类中当前字段映射对应的属性的类型
     * @return
     */
    private Class<?> resolveResultJavaType(Class<?> resultType, String property, Class<?> javaType) {
        if (javaType == null && property != null) {
            try {
                MetaClass metaResultType = MetaClass.forClass(resultType, configuration.getReflectorFactory());
                javaType = metaResultType.getSetterType(property);
            } catch (Exception e) {
                //ignore, following null check statement will deal with the situation
            }
        }
        if (javaType == null) {
            javaType = Object.class;
        }
        return javaType;
    }

    /**
     * 解析parameterMap中当前parameter的mapping对应的{@code javaType}
     *
     * <ol>
     *     <li>
     *         如果{@code javaType}不为null，则直接返回；否则继续
     *     </li>
     *     <li>
     *         如果当前parameterMapping的{@code jdbcType}为{@link JdbcType#CURSOR}，则返回{@link java.sql.ResultSet}.class；否则继续
     *     </li>
     *     <li>
     *         如果{@code parameterMappingType}为{@link Map}或者其子类，则返回{@link Object}.class（无法获得其属性类型）；否则继续
     *     </li>
     *     <li>
     *         来到这里认为{@code parameterMappingType}是一个JavaBean，使用{@link MetaClass#forClass(Class, ReflectorFactory)}获得{@code parameterMappingType}的{@link MetaClass}，然后调用{@link MetaClass#getGetterType(String)}传入{@code property}获得该属性的java类型，如果获得的结果不是null，则直接返回；否则继续
     *     </li>
     *     <li>
     *         返回{@link Object}.class
     *     </li>
     * </ol>
     *
     * @param parameterMapType paramterMapping的承载类
     * @param property parameterMapping承载类中针对当前parameter的property名称
     * @param javaType parameterMapping中当前parameter对应的java类型
     * @param jdbcType parameterMapping中当前parameter对应的jdbc类型
     * @return
     */
    private Class<?> resolveParameterJavaType(Class<?> parameterMapType, String property, Class<?> javaType, JdbcType jdbcType) {
        if (javaType == null) {
            if (JdbcType.CURSOR.equals(jdbcType)) {
                javaType = java.sql.ResultSet.class;
            } else if (Map.class.isAssignableFrom(parameterMapType)) {
                javaType = Object.class;
            } else {
                MetaClass metaResultType = MetaClass.forClass(parameterMapType, configuration.getReflectorFactory());
                javaType = metaResultType.getGetterType(property);
            }
        }
        if (javaType == null) {
            javaType = Object.class;
        }
        return javaType;
    }

    /** Backward compatibility signature */
    @Deprecated // add by 芋艿 保持兼容，目前未调用
    public ResultMapping buildResultMapping(
            Class<?> resultType,
            String property,
            String column,
            Class<?> javaType,
            JdbcType jdbcType,
            String nestedSelect,
            String nestedResultMap,
            String notNullColumn,
            String columnPrefix,
            Class<? extends TypeHandler<?>> typeHandler,
            List<ResultFlag> flags) {
        return buildResultMapping(
                resultType, property, column, javaType, jdbcType, nestedSelect,
                nestedResultMap, notNullColumn, columnPrefix, typeHandler, flags, null, null, configuration.isLazyLoadingEnabled());
    }

    /**
     * 获得对应的 {@link LanguageDriver} 对象：
     * <ol>
     *     <li>
     *         判断{@code langClass}是否为null：
     *         <ul>
     *             <li>
     *                 不为null则：调用成员变量{@link #configuration}的{@link Configuration#getLanguageRegistry()}获取
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param langClass "lang"属性对应的类（别名或者全限定名）
     * @return LanguageDriver 对象
     */
    public LanguageDriver getLanguageDriver(Class<? extends LanguageDriver> langClass) {
        if (langClass != null) {
            configuration.getLanguageRegistry().register(langClass);
        } else {
            langClass = configuration.getLanguageRegistry().getDefaultDriverClass();
        }
        return configuration.getLanguageRegistry().getDriver(langClass);
    }

    /** Backward compatibility signature */
    @Deprecated // add by 芋艿 保持兼容，目前未调用
    public MappedStatement addMappedStatement(
            String id,
            SqlSource sqlSource,
            StatementType statementType,
            SqlCommandType sqlCommandType,
            Integer fetchSize,
            Integer timeout,
            String parameterMap,
            Class<?> parameterType,
            String resultMap,
            Class<?> resultType,
            ResultSetType resultSetType,
            boolean flushCache,
            boolean useCache,
            boolean resultOrdered,
            KeyGenerator keyGenerator,
            String keyProperty,
            String keyColumn,
            String databaseId,
            LanguageDriver lang) {
        return addMappedStatement(
                id, sqlSource, statementType, sqlCommandType, fetchSize, timeout,
                parameterMap, parameterType, resultMap, resultType, resultSetType,
                flushCache, useCache, resultOrdered, keyGenerator, keyProperty,
                keyColumn, databaseId, lang, null);
    }

}