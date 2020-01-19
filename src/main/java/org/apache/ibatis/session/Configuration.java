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
package org.apache.ibatis.session;

import org.apache.ibatis.binding.MapperRegistry;
import org.apache.ibatis.builder.CacheRefResolver;
import org.apache.ibatis.builder.ResultMapResolver;
import org.apache.ibatis.builder.annotation.MethodResolver;
import org.apache.ibatis.builder.xml.XMLStatementBuilder;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.cache.decorators.FifoCache;
import org.apache.ibatis.cache.decorators.LruCache;
import org.apache.ibatis.cache.decorators.SoftCache;
import org.apache.ibatis.cache.decorators.WeakCache;
import org.apache.ibatis.cache.impl.PerpetualCache;
import org.apache.ibatis.datasource.jndi.JndiDataSourceFactory;
import org.apache.ibatis.datasource.pooled.PooledDataSourceFactory;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSourceFactory;
import org.apache.ibatis.executor.*;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.executor.loader.cglib.CglibProxyFactory;
import org.apache.ibatis.executor.loader.javassist.JavassistProxyFactory;
import org.apache.ibatis.executor.parameter.ParameterHandler;
import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.executor.statement.RoutingStatementHandler;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.logging.commons.JakartaCommonsLoggingImpl;
import org.apache.ibatis.logging.jdk14.Jdk14LoggingImpl;
import org.apache.ibatis.logging.log4j.Log4jImpl;
import org.apache.ibatis.logging.log4j2.Log4j2Impl;
import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.logging.slf4j.Slf4jImpl;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.plugin.InterceptorChain;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaObject;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.DefaultObjectFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.DefaultObjectWrapperFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.scripting.LanguageDriverRegistry;
import org.apache.ibatis.scripting.defaults.RawLanguageDriver;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeAliasRegistry;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.TypeHandlerRegistry;

import java.util.*;

/**
 * MyBatis 配置
 *
 * @author Clinton Begin
 */
public class Configuration {

    /**
     * DB Environment 对象
     */
    protected Environment environment;

    protected boolean safeRowBoundsEnabled;
    protected boolean safeResultHandlerEnabled = true;
    protected boolean mapUnderscoreToCamelCase;
    /**
     * 当开启时，任何方法的调用都会加载该对象的所有属性。否则，每个属性会按需加载（参考lazyLoadTriggerMethods)
     */
    protected boolean aggressiveLazyLoading;
    protected boolean multipleResultSetsEnabled = true;
    protected boolean useGeneratedKeys;
    protected boolean useColumnLabel = true;
    protected boolean cacheEnabled = true;
    protected boolean callSettersOnNulls;
    protected boolean useActualParamName = true;
    /**
     * 是否返回空行对应的对象
     */
    protected boolean returnInstanceForEmptyRow;

    protected String logPrefix;
    protected Class<? extends Log> logImpl;
    /**
     * VFS 实现类
     */
    protected Class<? extends VFS> vfsImpl;
    /**
     * {@link BaseExecutor} 本地缓存范围
     */
    protected LocalCacheScope localCacheScope = LocalCacheScope.SESSION;
    protected JdbcType jdbcTypeForNull = JdbcType.OTHER;
    /**
     * 指定哪个对象的方法触发一次延迟加载。
     */
    protected Set<String> lazyLoadTriggerMethods = new HashSet<>(Arrays.asList("equals", "clone", "hashCode", "toString"));
    protected Integer defaultStatementTimeout;
    protected Integer defaultFetchSize;
    protected ExecutorType defaultExecutorType = ExecutorType.SIMPLE;
    /**
     * 自动映射行为
     */
    protected AutoMappingBehavior autoMappingBehavior = AutoMappingBehavior.PARTIAL;
    /**
     * 自动映射失败的行为处理
     */
    protected AutoMappingUnknownColumnBehavior autoMappingUnknownColumnBehavior = AutoMappingUnknownColumnBehavior.NONE;

    /**
     * 变量 Properties 对象。
     *
     * 参见 {@link org.apache.ibatis.builder.xml.XMLConfigBuilder#propertiesElement(XNode context)} 方法
     */
    protected Properties variables = new Properties();
    /**
     * ReflectorFactory 对象
     */
    protected ReflectorFactory reflectorFactory = new DefaultReflectorFactory();
    /**
     * ObjectFactory 对象
     */
    protected ObjectFactory objectFactory = new DefaultObjectFactory();
    /**
     * ObjectWrapperFactory 对象
     */
    protected ObjectWrapperFactory objectWrapperFactory = new DefaultObjectWrapperFactory();

    /**
     * 延迟加载的全局开关。当开启时，所有关联对象都会延迟加载。 特定关联关系中可通过设置fetchType属性来覆盖该项的开关状态。
     */
    protected boolean lazyLoadingEnabled = false;
    protected ProxyFactory proxyFactory = new JavassistProxyFactory(); // #224 Using internal Javassist instead of OGNL

    /**
     * 数据库标识
     */
    protected String databaseId;
    /**
     * Configuration factory class.
     * Used to create Configuration for loading deserialized unread properties.
     *
     * @see <a href='https://code.google.com/p/mybatis/issues/detail?id=300'>Issue 300 (google code)</a>
     */
    protected Class<?> configurationFactory;

    /**
     * MapperRegistry 对象
     */
    protected final MapperRegistry mapperRegistry = new MapperRegistry(this);
    /**
     * 拦截器链
     */
    protected final InterceptorChain interceptorChain = new InterceptorChain();
    /**
     * TypeHandlerRegistry 对象
     */
    protected final TypeHandlerRegistry typeHandlerRegistry = new TypeHandlerRegistry();
    /**
     * TypeAliasRegistry 对象
     */
    protected final TypeAliasRegistry typeAliasRegistry = new TypeAliasRegistry();
    /**
     * LanguageDriverRegistry 对象
     */
    protected final LanguageDriverRegistry languageRegistry = new LanguageDriverRegistry();

    /**
     * MappedStatement 映射
     *
     * KEY：`${namespace}.${id}`
     */
    protected final Map<String, MappedStatement> mappedStatements = new StrictMap<>("Mapped Statements collection");
    /**
     * Cache 对象Map：
     *
     * key为完整的namespace或者简短的namespace（{@link StrictMap#getShortName(String)}）
     */
    protected final Map<String, Cache> caches = new StrictMap<>("Caches collection");
    /**
     * ResultMap 的映射
     *
     * KEY：`${namespace}.${id}`
     */
    protected final Map<String, ResultMap> resultMaps = new StrictMap<>("Result Maps collection");
    /**
     * ParameterMap的映射
     *
     * KEY：`${namespace}.${id}`
     */
    protected final Map<String, ParameterMap> parameterMaps = new StrictMap<>("Parameter Maps collection");
    /**
     * KeyGenerator 的映射
     *
     * KEY：在 {@link #mappedStatements} 的 KEY 的基础上，跟上 {@link SelectKeyGenerator#SELECT_KEY_SUFFIX}
     */
    protected final Map<String, KeyGenerator> keyGenerators = new StrictMap<>("Key Generators collection");

    /**
     * 已加载资源( Resource )集合，其中资源包括：
     * <ol>
     *     <li>
     *         "namespace:"+Mapper接口的全限定名（表示已经加载的mapper xml文件）
     *     </li>
     *     <li>
     *         Mapper接口class对象的{@link Class#toString()}【表示已经加载了的mapper的class对象（解析注解等）】
     *     </li>
     * </ol>
     */
    protected final Set<String> loadedResources = new HashSet<>();
    /**
     * 可被其他语句引用的可重用语句块的集合
     *
     * 例如：<sql id="userColumns"> ${alias}.id,${alias}.username,${alias}.password </sql>
     */
    protected final Map<String, XNode> sqlFragments = new StrictMap<>("XML fragments parsed from previous mappers");

    /**
     * 未完成的 XMLStatementBuilder 集合
     */
    protected final Collection<XMLStatementBuilder> incompleteStatements = new LinkedList<>();
    /**
     * 未完成的 CacheRefResolver 集合
     */
    protected final Collection<CacheRefResolver> incompleteCacheRefs = new LinkedList<>();
    /**
     * 未完成的 ResultMapResolver 集合
     */
    protected final Collection<ResultMapResolver> incompleteResultMaps = new LinkedList<>();
    /**
     * 未完成的 MethodResolver 集合
     */
    protected final Collection<MethodResolver> incompleteMethods = new LinkedList<>();

    /**
     * A map holds cache-ref relationship. The key is the namespace that
     * references a cache bound to another namespace and the value is the
     * namespace which the actual cache is bound to.
     *
     *
     * @see #addCacheRef(String, String)
     * @see org.apache.ibatis.builder.xml.XMLMapperBuilder#cacheRefElement(XNode)
     */
    protected final Map<String, String> cacheRefMap = new HashMap<>();

    public Configuration(Environment environment) {
        this();
        this.environment = environment;
    }

    public Configuration() {
        // 注册到 typeAliasRegistry 中 begin ~~~~
        typeAliasRegistry.registerAlias("JDBC", JdbcTransactionFactory.class);
        typeAliasRegistry.registerAlias("MANAGED", ManagedTransactionFactory.class);

        typeAliasRegistry.registerAlias("JNDI", JndiDataSourceFactory.class);
        typeAliasRegistry.registerAlias("POOLED", PooledDataSourceFactory.class);
        typeAliasRegistry.registerAlias("UNPOOLED", UnpooledDataSourceFactory.class);

        typeAliasRegistry.registerAlias("PERPETUAL", PerpetualCache.class);
        typeAliasRegistry.registerAlias("FIFO", FifoCache.class);
        typeAliasRegistry.registerAlias("LRU", LruCache.class);
        typeAliasRegistry.registerAlias("SOFT", SoftCache.class);
        typeAliasRegistry.registerAlias("WEAK", WeakCache.class);

        typeAliasRegistry.registerAlias("DB_VENDOR", VendorDatabaseIdProvider.class);

        typeAliasRegistry.registerAlias("XML", XMLLanguageDriver.class);
        typeAliasRegistry.registerAlias("RAW", RawLanguageDriver.class);

        typeAliasRegistry.registerAlias("SLF4J", Slf4jImpl.class);
        typeAliasRegistry.registerAlias("COMMONS_LOGGING", JakartaCommonsLoggingImpl.class);
        typeAliasRegistry.registerAlias("LOG4J", Log4jImpl.class);
        typeAliasRegistry.registerAlias("LOG4J2", Log4j2Impl.class);
        typeAliasRegistry.registerAlias("JDK_LOGGING", Jdk14LoggingImpl.class);
        typeAliasRegistry.registerAlias("STDOUT_LOGGING", StdOutImpl.class);
        typeAliasRegistry.registerAlias("NO_LOGGING", NoLoggingImpl.class);

        typeAliasRegistry.registerAlias("CGLIB", CglibProxyFactory.class);
        typeAliasRegistry.registerAlias("JAVASSIST", JavassistProxyFactory.class);
        // 注册到 typeAliasRegistry 中 end ~~~~

        // 注册到 languageRegistry 中
        languageRegistry.setDefaultDriverClass(XMLLanguageDriver.class);
        languageRegistry.register(RawLanguageDriver.class);
    }

    public String getLogPrefix() {
        return logPrefix;
    }

    public void setLogPrefix(String logPrefix) {
        this.logPrefix = logPrefix;
    }

    public Class<? extends Log> getLogImpl() {
        return logImpl;
    }

    public void setLogImpl(Class<? extends Log> logImpl) {
        if (logImpl != null) {
            this.logImpl = logImpl;
            LogFactory.useCustomLogging(this.logImpl);
        }
    }

    public Class<? extends VFS> getVfsImpl() {
        return this.vfsImpl;
    }

    /**
     * 设置自定义{@link VFS}实现类，先将{@code vfsImpl}设置为{@link Configuration#vfsImpl}，然后调用{@link VFS#addImplClass(Class)}
     *
     * @param vfsImpl
     */
    public void setVfsImpl(Class<? extends VFS> vfsImpl) {
        if (vfsImpl != null) {
            this.vfsImpl = vfsImpl;
            VFS.addImplClass(this.vfsImpl);
        }
    }

    public boolean isCallSettersOnNulls() {
        return callSettersOnNulls;
    }

    public void setCallSettersOnNulls(boolean callSettersOnNulls) {
        this.callSettersOnNulls = callSettersOnNulls;
    }

    public boolean isUseActualParamName() {
        return useActualParamName;
    }

    public void setUseActualParamName(boolean useActualParamName) {
        this.useActualParamName = useActualParamName;
    }

    public boolean isReturnInstanceForEmptyRow() {
        return returnInstanceForEmptyRow;
    }

    public void setReturnInstanceForEmptyRow(boolean returnEmptyInstance) {
        this.returnInstanceForEmptyRow = returnEmptyInstance;
    }

    public String getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(String databaseId) {
        this.databaseId = databaseId;
    }

    public Class<?> getConfigurationFactory() {
        return configurationFactory;
    }

    public void setConfigurationFactory(Class<?> configurationFactory) {
        this.configurationFactory = configurationFactory;
    }

    public boolean isSafeResultHandlerEnabled() {
        return safeResultHandlerEnabled;
    }

    public void setSafeResultHandlerEnabled(boolean safeResultHandlerEnabled) {
        this.safeResultHandlerEnabled = safeResultHandlerEnabled;
    }

    public boolean isSafeRowBoundsEnabled() {
        return safeRowBoundsEnabled;
    }

    public void setSafeRowBoundsEnabled(boolean safeRowBoundsEnabled) {
        this.safeRowBoundsEnabled = safeRowBoundsEnabled;
    }

    public boolean isMapUnderscoreToCamelCase() {
        return mapUnderscoreToCamelCase;
    }

    public void setMapUnderscoreToCamelCase(boolean mapUnderscoreToCamelCase) {
        this.mapUnderscoreToCamelCase = mapUnderscoreToCamelCase;
    }

    /**
     * 调用成员变量{@link #loadedResources}的{@link HashSet#add(Object)}传入{@code resource}添加到集合中
     *
     * @param resource
     */
    public void addLoadedResource(String resource) {
        loadedResources.add(resource);
    }

    /**
     * {@link Configuration#loadedResources}是否{@link Set#contains(Object)}
     *
     * @param resource
     * @return
     */
    public boolean isResourceLoaded(String resource) {
        return loadedResources.contains(resource);
    }

    public Environment getEnvironment() {
        return environment;
    }

    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }

    public AutoMappingBehavior getAutoMappingBehavior() {
        return autoMappingBehavior;
    }

    public void setAutoMappingBehavior(AutoMappingBehavior autoMappingBehavior) {
        this.autoMappingBehavior = autoMappingBehavior;
    }

    /**
     * @since 3.4.0
     */
    public AutoMappingUnknownColumnBehavior getAutoMappingUnknownColumnBehavior() {
        return autoMappingUnknownColumnBehavior;
    }

    /**
     * @since 3.4.0
     */
    public void setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior autoMappingUnknownColumnBehavior) {
        this.autoMappingUnknownColumnBehavior = autoMappingUnknownColumnBehavior;
    }

    public boolean isLazyLoadingEnabled() {
        return lazyLoadingEnabled;
    }

    public void setLazyLoadingEnabled(boolean lazyLoadingEnabled) {
        this.lazyLoadingEnabled = lazyLoadingEnabled;
    }

    public ProxyFactory getProxyFactory() {
        return proxyFactory;
    }

    public void setProxyFactory(ProxyFactory proxyFactory) {
        if (proxyFactory == null) {
            proxyFactory = new JavassistProxyFactory();
        }
        this.proxyFactory = proxyFactory;
    }

    public boolean isAggressiveLazyLoading() {
        return aggressiveLazyLoading;
    }

    public void setAggressiveLazyLoading(boolean aggressiveLazyLoading) {
        this.aggressiveLazyLoading = aggressiveLazyLoading;
    }

    public boolean isMultipleResultSetsEnabled() {
        return multipleResultSetsEnabled;
    }

    public void setMultipleResultSetsEnabled(boolean multipleResultSetsEnabled) {
        this.multipleResultSetsEnabled = multipleResultSetsEnabled;
    }

    public Set<String> getLazyLoadTriggerMethods() {
        return lazyLoadTriggerMethods;
    }

    public void setLazyLoadTriggerMethods(Set<String> lazyLoadTriggerMethods) {
        this.lazyLoadTriggerMethods = lazyLoadTriggerMethods;
    }

    public boolean isUseGeneratedKeys() {
        return useGeneratedKeys;
    }

    public void setUseGeneratedKeys(boolean useGeneratedKeys) {
        this.useGeneratedKeys = useGeneratedKeys;
    }

    public ExecutorType getDefaultExecutorType() {
        return defaultExecutorType;
    }

    public void setDefaultExecutorType(ExecutorType defaultExecutorType) {
        this.defaultExecutorType = defaultExecutorType;
    }

    public boolean isCacheEnabled() {
        return cacheEnabled;
    }

    public void setCacheEnabled(boolean cacheEnabled) {
        this.cacheEnabled = cacheEnabled;
    }

    public Integer getDefaultStatementTimeout() {
        return defaultStatementTimeout;
    }

    public void setDefaultStatementTimeout(Integer defaultStatementTimeout) {
        this.defaultStatementTimeout = defaultStatementTimeout;
    }

    /**
     * @since 3.3.0
     */
    public Integer getDefaultFetchSize() {
        return defaultFetchSize;
    }

    /**
     * @since 3.3.0
     */
    public void setDefaultFetchSize(Integer defaultFetchSize) {
        this.defaultFetchSize = defaultFetchSize;
    }

    public boolean isUseColumnLabel() {
        return useColumnLabel;
    }

    public void setUseColumnLabel(boolean useColumnLabel) {
        this.useColumnLabel = useColumnLabel;
    }

    public LocalCacheScope getLocalCacheScope() {
        return localCacheScope;
    }

    public void setLocalCacheScope(LocalCacheScope localCacheScope) {
        this.localCacheScope = localCacheScope;
    }

    public JdbcType getJdbcTypeForNull() {
        return jdbcTypeForNull;
    }

    public void setJdbcTypeForNull(JdbcType jdbcTypeForNull) {
        this.jdbcTypeForNull = jdbcTypeForNull;
    }

    public Properties getVariables() {
        return variables;
    }

    public void setVariables(Properties variables) {
        this.variables = variables;
    }

    public TypeHandlerRegistry getTypeHandlerRegistry() {
        return typeHandlerRegistry;
    }

    /**
     * Set a default {@link TypeHandler} class for {@link Enum}.
     * A default {@link TypeHandler} is {@link org.apache.ibatis.type.EnumTypeHandler}.
     * @param typeHandler a type handler class for {@link Enum}
     * @since 3.4.5
     */
    public void setDefaultEnumTypeHandler(Class<? extends TypeHandler> typeHandler) {
        if (typeHandler != null) {
            getTypeHandlerRegistry().setDefaultEnumTypeHandler(typeHandler);
        }
    }

    public TypeAliasRegistry getTypeAliasRegistry() {
        return typeAliasRegistry;
    }

    /**
     * @since 3.2.2
     */
    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

    public ReflectorFactory getReflectorFactory() {
        return reflectorFactory;
    }

    public void setReflectorFactory(ReflectorFactory reflectorFactory) {
        this.reflectorFactory = reflectorFactory;
    }

    public ObjectFactory getObjectFactory() {
        return objectFactory;
    }

    public void setObjectFactory(ObjectFactory objectFactory) {
        this.objectFactory = objectFactory;
    }

    public ObjectWrapperFactory getObjectWrapperFactory() {
        return objectWrapperFactory;
    }

    public void setObjectWrapperFactory(ObjectWrapperFactory objectWrapperFactory) {
        this.objectWrapperFactory = objectWrapperFactory;
    }

    /**
     * @since 3.2.2
     */
    public List<Interceptor> getInterceptors() {
        return interceptorChain.getInterceptors();
    }

    /**
     * 返回{@link #languageRegistry}
     *
     * @return
     */
    public LanguageDriverRegistry getLanguageRegistry() {
        return languageRegistry;
    }

    public void setDefaultScriptingLanguage(Class<? extends LanguageDriver> driver) {
        if (driver == null) {
            driver = XMLLanguageDriver.class;
        }
        getLanguageRegistry().setDefaultDriverClass(driver);
    }

    /**
     * @return {@link #languageRegistry}的{@link LanguageDriverRegistry#getDefaultDriver()}
     */
    public LanguageDriver getDefaultScriptingLanguageInstance() {
        return languageRegistry.getDefaultDriver();
    }

    /** @deprecated Use {@link #getDefaultScriptingLanguageInstance()} */
    @Deprecated
    public LanguageDriver getDefaultScriptingLanuageInstance() {
        return getDefaultScriptingLanguageInstance();
    }

    /**
     * 调用{@link MetaObject#forObject(Object, ObjectFactory, ObjectWrapperFactory, ReflectorFactory)}传入{@code object}、{@link #objectFactory}、{@link #objectWrapperFactory}、{@link #reflectorFactory}构建传入对象的{@link MetaObject}对象并返回
     *
     * @param object
     * @return
     */
    public MetaObject newMetaObject(Object object) {
        return MetaObject.forObject(object, objectFactory, objectWrapperFactory, reflectorFactory);
    }

    // 创建 ParameterHandler 对象
    public ParameterHandler newParameterHandler(MappedStatement mappedStatement, Object parameterObject, BoundSql boundSql) {
        // 创建 ParameterHandler 对象
        ParameterHandler parameterHandler = mappedStatement.getLang().createParameterHandler(mappedStatement, parameterObject, boundSql);
        // 应用插件
        parameterHandler = (ParameterHandler) interceptorChain.pluginAll(parameterHandler);
        return parameterHandler;
    }

    // 创建 ResultSetHandler 对象
    public ResultSetHandler newResultSetHandler(Executor executor, MappedStatement mappedStatement, RowBounds rowBounds, ParameterHandler parameterHandler,
                                                ResultHandler resultHandler, BoundSql boundSql) {
        // 创建 DefaultResultSetHandler 对象
        ResultSetHandler resultSetHandler = new DefaultResultSetHandler(executor, mappedStatement, parameterHandler, resultHandler, boundSql, rowBounds);
        // 应用插件
        resultSetHandler = (ResultSetHandler) interceptorChain.pluginAll(resultSetHandler);
        return resultSetHandler;
    }

    // 创建 StatementHandler 对象
    public StatementHandler newStatementHandler(Executor executor, MappedStatement mappedStatement, Object parameterObject, RowBounds rowBounds, ResultHandler resultHandler, BoundSql boundSql) {
        // 创建 RoutingStatementHandler 对象
        StatementHandler statementHandler = new RoutingStatementHandler(executor, mappedStatement, parameterObject, rowBounds, resultHandler, boundSql);
        // 应用插件
        statementHandler = (StatementHandler) interceptorChain.pluginAll(statementHandler);
        return statementHandler;
    }

    public Executor newExecutor(Transaction transaction) {
        return newExecutor(transaction, defaultExecutorType);
    }

    /**
     * 创建 Executor 对象
     *
     * @param transaction 事务对象
     * @param executorType 执行器类型
     * @return Executor 对象
     */
    public Executor newExecutor(Transaction transaction, ExecutorType executorType) {
        // 获得执行器类型
        executorType = executorType == null ? defaultExecutorType : executorType; // 使用默认
        executorType = executorType == null ? ExecutorType.SIMPLE : executorType; // 使用 ExecutorType.SIMPLE
        // 创建对应实现的 Executor 对象
        Executor executor;
        if (ExecutorType.BATCH == executorType) {
            executor = new BatchExecutor(this, transaction);
        } else if (ExecutorType.REUSE == executorType) {
            executor = new ReuseExecutor(this, transaction);
        } else {
            executor = new SimpleExecutor(this, transaction);
        }
        // 如果开启缓存，创建 CachingExecutor 对象，进行包装
        if (cacheEnabled) {
            executor = new CachingExecutor(executor);
        }
        // 应用插件
        executor = (Executor) interceptorChain.pluginAll(executor);
        return executor;
    }

    /**
     * 调用成员变量{@link #keyGenerators}的{@link StrictMap#put(String, Object)}方法将{@code id}作为key，{@code keyGenerator}本身作为value进行设置
     *
     * @param id
     * @param keyGenerator
     */
    public void addKeyGenerator(String id, KeyGenerator keyGenerator) {
        keyGenerators.put(id, keyGenerator);
    }

    public Collection<String> getKeyGeneratorNames() {
        return keyGenerators.keySet();
    }

    public Collection<KeyGenerator> getKeyGenerators() {
        return keyGenerators.values();
    }

    /**
     * 调用成员变量{@link #keyGenerators}的方法{@link StrictMap#get(Object)}传入{@code id}获取对应的{@link KeyGenerator}对象并返回
     *
     * @param id
     * @return
     */
    public KeyGenerator getKeyGenerator(String id) {
        return keyGenerators.get(id);
    }

    /**
     * 调用成员变量{@link #keyGenerators}的非重写方法{@link HashMap#containsKey(Object)}传入{@code id}并返回结果
     *
     * @param id
     * @return
     */
    public boolean hasKeyGenerator(String id) {
        return keyGenerators.containsKey(id);
    }

    /**
     * 调用成员变量{@link #caches}的{@link StrictMap#put(String, Object)}方法将{@code cache}的{@link Cache#getId()}作为key，{@code cache}本身作为value进行设置
     *
     * @param cache
     */
    public void addCache(Cache cache) {
        caches.put(cache.getId(), cache);
    }

    public Collection<String> getCacheNames() {
        return caches.keySet();
    }

    public Collection<Cache> getCaches() {
        return caches.values();
    }

    /**
     * 调用成员变量{@link #caches}的{@link StrictMap#get(Object)}获取{@link Cache}对象
     *
     * @param id
     * @return
     */
    public Cache getCache(String id) {
        return caches.get(id);
    }

    public boolean hasCache(String id) {
        return caches.containsKey(id);
    }

    /**
     * <ol>
     *     <li>
     *         调用成员变量{@link #resultMaps}的重写方法{@link StrictMap#put(String, Object)}传入{@code rm}的id和{@code rm}本身记录{@code rm}到{@link #resultMaps}
     *     </li>
     *     <li>
     *         调用{@link #checkLocallyForDiscriminatedNestedResultMaps(ResultMap)}检查{@code rm}是否存在拥有nested result map的discriminator result map
     *     </li>
     *     <li>
     *         调用{@link #checkGloballyForDiscriminatedNestedResultMaps(ResultMap)}检查{@code rm}是否拥有过nested result map且当前{@link Configuration}中所有{@link ResultMap}对象是否有通过discriminator引用了{@code rm}的情况
     *     </li>
     * </ol>
     *
     * @param rm 要记录的{@link ResultMap}对象
     */
    public void addResultMap(ResultMap rm) {
        // 添加到 resultMaps 中
        resultMaps.put(rm.getId(), rm);
        // TODO
        checkLocallyForDiscriminatedNestedResultMaps(rm);
        // TODO
        checkGloballyForDiscriminatedNestedResultMaps(rm);
    }

    /**
     * @return {@link #resultMaps}.keySet()
     */
    public Collection<String> getResultMapNames() {
        return resultMaps.keySet();
    }

    /**
     * @return {@link #resultMaps}.values()
     */
    public Collection<ResultMap> getResultMaps() {
        return resultMaps.values();
    }

    /**
     * 传入一个{@link ResultMap}对象的id（全称或者缩写），调用成员变量{@link #resultMaps}.get(id)（{@link StrictMap#get(Object)}）获取对应的{@link ResultMap}对象
     *
     * @param id
     * @return
     */
    public ResultMap getResultMap(String id) {
        return resultMaps.get(id);
    }

    /**
     * 传入一个{@link ResultMap}对象的id（全称或者缩写），调用成员变量{@link #resultMaps}.containsKey(id)来判断是否拥有该对象
     *
     * @param id
     * @return
     */
    public boolean hasResultMap(String id) {
        return resultMaps.containsKey(id);
    }

    /**
     * 根据{@code pm}的{@link ParameterMap#getId()}作为key，{@code pm}作为value，然后调用成员变量{@link #parameterMaps}的{@link StrictMap#put(String, Object)}方法put到容器中
     *
     * @param pm
     */
    public void addParameterMap(ParameterMap pm) {
        parameterMaps.put(pm.getId(), pm);
    }

    public Collection<String> getParameterMapNames() {
        return parameterMaps.keySet();
    }

    public Collection<ParameterMap> getParameterMaps() {
        return parameterMaps.values();
    }
    /**
     * 传入一个{@link ParameterMap}对象的id（全称或者缩写），调用成员变量{@link #parameterMaps}.get(id)（{@link StrictMap#get(Object)}）获取对应的{@link ParameterMap}对象
     *
     * @param id
     * @return
     */
    public ParameterMap getParameterMap(String id) {
        return parameterMaps.get(id);
    }

    public boolean hasParameterMap(String id) {
        return parameterMaps.containsKey(id);
    }

    /**
     * 调用成员变量{@link #mappedStatements}的重写方法{@link StrictMap#put(String, Object)}传入{@code ms}的id和{@code ms}本身记录{@code ms}到{@link #mappedStatements}
     *
     * @param ms
     */
    public void addMappedStatement(MappedStatement ms) {
        mappedStatements.put(ms.getId(), ms);
    }

    public Collection<String> getMappedStatementNames() {
        buildAllStatements();
        return mappedStatements.keySet();
    }

    public Collection<MappedStatement> getMappedStatements() {
        buildAllStatements();
        return mappedStatements.values();
    }

    public Collection<XMLStatementBuilder> getIncompleteStatements() {
        return incompleteStatements;
    }

    public void addIncompleteStatement(XMLStatementBuilder incompleteStatement) {
        incompleteStatements.add(incompleteStatement);
    }

    public Collection<CacheRefResolver> getIncompleteCacheRefs() {
        return incompleteCacheRefs;
    }

    public void addIncompleteCacheRef(CacheRefResolver incompleteCacheRef) {
        incompleteCacheRefs.add(incompleteCacheRef);
    }

    public Collection<ResultMapResolver> getIncompleteResultMaps() {
        return incompleteResultMaps;
    }

    public void addIncompleteResultMap(ResultMapResolver resultMapResolver) {
        incompleteResultMaps.add(resultMapResolver);
    }

    /**
     * 调用成员变量{@link #incompleteMethods}的{@link LinkedList#add(Object)}传入{@code builder}
     *
     * @param builder
     */
    public void addIncompleteMethod(MethodResolver builder) {
        incompleteMethods.add(builder);
    }

    public Collection<MethodResolver> getIncompleteMethods() {
        return incompleteMethods;
    }

    public MappedStatement getMappedStatement(String id) {
        return this.getMappedStatement(id, true);
    }

    /**
     * <ol>
     *     <li>
     *         如果{@code validateIncompleteStatements}为true则调用{@link #buildAllStatements()}（TODO :这里暂时看不太明白）
     *     </li>
     *     <li>
     *         调用成员变量{@link #mappedStatements}的方法{@link StrictMap#get(Object)}传入{@code id}获取对应的{@link MappedStatement}对象并返回
     *     </li>
     * </ol>
     *
     *
     * @param id
     * @param validateIncompleteStatements
     * @return
     */
    public MappedStatement getMappedStatement(String id, boolean validateIncompleteStatements) {
        if (validateIncompleteStatements) {
            buildAllStatements();
        }
        return mappedStatements.get(id);
    }

    public Map<String, XNode> getSqlFragments() {
        return sqlFragments;
    }

    public void addInterceptor(Interceptor interceptor) {
        interceptorChain.addInterceptor(interceptor);
    }

    public void addMappers(String packageName, Class<?> superType) {
        mapperRegistry.addMappers(packageName, superType);
    }

    /**
     * 调用{@link #mapperRegistry}的{@link MapperRegistry#addMappers(String)}传入{@code packageName}
     *
     * @param packageName 包名
     */
    public void addMappers(String packageName) {
        mapperRegistry.addMappers(packageName);
    }

    /**
     * 调用成员变量{@link #mapperRegistry}的{@link MapperRegistry#addMapper(Class)}传入{@code type}
     *
     * @param type mapper{@link Class}对象
     * @param <T>
     */
    public <T> void addMapper(Class<T> type) {
        mapperRegistry.addMapper(type);
    }

    public <T> T getMapper(Class<T> type, SqlSession sqlSession) {
        return mapperRegistry.getMapper(type, sqlSession);
    }

    /**
     * 调用成员变量{@link #mapperRegistry}的{@link MapperRegistry#hasMapper(Class)}传入{@code type}
     *
     * @param type
     * @return
     */
    public boolean hasMapper(Class<?> type) {
        return mapperRegistry.hasMapper(type);
    }

    public boolean hasStatement(String statementName) {
        return hasStatement(statementName, true);
    }

    /**
     * <ol>
     *     <li>
     *         如果{@code validateIncompleteStatements}为true则调用{@link #buildAllStatements()}（TODO :这里暂时看不太明白）
     *     </li>
     *     <li>
     *         调用成员变量{@link #mappedStatements}的方法{@link StrictMap#containsKey(Object)}传入{@code statementName}判断是否存在该id对应的{@link MappedStatement}并把判断结果返回
     *     </li>
     * </ol>
     *
     *
     * @param statementName id
     * @param validateIncompleteStatements
     * @return
     */
    public boolean hasStatement(String statementName, boolean validateIncompleteStatements) {
        if (validateIncompleteStatements) {
            buildAllStatements();
        }
        return mappedStatements.containsKey(statementName);
    }

    public void addCacheRef(String namespace, String referencedNamespace) {
        cacheRefMap.put(namespace, referencedNamespace);
    }

    /**
     * Parses all the unprocessed statement nodes in the cache. It is recommended
     * to call this method once all the mappers are added as it provides fail-fast
     * statement validation. <br> (TODO: 看不太懂)
     *
     * <ol>
     *     <li>
     *         如果!{@link #incompleteResultMaps}.isEmpty()则：使用 synchronized 锁住 {@link #incompleteResultMaps}，调用 {@link #incompleteResultMaps}.iterator().next().resolve();
     *     </li>
     *     <li>
     *         如果!{@link #incompleteCacheRefs}.isEmpty()则：使用 synchronized 锁住 {@link #incompleteCacheRefs}，调用 {@link #incompleteCacheRefs}.iterator().next().resolveCacheRef();
     *     </li>
     *     <li>
     *         如果!{@link #incompleteStatements}.isEmpty()则：使用 synchronized 锁住 {@link #incompleteStatements}，调用 {@link #incompleteStatements}.iterator().next().parseStatementNode();
     *     </li>
     *     <li>
     *         如果!{@link #incompleteMethods}.isEmpty()则：使用 synchronized 锁住 {@link #incompleteMethods}，调用 {@link #incompleteMethods}.iterator().next().resolve();
     *     </li>
     * </ol>
     */
    protected void buildAllStatements() {
        if (!incompleteResultMaps.isEmpty()) {
            synchronized (incompleteResultMaps) { // 保证 incompleteResultMaps 被解析完
                // This always throws a BuilderException.
                incompleteResultMaps.iterator().next().resolve();
            }
        }
        if (!incompleteCacheRefs.isEmpty()) {
            synchronized (incompleteCacheRefs) { // 保证 incompleteCacheRefs 被解析完
                // This always throws a BuilderException.
                incompleteCacheRefs.iterator().next().resolveCacheRef();
            }
        }
        if (!incompleteStatements.isEmpty()) {
            synchronized (incompleteStatements) { // 保证 incompleteStatements 被解析完
                // This always throws a BuilderException.
                incompleteStatements.iterator().next().parseStatementNode();
            }
        }
        if (!incompleteMethods.isEmpty()) {
            synchronized (incompleteMethods) { // 保证 incompleteMethods 被解析完
                // This always throws a BuilderException.
                incompleteMethods.iterator().next().resolve();
            }
        }
    }

    /*
     * Extracts namespace from fully qualified statement id.
     *
     * @param statementId
     * @return namespace or null when id does not contain period.
     */
    protected String extractNamespace(String statementId) {
        int lastPeriod = statementId.lastIndexOf('.');
        return lastPeriod > 0 ? statementId.substring(0, lastPeriod) : null;
    }


    /**
     * 如果{@code rm}拥有nested result map则将所有通过{@link Discriminator}引用了当前{@code rm}的{@link ResultMap}调用{@link ResultMap#forceNestedResultMaps()}标识为拥有nested result map：
     * <ol>
     *     <li>
     *         如果{@code rm}的{@link ResultMap#hasNestedResultMaps()}为true则继续往下，否则直接返回
     *     </li>
     *     <li>
     *         遍历当前{@link #resultMaps}的value（所有{@link ResultMap}对象）：
     *         <ul>
     *             <li>
     *                 如果当前迭代的{@link ResultMap}对象：不为null且{@link ResultMap#hasNestedResultMaps()}为false且{@link ResultMap#getDiscriminator()}不为null，则通过{@link ResultMap#getDiscriminator()}然后再调用{@link Discriminator#getDiscriminatorMap()}最终得到一个{@link Map#values()}
     *                 得到当前迭代的{@link ResultMap}对象的所有通过{@link Discriminator}指向的{@link ResultMap}的id，然后判断这些id中是否包含了{@code rm}的id，如果包含就调用当前迭代的{@link ResultMap#forceNestedResultMaps()}
     *             </li>
     *             <li>
     *                 否则什么都不做
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param rm ResultMap 对象
     */
    // Slow but a one time cost. A better solution is welcome.
    protected void checkGloballyForDiscriminatedNestedResultMaps(ResultMap rm) {
        if (rm.hasNestedResultMaps()) {
            for (Map.Entry<String, ResultMap> entry : resultMaps.entrySet()) {
                Object value = entry.getValue();
                if (value != null) {
                    ResultMap entryResultMap = (ResultMap) value;
                    if (!entryResultMap.hasNestedResultMaps() && entryResultMap.getDiscriminator() != null) {
                        Collection<String> discriminatedResultMapNames = entryResultMap.getDiscriminator().getDiscriminatorMap().values();
                        if (discriminatedResultMapNames.contains(rm.getId())) {
                            entryResultMap.forceNestedResultMaps();
                        }
                    }
                }
            }
        }
    }

    /**
     * 判断{@code rm}是否有通过{@link Discriminator}指向的{@link ResultMap}对象，这些{@link ResultMap}对象是否有nested result map，如果有就给当前{@code rm}也打上拥有nested result map的标识：
     * <ol>
     *     <li>
     *         如果{@code rm}的{@link ResultMap#hasNestedResultMaps()}为false且{@link ResultMap#getDiscriminator()}不为null，则继续往下；否则什么都不做直接返回
     *     </li>
     *     <li>
     *         通过{@link ResultMap#getDiscriminator()}然后再通过{@link Discriminator#getDiscriminatorMap()}得到一个{@link Map}，然后或者这个{@link Map#values()}即为{@code rm}的所有通过
     *         {@link Discriminator}引用的{@link ResultMap}对象id集合，然后遍历这个集合，对于每一个迭代的{@link ResultMap}对象id：通过{@link #resultMaps}.get(id)获取对应的{@link ResultMap}对象
     *         并判断该对象{@link ResultMap#hasNestedResultMaps()}是否为true，为true则调用{@code rm}的{@link ResultMap#forceNestedResultMaps()}并break；为false则什么也不做继续遍历直至遍历完成
     *     </li>
     * </ol>
     *
     * @param rm ResultMap 对象
     */
    // Slow but a one time cost. A better solution is welcome.
    protected void checkLocallyForDiscriminatedNestedResultMaps(ResultMap rm) {
        if (!rm.hasNestedResultMaps() && rm.getDiscriminator() != null) {
            for (Map.Entry<String, String> entry : rm.getDiscriminator().getDiscriminatorMap().entrySet()) {
                String discriminatedResultMapName = entry.getValue();
                if (hasResultMap(discriminatedResultMapName)) {
                    ResultMap discriminatedResultMap = resultMaps.get(discriminatedResultMapName);
                    if (discriminatedResultMap.hasNestedResultMaps()) {
                        rm.forceNestedResultMaps();
                        break;
                    }
                }
            }
        }
    }

    protected static class StrictMap<V> extends HashMap<String, V> {

        private static final long serialVersionUID = -4950446264854982944L;
        private final String name;

        public StrictMap(String name, int initialCapacity, float loadFactor) {
            super(initialCapacity, loadFactor);
            this.name = name;
        }

        public StrictMap(String name, int initialCapacity) {
            super(initialCapacity);
            this.name = name;
        }

        public StrictMap(String name) {
            super();
            this.name = name;
        }

        public StrictMap(String name, Map<String, ? extends V> m) {
            super(m);
            this.name = name;
        }

        /**
         * 传入完整的key以及value：
         * <ol>
         *     <li>
         *         如果完整的key已经存在（{@link HashMap#containsKey(Object)}），直接抛出异常，否则继续
         *     </li>
         *     <li>
         *         如果完整的key包含"."，调用{@link #getShortName(String)}获取简短key：
         *         <ul>
         *             <li>
         *                 如果该简短key没有对应的value，则将传入value设置给该简短key
         *             </li>
         *             <li>
         *                 如果该简短key已经存在value，则设置一个{@link Ambiguity}对象给它，表示冲突了
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         将value设置给该完整key
         *     </li>
         * </ol>
         *
         * @param key
         * @param value
         * @return
         */
        @SuppressWarnings("unchecked")
        public V put(String key, V value) {
            if (containsKey(key)) {
                throw new IllegalArgumentException(name + " already contains value for " + key);
            }
            if (key.contains(".")) {
                final String shortKey = getShortName(key);
                if (super.get(shortKey) == null) {
                    super.put(shortKey, value);
                } else {
                    super.put(shortKey, (V) new Ambiguity(shortKey));
                }
            }
            return super.put(key, value);
        }

        /**
         * 传入完整的key或者简短的key获取对应的value
         *
         * <ol>
         *     <li>
         *         调用super.get(key)从继承的hashmap容器中获取对应的value
         *     </li>
         *     <li>
         *         如果对应的value是null，抛出异常
         *     </li>
         *     <li>
         *         如果对应的value是{@link Ambiguity}，抛出异常
         *     </li>
         * </ol>
         *
         * @param key
         * @return
         */
        public V get(Object key) {
            V value = super.get(key);
            if (value == null) {
                throw new IllegalArgumentException(name + " does not contain value for " + key);
            }
            if (value instanceof Ambiguity) {
                throw new IllegalArgumentException(((Ambiguity) value).getSubject() + " is ambiguous in " + name
                        + " (try using the full name including the namespace, or rename one of the entries)");
            }
            return value;
        }

        /**
         * 如果传入的{@code key}包含"."，对其进行切割，取切割之后得到的数组的最后一个字符串并返回
         *
         * @param key
         * @return
         */
        private String getShortName(String key) {
            final String[] keyParts = key.split("\\.");
            return keyParts[keyParts.length - 1];
        }

        /**
         * 当命名空间的简称在map中存在冲突时，将当前对象设置为其value，以示冲突
         */
        protected static class Ambiguity {
            final private String subject;

            public Ambiguity(String subject) {
                this.subject = subject;
            }

            public String getSubject() {
                return subject;
            }
        }
    }

}
