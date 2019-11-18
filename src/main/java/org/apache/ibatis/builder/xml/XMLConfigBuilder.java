package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.*;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

/**
 * Copyright 2009-2019 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


/**
 * XML 配置构建器，主要负责解析 mybatis-config.xml 配置文件
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

    /**
     * 是否已解析
     */
    private boolean parsed;
    /**
     * 基于 Java XPath 解析器
     */
    private final XPathParser parser;
    /**
     * 环境
     */
    private String environment;
    /**
     * ReflectorFactory 对象
     */
    private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

    public XMLConfigBuilder(Reader reader) {
        this(reader, null, null);
    }

    public XMLConfigBuilder(Reader reader, String environment) {
        this(reader, environment, null);
    }

    public XMLConfigBuilder(Reader reader, String environment, Properties props) {
        this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
    }

    public XMLConfigBuilder(InputStream inputStream) {
        this(inputStream, null, null);
    }

    public XMLConfigBuilder(InputStream inputStream, String environment) {
        this(inputStream, environment, null);
    }

    public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
        this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
    }

    private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
        // 创建 Configuration 对象
        super(new Configuration());
        ErrorContext.instance().resource("SQL Mapper Configuration");
        // 设置 Configuration 的 variables 属性
        this.configuration.setVariables(props);
        this.parsed = false;
        this.environment = environment;
        this.parser = parser;
    }

    /**
     * 解析 XML 成 Configuration 对象。
     *
     * @return Configuration 对象
     */
    public Configuration parse() {
        // 若已解析，抛出 BuilderException 异常
        if (parsed) {
            throw new BuilderException("Each XMLConfigBuilder can only be used once.");
        }
        // 标记已解析
        parsed = true;
        // 解析 XML configuration 节点
        parseConfiguration(parser.evalNode("/configuration"));
        return configuration;
    }

    /**
     * 解析 XML
     *
     * 具体 MyBatis 有哪些 XML 标签，参见 《XML 映射配置文件》http://www.mybatis.org/mybatis-3/zh/configuration.html
     *
     * @param root 根节点
     */
    private void parseConfiguration(XNode root) {
        try {
            //issue #117 read properties first
            propertiesElement(root.evalNode("properties"));
            Properties settings = settingsAsProperties(root.evalNode("settings"));
            loadCustomVfs(settings);
            typeAliasesElement(root.evalNode("typeAliases"));
            pluginElement(root.evalNode("plugins"));
            objectFactoryElement(root.evalNode("objectFactory"));
            objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
            reflectorFactoryElement(root.evalNode("reflectorFactory"));
            settingsElement(settings);
            // read it after objectFactory and objectWrapperFactory issue #631
            environmentsElement(root.evalNode("environments"));
            databaseIdProviderElement(root.evalNode("databaseIdProvider"));
            typeHandlerElement(root.evalNode("typeHandlers"));
            mapperElement(root.evalNode("mappers"));
        } catch (Exception e) {
            throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
        }
    }

    /**
     * 将 {@code <setting />} 标签解析为 Properties 对象，settings中定义的name必须是{@link Configuration}拥有setter的属性，否则将抛出异常{@link BuilderException}
     *
     * @param context 节点
     * @return Properties 对象
     */
    private Properties settingsAsProperties(XNode context) {
        if (context == null) {
            return new Properties();
        }
        Properties props = context.getChildrenAsProperties();
        // Check that all settings are known to the configuration class
        MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
        for (Object key : props.keySet()) {
            if (!metaConfig.hasSetter(String.valueOf(key))) {
                throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
            }
        }
        return props;
    }

    /**
     * 加载自定义 VFS 类。传入一个{@link Properties}对象，然后获取key为"vfsImpl"的value，使用","作为分隔符将字符串分割（如果定义了多个VFS实现）成一个全限定名字符串数组，循环数组对
     * 每个全限定名调用{@link Resources#classForName(String)}获得对应的{@link Class}对象，然后对其使用{@link Configuration#setVfsImpl(Class)}
     *
     * @param props Properties 对象
     * @throws ClassNotFoundException 当 VFS 类不存在时
     */
    private void loadCustomVfs(Properties props) throws ClassNotFoundException {
        String value = props.getProperty("vfsImpl");
        if (value != null) {
            String[] clazzes = value.split(",");
            for (String clazz : clazzes) {
                if (!clazz.isEmpty()) {
                    @SuppressWarnings("unchecked")
                    Class<? extends VFS> vfsImpl = (Class<? extends VFS>) Resources.classForName(clazz);
                    configuration.setVfsImpl(vfsImpl);
                }
            }
        }
    }

    /**
     * 解析{@code <typeAliases />}标签，mybatis xml dtd 文件约束该标签下面只能是{@code <package/>}和{@code <typeAlias/>}标签，本方法逻辑：
     * <ol>
     *     获取{@code <typeAliases/>}下的所有子标签({@code <package/>}和{@code <typeAlias/>})，然后遍历所有子标签：
     *     <li>
     *         判断是否是{@code <package/>}标签，如果是，获取其name属性作为包名传入{@link Configuration#typeAliasRegistry}的{@link org.apache.ibatis.type.TypeAliasRegistry#registerAliases(String)}方法进行别名注册
     *     </li>
     *     <li>
     *         如果不是{@code <package/>}标签，则认为是{@code <typeAlias/>}标签，获取alias属性和type属性，分别表示别名和全限定名，如果alias是null，调用{@link org.apache.ibatis.type.TypeAliasRegistry#registerAlias(Class)}，如果不是null，调用{@link org.apache.ibatis.type.TypeAliasRegistry#registerAlias(String, Class)}
     *     </li>
     * </ol>
     *
     * @param parent 节点
     */
    private void typeAliasesElement(XNode parent) {
        if (parent != null) {
            for (XNode child : parent.getChildren()) {
                if ("package".equals(child.getName())) {
                    String typeAliasPackage = child.getStringAttribute("name");
                    configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
                } else {
                    String alias = child.getStringAttribute("alias");
                    String type = child.getStringAttribute("type");
                    try {
                        Class<?> clazz = Resources.classForName(type);
                        if (alias == null) {
                            typeAliasRegistry.registerAlias(clazz);
                        } else {
                            typeAliasRegistry.registerAlias(alias, clazz);
                        }
                    } catch (ClassNotFoundException e) {
                        throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
                    }
                }
            }
        }
    }

    /**
     * 解析 {@code <plugins />} 标签，获取{@code <plugins />}下的所有子标签{@code <plugin/>}，遍历所有子标签，获取"interceptor"属性（该属性值可以是类别名或者全限定名）,调用
     * {@link BaseBuilder#resolveClass(String)}获得Class对象之后调用{@link Class#newInstance()}获取实例强转为{@link Interceptor}类型的对象，并使用{@link XNode#getChildrenAsProperties()}
     * 获取当前{@code <plugin/>}标签下的所有{@code <property/>标签}为{@link Properties}对象，通过{@link Interceptor#setProperties(Properties)}设置该拦截器对象，然后调用{@link Configuration#addInterceptor(Interceptor)}
     * 将所有new出来的实例添加到添加到 {@link Configuration#interceptorChain} 中
     *
     * @param parent 节点
     * @throws Exception 发生异常时
     */
    private void pluginElement(XNode parent) throws Exception {
        if (parent != null) {
            for (XNode child : parent.getChildren()) {
                String interceptor = child.getStringAttribute("interceptor");
                Properties properties = child.getChildrenAsProperties();
                //要显示地在调用泛型方法的时候给泛型赋值，需要使用xxx.<GenericType>Method()的方式，例如这里使用在方法前面加了this，不加的话语法不过：  Interceptor interceptor = this.<Interceptor>resolveClass(interceptor).newInstance();
                Interceptor interceptorInstance = (Interceptor)resolveClass(interceptor).newInstance();
                interceptorInstance.setProperties(properties);
                configuration.addInterceptor(interceptorInstance);
            }
        }
    }

    /**
     * 解析{@code <objectFactory/>}标签获取对象工厂，获取其type属性，调用{@link BaseBuilder#resolveClass(String)}获得Class对象并调用{@link Class#newInstance()}获得对象，然后
     * 将本标签下的所有{@code <property/>}标签的值通过{@link ObjectFactory#setProperties(Properties)}进行设置，并将该{@link ObjectFactory}对象设置到{@link Configuration#objectFactory}
     *
     * @param context
     * @throws Exception
     */
    private void objectFactoryElement(XNode context) throws Exception {
        if (context != null) {
            String type = context.getStringAttribute("type");
            Properties properties = context.getChildrenAsProperties();
            ObjectFactory factory = (ObjectFactory) resolveClass(type).newInstance();
            factory.setProperties(properties);
            configuration.setObjectFactory(factory);
        }
    }


    /**
     * 解析{@code <objectWrapperFactory/>}标签获取对象包装器工厂，获取其type属性，调用{@link BaseBuilder#resolveClass(String)}获得Class对象并调用{@link Class#newInstance()}获得对象，然后
     * 将该{@link ObjectWrapperFactory}对象设置到{@link Configuration#objectWrapperFactory}
     *
     * @param context
     * @throws Exception
     */
    private void objectWrapperFactoryElement(XNode context) throws Exception {
        if (context != null) {
            String type = context.getStringAttribute("type");
            ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).newInstance();
            configuration.setObjectWrapperFactory(factory);
        }
    }

    /**
     * 解析{@code <reflectorFactory/>}标签反射器工厂，获取其type属性，调用{@link BaseBuilder#resolveClass(String)}获得Class对象并调用{@link Class#newInstance()}获得对象，然后
     * 将该{@link ReflectorFactory}对象设置到{@link Configuration#reflectorFactory}
     *
     * @param context
     * @throws Exception
     */
    private void reflectorFactoryElement(XNode context) throws Exception {
        if (context != null) {
            String type = context.getStringAttribute("type");
            ReflectorFactory factory = (ReflectorFactory) resolveClass(type).newInstance();
            configuration.setReflectorFactory(factory);
        }
    }

    /**
     * 1. 解析 <properties /> 标签，成 Properties 对象。
     * 2. 覆盖 configuration 中的 Properties 对象到上面的结果。
     * 3. 设置结果到 parser 和 configuration 中
     *
     * @param context 节点
     * @throws Exception 解析发生异常
     */
    private void propertiesElement(XNode context) throws Exception {
        if (context != null) {
            // 读取子标签们，为 Properties 对象
            Properties defaults = context.getChildrenAsProperties();
            // 读取 resource 和 url 属性
            String resource = context.getStringAttribute("resource");
            String url = context.getStringAttribute("url");
            if (resource != null && url != null) { // resource 和 url 都存在的情况下，抛出 BuilderException 异常
                throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
            }
            // 读取本地 Properties 配置文件到 defaults 中。
            if (resource != null) {
                defaults.putAll(Resources.getResourceAsProperties(resource));
                // 读取远程 Properties 配置文件到 defaults 中。
            } else if (url != null) {
                defaults.putAll(Resources.getUrlAsProperties(url));
            }
            // 添加 configuration 中的 Properties 对象（在执行XMLConfigBuilder的时候构造函数可以接收一个properties进行参数传入）到 defaults 中。
            Properties vars = configuration.getVariables();
            if (vars != null) {
                defaults.putAll(vars);
            }
            // 设置 defaults 到 parser 和 configuration 中。
            parser.setVariables(defaults);
            configuration.setVariables(defaults);
        }
    }

    /**
     * 设置{@code <settings/>}标签到{@link Configuration}中的属性
     *
     * @param props
     * @throws Exception
     */
    private void settingsElement(Properties props) throws Exception {
        configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
        configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
        configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
        configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
        configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
        configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
        configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
        configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
        configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
        configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
        configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
        configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
        configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
        configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
        configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
        configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
        configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
        configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
        configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
        @SuppressWarnings("unchecked")
        Class<? extends TypeHandler> typeHandler = resolveClass(props.getProperty("defaultEnumTypeHandler"));
        configuration.setDefaultEnumTypeHandler(typeHandler);
        configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
        configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
        configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
        configuration.setLogPrefix(props.getProperty("logPrefix"));
        @SuppressWarnings("unchecked")
        Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
        configuration.setLogImpl(logImpl);
        configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
    }

    /**
     * 解析{@code <environments/>}标签，如果当前{@link XMLConfigBuilder#environment}为null（在调用本类构造函数的时候没有指定），则获取该标签的"default"属性设置为该值，
     * 然后获取该标签的所有{@code <environment/>}子标签并遍历，调用{@link XMLConfigBuilder#isSpecifiedEnvironment(String)}判断当前{@code <enviroment/>}标签的
     * id属性是否和{@link XMLConfigBuilder#environment}相等，如果是则解析该{@code <environment/>}标签（{@link XMLConfigBuilder#transactionManagerElement(XNode)}
     * 解析{@code <transactionManager/>}子标签获得{@link TransactionFactory}对象，{@link XMLConfigBuilder#dataSourceElement(XNode)}解析{@code <dataSource/>}
     * 子标签获得{@link DataSource}对象，并使用{@link Environment.Builder#build()}构建{@link Environment}对象，并设置到{@link Configuration#environment}）
     *
     * @param context
     * @throws Exception
     */
    private void environmentsElement(XNode context) throws Exception {
        if (context != null) {
            if (environment == null) {
                environment = context.getStringAttribute("default");
            }
            for (XNode child : context.getChildren()) {
                String id = child.getStringAttribute("id");
                if (isSpecifiedEnvironment(id)) {
                    TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
                    DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
                    DataSource dataSource = dsFactory.getDataSource();
                    Environment.Builder environmentBuilder = new Environment.Builder(id)
                            .transactionFactory(txFactory)
                            .dataSource(dataSource);
                    configuration.setEnvironment(environmentBuilder.build());
                }
            }
        }
    }

    /**
     * 解析{@code <databaseIdProvider/>}子标签，同样获取标签type属性作为别名或者全限定名通过{@link BaseBuilder#resolveClass(String)}获取Class对象之后通过{@link Class#newInstance()}
     * 获得数据库唯一标志生成类，然后通过{@link XNode#getChildrenAsProperties()}获取{@code <dataSource/>}的所有子标签{@code <property/>}的{@link Properties}对象通过{@link DatabaseIdProvider#setProperties(Properties)}
     * 方法进行属性设置，然后调用{@link Configuration#getEnvironment()}获得{@link Environment}对象之后通过{@link DatabaseIdProvider#getDatabaseId(DataSource)}（传入{@link Environment#getDataSource()}获得databaseId设置到{@link Configuration#databaseId}，参考mybatis默认{@link DatabaseIdProvider}实现类{@link org.apache.ibatis.mapping.VendorDatabaseIdProvider}）
     *
     * @param context
     * @throws Exception
     */
    private void databaseIdProviderElement(XNode context) throws Exception {
        DatabaseIdProvider databaseIdProvider = null;
        if (context != null) {
            String type = context.getStringAttribute("type");
            // awful patch to keep backward compatibility
            if ("VENDOR".equals(type)) {
                type = "DB_VENDOR";
            }
            Properties properties = context.getChildrenAsProperties();
            databaseIdProvider = (DatabaseIdProvider) resolveClass(type).newInstance();
            databaseIdProvider.setProperties(properties);
        }
        Environment environment = configuration.getEnvironment();
        if (environment != null && databaseIdProvider != null) {
            String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
            configuration.setDatabaseId(databaseId);
        }
    }

    /**
     * 解析{@code <transactionManager/>}子标签，同样获取标签type属性作为别名或者全限定名通过{@link BaseBuilder#resolveClass(String)}获取Class对象之后通过{@link Class#newInstance()}
     * 获得事务工厂类，然后通过{@link XNode#getChildrenAsProperties()}获取{@code <dataSource/>}的所有子标签{@code <property/>}的{@link Properties}对象通过{@link TransactionFactory#setProperties(Properties)}
     * 方法进行属性设置
     *
     * @param context
     * @return
     * @throws Exception
     */
    private TransactionFactory transactionManagerElement(XNode context) throws Exception {
        if (context != null) {
            String type = context.getStringAttribute("type");
            Properties props = context.getChildrenAsProperties();
            TransactionFactory factory = (TransactionFactory) resolveClass(type).newInstance();
            factory.setProperties(props);
            return factory;
        }
        throw new BuilderException("Environment declaration requires a TransactionFactory.");
    }

    /**
     * 解析{@code <dataSource/>}子标签，同样获取标签type属性作为别名或者全限定名通过{@link BaseBuilder#resolveClass(String)}获取Class对象之后通过{@link Class#newInstance()}
     * 获得数据源工厂类，然后通过{@link XNode#getChildrenAsProperties()}获取{@code <dataSource/>}的所有子标签{@code <property/>}的{@link Properties}对象通过{@link DataSourceFactory#setProperties(Properties)}
     * 方法进行属性设置
     *
     * @param context
     * @return
     * @throws Exception
     */
    private DataSourceFactory dataSourceElement(XNode context) throws Exception {
        if (context != null) {
            String type = context.getStringAttribute("type");
            Properties props = context.getChildrenAsProperties();
            DataSourceFactory factory = (DataSourceFactory) resolveClass(type).newInstance();
            factory.setProperties(props);
            return factory;
        }
        throw new BuilderException("Environment declaration requires a DataSourceFactory.");
    }

    /**
     * 解析{@code <typeHandlers />}标签，mybatis xml dtd 文件约束该标签下面只能是{@code <package/>}和{@code <typeHandler/>}标签，本方法逻辑：
     * <ol>
     *     获取{@code <typeHandlers/>}下的所有子标签({@code <package/>}和{@code <typeHandler/>})，然后遍历所有子标签：
     *     <li>
     *         判断是否是{@code <package/>}标签，如果是，获取其name属性作为包名传入{@link Configuration#typeHandlerRegistry}的{@link org.apache.ibatis.type.TypeHandlerRegistry#register(String)} 方法进行{@link TypeHandler}注册
     *     </li>
     *     <li>
     *         如果不是{@code <package/>}标签，则认为是{@code <typeHandler/>}标签，获取javaType属性和jdbcType属性和handler属性，根据各值是否为空的情况分别调用{@link org.apache.ibatis.type.TypeHandlerRegistry#register(Class, Class)}、{@link org.apache.ibatis.type.TypeHandlerRegistry#register(Class, JdbcType, Class)}、{@link org.apache.ibatis.type.TypeHandlerRegistry#register(Class)}
     *     </li>
     * </ol>
     *
     * @param parent
     * @throws Exception
     */
    private void typeHandlerElement(XNode parent) throws Exception {
        if (parent != null) {
            for (XNode child : parent.getChildren()) {
                if ("package".equals(child.getName())) {
                    String typeHandlerPackage = child.getStringAttribute("name");
                    typeHandlerRegistry.register(typeHandlerPackage);
                } else {
                    String javaTypeName = child.getStringAttribute("javaType");
                    String jdbcTypeName = child.getStringAttribute("jdbcType");
                    String handlerTypeName = child.getStringAttribute("handler");
                    Class<?> javaTypeClass = resolveClass(javaTypeName);
                    JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
                    Class<?> typeHandlerClass = resolveClass(handlerTypeName);
                    if (javaTypeClass != null) {
                        if (jdbcType == null) {
                            typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
                        } else {
                            typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
                        }
                    } else {
                        typeHandlerRegistry.register(typeHandlerClass);
                    }
                }
            }
        }
    }

    /**
     * 解析 {@code <mappers />} 标签
     *
     * @param parent
     * @throws Exception
     */
    private void mapperElement(XNode parent) throws Exception {
        if (parent != null) {
            for (XNode child : parent.getChildren()) {
                if ("package".equals(child.getName())) {
                    String mapperPackage = child.getStringAttribute("name");
                    configuration.addMappers(mapperPackage);
                } else {
                    String resource = child.getStringAttribute("resource");
                    String url = child.getStringAttribute("url");
                    String mapperClass = child.getStringAttribute("class");
                    if (resource != null && url == null && mapperClass == null) {
                        ErrorContext.instance().resource(resource);
                        InputStream inputStream = Resources.getResourceAsStream(resource);
                        XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
                        mapperParser.parse();
                    } else if (resource == null && url != null && mapperClass == null) {
                        ErrorContext.instance().resource(url);
                        InputStream inputStream = Resources.getUrlAsStream(url);
                        XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
                        mapperParser.parse();
                    } else if (resource == null && url == null && mapperClass != null) {
                        Class<?> mapperInterface = Resources.classForName(mapperClass);
                        configuration.addMapper(mapperInterface);
                    } else {
                        throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
                    }
                }
            }
        }
    }

    /**
     * {@code id} equals {@link XMLConfigBuilder#configuration} ?
     *
     * @param id 环境编号
     * @return 是否匹配
     */
    private boolean isSpecifiedEnvironment(String id) {
        if (environment == null) {
            throw new BuilderException("No environment specified.");
        } else if (id == null) {
            throw new BuilderException("Environment requires an id attribute.");
        } else if (environment.equals(id)) {
            return true;
        }
        return false;
    }

}
