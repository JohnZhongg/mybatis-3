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
package org.apache.ibatis.builder.xml;

import org.apache.ibatis.builder.*;
import org.apache.ibatis.cache.Cache;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;

import java.io.InputStream;
import java.io.Reader;
import java.util.*;

/**
 * Mapper XML 配置构建器，主要负责解析 Mapper 映射配置文件
 *
 * @author Clinton Begin
 */
public class XMLMapperBuilder extends BaseBuilder {

    /**
     * 基于 Java XPath 解析器
     */
    private final XPathParser parser;
    /**
     * Mapper 构造器助手
     */
    private final MapperBuilderAssistant builderAssistant;
    /**
     * 可被其他语句引用的可重用语句块的集合
     *
     * 例如：<sql id="userColumns"> ${alias}.id,${alias}.username,${alias}.password </sql>
     */
    private final Map<String, XNode> sqlFragments;
    /**
     * 资源的唯一标识（就是xml文件路径）（在{@link Configuration#loadedResources}中代表本XML资源的唯一标识是字符串"namespace:"拼接当前Mapper接口的全限定名，参考方法{@link #bindMapperForNamespace()}的步骤2.true.2.true.1）
     */
    private final String resource;

    @Deprecated
    public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
        this(reader, configuration, resource, sqlFragments);
        this.builderAssistant.setCurrentNamespace(namespace);
    }

    @Deprecated
    public XMLMapperBuilder(Reader reader, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
        this(new XPathParser(reader, true, configuration.getVariables(), new XMLMapperEntityResolver()),
                configuration, resource, sqlFragments);
    }

    public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments, String namespace) {
        this(inputStream, configuration, resource, sqlFragments);
        this.builderAssistant.setCurrentNamespace(namespace);
    }

    public XMLMapperBuilder(InputStream inputStream, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
        this(new XPathParser(inputStream, true, configuration.getVariables(), new XMLMapperEntityResolver()),
                configuration, resource, sqlFragments);
    }

    private XMLMapperBuilder(XPathParser parser, Configuration configuration, String resource, Map<String, XNode> sqlFragments) {
        super(configuration);
        // 创建 MapperBuilderAssistant 对象
        this.builderAssistant = new MapperBuilderAssistant(configuration, resource);
        this.parser = parser;
        this.sqlFragments = sqlFragments;
        this.resource = resource;
    }

    /**
     * <ol>
     *     <li>
     *          检查当前mapper xml是否已经加载过：通过{@link Configuration#isResourceLoaded(String)}方法查看{@link Configuration#loadedResources}中是否包含当前xml的唯一标识
     *          <ul>
     *              <li>
     *                  没有加载过：
     *                  <ol>
     *                      <li>
     *                          先调用{@link #parser}的{@link XPathParser#evalNode(String)}传入"/mapper"解析整个{@code <mapper/>}标签成为对应的{@link XNode}对象，然后调用{@link #configurationElement(XNode)}传入该{@link XNode}对象进行解析
     *                      </li>
     *                      <li>
     *                          调用{@link #configuration}的{@link Configuration#addLoadedResource(String)}传入{@link #resource}标记当前资源（当前mapper对应的xml文件）已经加载
     *                      </li>
     *                      <li>
     *                          调用{@link #bindMapperForNamespace()} 添加当前xml对应的mapper的{@link Class}对象到{@link Configuration}中，并解析其包含的一些mybatis的注解
     *                      </li>
     *                  </ol>
     *              </li>
     *              <li>
     *                  加载过了：什么也不做，继续往下走
     *              </li>
     *          </ul>
     *     </li>
     *     <li>
     *         调用{@link #parsePendingResultMaps()}解析待定的 {@code <resultMap />} 节点
     *     </li>
     *     <li>
     *         调用{@link #parsePendingResultMaps()}解析待定的 {@code <cache-ref />} 节点
     *     </li>
     *     <li>
     *         调用{@link #parsePendingResultMaps()}解析待定的 SQL 语句的节点
     *     </li>
     * </ol>
     */
    public void parse() {
        if (!configuration.isResourceLoaded(resource)) {
            configurationElement(parser.evalNode("/mapper"));
            configuration.addLoadedResource(resource);
            bindMapperForNamespace();
        }

        parsePendingResultMaps();
        parsePendingCacheRefs();
        parsePendingStatements();
    }

    public XNode getSqlFragment(String refid) {
        return sqlFragments.get(refid);
    }


    /**
     * 解析{@code <mapper/>}标签：
     * <ol>
     *     <li>
     *         {@link XNode#getStringAttribute(String)}获取"namespace"属性值，如果该属性值没有设置或者为空字符串，直接抛出异常{@link BuilderException}，否则继续
     *     </li>
     *     <li>
     *         将"namespace"属性值设置为{@link #builderAssistant}的{@link MapperBuilderAssistant#currentNamespace}属性
     *     </li>
     *     <li>
     *         <ul>
     *             <li>
     *                 调用当前{@code <mapper/>}标签的{@link XNode}对象的{@link XNode#evalNode(String)}分别传入"cache-ref"、"cache"作为相对路径解析当前{@link XNode}节点下的子节点{@link XNode}
     *             </li>
     *             <li>
     *                 调用当前{@code <mapper/>}标签的{@link XNode}对象的{@link XNode#evalNodes(String)}：
     *                 <ul>
     *                     <li>
     *                         分别传入"/mapper/parameterMap"、"/mapper/resultMap"、"/mapper/sql"作为绝对路径解析对应的子节点{@link XNode}节点列表
     *                     </li>
     *                     <li>
     *                         传入"select|insert|update|delete"作为相对路径指向的所有节点{@link XNode}节点列表
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         解析上述得到的所有{@link XNode}对象
     *         <ul>
     *             <li>
     *                 调用{@link #cacheRefElement(XNode)}解析得到得"cache-ref"对应的{@link XNode}节点
     *             </li>
     *             <li>
     *                 调用{@link #cacheElement(XNode)}解析得到得"cache"对应的{@link XNode}节点
     *             </li>
     *             <li>
     *                 调用{@link #parameterMapElement(List)}解析得到得"/mapper/parameterMap"对应的{@link XNode}节点列表
     *             </li>
     *             <li>
     *                 调用{@link #resultMapElements(List)}解析得到得"/mapper/resultMap"对应的{@link XNode}节点列表
     *             </li>
     *             <li>
     *                 调用{@link #sqlElement(List)}解析得到得"/mapper/sql"对应的{@link XNode}节点列表
     *             </li>
     *             <li>
     *                 调用{@link #buildStatementFromContext(List)}解析得到得"select|insert|update|delete"对应的{@link XNode}节点列表
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param context
     */
    private void configurationElement(XNode context) {
        try {
            String namespace = context.getStringAttribute("namespace");
            if (namespace == null || namespace.equals("")) {
                throw new BuilderException("Mapper's namespace cannot be empty");
            }
            builderAssistant.setCurrentNamespace(namespace);
            cacheRefElement(context.evalNode("cache-ref"));
            cacheElement(context.evalNode("cache"));
            parameterMapElement(context.evalNodes("/mapper/parameterMap"));
            resultMapElements(context.evalNodes("/mapper/resultMap"));
            sqlElement(context.evalNodes("/mapper/sql"));
            buildStatementFromContext(context.evalNodes("select|insert|update|delete"));
        } catch (Exception e) {
            throw new BuilderException("Error parsing Mapper XML. The XML location is '" + resource + "'. Cause: " + e, e);
        }
    }


    /**
     * 解析 {@code <select />} {@code <insert />} {@code <update />} {@code <delete />} 节点们：
     * <ul>
     *     通过成员变量{@link #configuration}的{@link Configuration#getDatabaseId()}检查{@link Configuration#databaseId}是否为null
     *     <li>
     *         是则：调用{@link #buildStatementFromContext(List, String)}传入{@code list}和{@link Configuration#databaseId}进行解析
     *     </li>
     *     <li>
     *         否则：调用{@link #buildStatementFromContext(List, String)}传入{@code list}和 null 进行解析
     *     </li>
     * </ul>
     *
     * @param list {@code <select />} {@code <insert />} {@code <update />} {@code <delete />} 节点对应的{@link XNode}对象们
     */
    private void buildStatementFromContext(List<XNode> list) {
        if (configuration.getDatabaseId() != null) {
            buildStatementFromContext(list, configuration.getDatabaseId());
        }
        buildStatementFromContext(list, null);
    }

    /**
     * 解析 {@code <select />} {@code <insert />} {@code <update />} {@code <delete />} 节点们：
     * 遍历{@code list}，对于每一个迭代的{@link XNode}对象：
     * <ol>
     *     <li>
     *         调用{@link XMLStatementBuilder#XMLStatementBuilder(Configuration, MapperBuilderAssistant, XNode, String)}传入{@link #configuration}、{@link #builderAssistant}、当前迭代的{@link XNode}对象、{@code requiredDatabaseId}四个参数实例化一个
     *         {@link XMLStatementBuilder}对象
     *     </li>
     *     <li>
     *         然后如下代码调用{@link XMLStatementBuilder#parseStatementNode()}尝试进行解析：
     *         <pre>
     *             try {
     *                 statementParser.parseStatementNode();
     *             } catch (IncompleteElementException e) {
     *                 configuration.addIncompleteStatement(statementParser);
     *             }
     *         </pre>
     *         如果解析过程中，抛出{@link IncompleteElementException}异常则调用成员变量{@link #configuration}的{@link Configuration#addIncompleteStatement(XMLStatementBuilder)}传入该{@link XMLStatementBuilder}对象进行记录
     *     </li>
     * </ol>
     *
     * @param list {@code <select />} {@code <insert />} {@code <update />} {@code <delete />} 节点对应的{@link XNode}对象们
     * @param requiredDatabaseId {@link Configuration#databaseId}
     */
    private void buildStatementFromContext(List<XNode> list, String requiredDatabaseId) {
        for (XNode context : list) {
            final XMLStatementBuilder statementParser = new XMLStatementBuilder(configuration, builderAssistant, context, requiredDatabaseId);
            try {
                statementParser.parseStatementNode();
            } catch (IncompleteElementException e) {
                configuration.addIncompleteStatement(statementParser);
            }
        }
    }

    /**
     * <ol>
     *     <li>
     *         调用成员变量{@link #configuration}的{@link Configuration#getIncompleteResultMaps()}获取未处理的{@link ResultMap}的{@link ResultMapResolver}对象集合
     *     </li>
     *     <li>
     *         用 synchronized 锁住该集合对象
     *     </li>
     *     <li>
     *         调用{@link Collection#iterator()}获取集合的迭代对象{@link Iterator}
     *     </li>
     *     <li>
     *         使用以上迭代器进行迭代({@link Iterator#hasNext()})：调用{@link Iterator#next()}获取下一个未解析的{@link ResultMapResolver}对象并调用{@link ResultMapResolver#resolve()}进行解析，然后调用{@link Iterator#remove()}移出当前{@link ResultMapResolver}，本过程
     *         使用try catch进行异常（{@link IncompleteElementException}）捕获，如果捕获到了异常，什么也不做，结束方法
     *     </li>
     * </ol>
     */
    private void parsePendingResultMaps() {
        Collection<ResultMapResolver> incompleteResultMaps = configuration.getIncompleteResultMaps();
        synchronized (incompleteResultMaps) {
            Iterator<ResultMapResolver> iter = incompleteResultMaps.iterator();
            while (iter.hasNext()) {
                try {
                    iter.next().resolve();
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // ResultMap is still missing a resource...
                }
            }
        }
    }
    /**
     * <ol>
     *     <li>
     *         调用成员变量{@link #configuration}的{@link Configuration#getIncompleteCacheRefs()}获取未处理的{@link Cache}的{@link CacheRefResolver}对象集合
     *     </li>
     *     <li>
     *         用 synchronized 锁住该集合对象
     *     </li>
     *     <li>
     *         调用{@link Collection#iterator()}获取集合的迭代对象{@link Iterator}
     *     </li>
     *     <li>
     *         使用以上迭代器进行迭代({@link Iterator#hasNext()})：调用{@link Iterator#next()}获取下一个未解析的{@link CacheRefResolver}对象并调用{@link CacheRefResolver#resolveCacheRef()}进行解析，然后调用{@link Iterator#remove()}移出当前{@link CacheRefResolver}，本过程
     *         使用try catch进行异常（{@link IncompleteElementException}）捕获，如果捕获到了异常，什么也不做，结束方法
     *     </li>
     * </ol>
     */
    private void parsePendingCacheRefs() {
        Collection<CacheRefResolver> incompleteCacheRefs = configuration.getIncompleteCacheRefs();
        synchronized (incompleteCacheRefs) {
            Iterator<CacheRefResolver> iter = incompleteCacheRefs.iterator();
            while (iter.hasNext()) {
                try {
                    iter.next().resolveCacheRef();
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // Cache ref is still missing a resource...
                }
            }
        }
    }
    /**
     * <ol>
     *     <li>
     *         调用成员变量{@link #configuration}的{@link Configuration#getIncompleteStatements()}获取未处理的{@link Cache}的{@link XMLStatementBuilder}对象集合
     *     </li>
     *     <li>
     *         用 synchronized 锁住该集合对象
     *     </li>
     *     <li>
     *         调用{@link Collection#iterator()}获取集合的迭代对象{@link Iterator}
     *     </li>
     *     <li>
     *         使用以上迭代器进行迭代({@link Iterator#hasNext()})：调用{@link Iterator#next()}获取下一个未解析的{@link XMLStatementBuilder}对象并调用{@link XMLStatementBuilder#parseStatementNode()}进行解析，然后调用{@link Iterator#remove()}移出当前{@link XMLStatementBuilder}，本过程
     *         使用try catch进行异常（{@link IncompleteElementException}）捕获，如果捕获到了异常，什么也不做，结束方法
     *     </li>
     * </ol>
     */
    private void parsePendingStatements() {
        Collection<XMLStatementBuilder> incompleteStatements = configuration.getIncompleteStatements();
        synchronized (incompleteStatements) {
            Iterator<XMLStatementBuilder> iter = incompleteStatements.iterator();
            while (iter.hasNext()) {
                try {
                    iter.next().parseStatementNode();
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // Statement is still missing a resource...
                }
            }
        }
    }


    /**
     * 解析{@code <cache-ref />}标签：
     * <ol>
     *     <li>
     *         建立以下映射关系，通过{@link Configuration#addCacheRef(String, String)}设置到{@link Configuration#cacheRefMap}以供便用:
     *         <ul>
     *             <li>
     *                 <b>KEY</b>：使用{@link #builderAssistant}的{@link MapperBuilderAssistant#getCurrentNamespace()}获取当前xml的namespace，
     *             </li>
     *             <li>
     *                 <b>VALUE</b>：获取当前{@code <cache-ref/>}标签的"namespace"属性作为cache的namespace
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         将当前{@link #builderAssistant}和{@code <cache-ref />}中定义的namespace传入{@link CacheRefResolver#CacheRefResolver(MapperBuilderAssistant, String)}构建一个{@link CacheRefResolver}对象，
     *         然后调用{@link CacheRefResolver#resolveCacheRef()}方法解析{@code <cache-ref />}中定义的namespace指向的{@link Cache}对象，该方法会尝试在{@link Configuration#caches}中寻找namespace对应的{@link Cache}
     *         对象，如果不存在会抛出异常{@link IncompleteElementException}：
     *         <ul>
     *             <li>
     *                 如果抛出了该异常，则进行异常处理，调用{@link Configuration#addIncompleteCacheRef(CacheRefResolver)}将该{@link CacheRefResolver}对象传入设置到{@link Configuration#incompleteCacheRefs}
     *             </li>
     *             <li>
     *                 如果没有异常，则什么也不做，方法结束，当前方法仅仅是为了监测未解析完成{@code <cache-ref />}标签，以便后续可以进行补充解析或者在用户尝试获取{@link Cache}之前可以判断一下是否该mapper的{@link Cache}尚未解析
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param context
     */
    private void cacheRefElement(XNode context) {
        if (context != null) {
            configuration.addCacheRef(builderAssistant.getCurrentNamespace(), context.getStringAttribute("namespace"));
            CacheRefResolver cacheRefResolver = new CacheRefResolver(builderAssistant, context.getStringAttribute("namespace"));
            try {
                cacheRefResolver.resolveCacheRef();
            } catch (IncompleteElementException e) {
                configuration.addIncompleteCacheRef(cacheRefResolver);
            }
        }
    }


    /**
     * 解析{@code <cache/>}标签
     * <ol>
     *     <li>
     *         获取{@code <cache/>}中的"type"属性，如果获取不到，则设置默认值为"PERPETUAL"，并调用成员变量{@link #typeAliasRegistry}的{@link org.apache.ibatis.type.TypeAliasRegistry#resolveAlias(String)}方法转化为{@link Class}对象
     *     </li>
     *     <li>
     *         获取{@code <cache/>}中的"eviction"属性，如果获取不到，则设置默认值为"LRU"，并调用成员变量{@link #typeAliasRegistry}的{@link org.apache.ibatis.type.TypeAliasRegistry#resolveAlias(String)}方法转化为{@link Class}对象
     *     </li>
     *     <li>
     *         获取{@code <cache/>}中的"flushInterval"、"size"属性
     *     </li>
     *     <li>
     *         获取{@code <cache/>}中的"readOnly"、"blocking"属性，如果不存在则设置值为false
     *     </li>
     *     <li>
     *         获取{@code <cache/>}中的所有{@code <property/>}子标签为一个{@link Properties}对象
     *     </li>
     *     <li>
     *         将以上所有变量作为参数传入到成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#useNewCache(Class, Class, Long, Integer, boolean, boolean, Properties)}方法中构建一个新的{@link Cache}对象并返回
     *     </li>
     * </ol>
     *
     * org.apache.ibatis.session.Configuration#Configuration()
     * @param context
     * @throws Exception
     */
    private void cacheElement(XNode context) throws Exception {
        if (context != null) {
            String type = context.getStringAttribute("type", "PERPETUAL");
            Class<? extends Cache> typeClass = typeAliasRegistry.resolveAlias(type);
            String eviction = context.getStringAttribute("eviction", "LRU");
            Class<? extends Cache> evictionClass = typeAliasRegistry.resolveAlias(eviction);
            Long flushInterval = context.getLongAttribute("flushInterval");
            Integer size = context.getIntAttribute("size");
            boolean readWrite = !context.getBooleanAttribute("readOnly", false);
            boolean blocking = context.getBooleanAttribute("blocking", false);
            Properties props = context.getChildrenAsProperties();
            builderAssistant.useNewCache(typeClass, evictionClass, flushInterval, size, readWrite, blocking, props);
        }
    }

    /**
     * FROM <a href='http://www.mybatis.org/mybatis-3/zh/sqlmap-xml.html'>《MyBatis 官方文档 —— Mapper XML 文件》</a>
     * 已废弃！老式风格的参数映射。内联参数是首选,这个元素可能在将来被移除，这里不会记录。<br>
     *
     * 传入所有的{@code <parameterMap/>}标签集合，遍历该集合：
     * <ol>
     *     <li>
     *         获取当前{@code <parameterMap/>}标签的"id"，"type"属性，调用{@link BaseBuilder#resolveClass(String)}解析"type"为一个{@link Class}对象
     *     </li>
     *     <li>
     *         获取当前{@code <parameterMap/>}标签下的所有{@code <paramter/>}标签集合，然后遍历该集合，获取{@code <paramter/>}标签所有属性，并调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#buildParameterMapping(Class, String, Class, JdbcType, String, ParameterMode, Class, Integer)}方法传入{@code <paramter/>}标签的所有属性将所有{@code <parameter/>}标签构建一个{@link ParameterMapping}对象
     *     </li>
     *     <li>
     *         利用以上的"id"、解析之后的"type"、和构建好的{@link ParameterMapping}集合调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#addParameterMap(String, Class, List)}方法构建一个{@link ParameterMap}对象并记录到{@link Configuration}中
     *     </li>
     * </ol>
     *
     *
     * @param list
     * @throws Exception
     */
    @Deprecated
    private void parameterMapElement(List<XNode> list) throws Exception {
        for (XNode parameterMapNode : list) {
            String id = parameterMapNode.getStringAttribute("id");
            String type = parameterMapNode.getStringAttribute("type");
            Class<?> parameterClass = resolveClass(type);
            List<XNode> parameterNodes = parameterMapNode.evalNodes("parameter");
            List<ParameterMapping> parameterMappings = new ArrayList<>();
            for (XNode parameterNode : parameterNodes) {
                String property = parameterNode.getStringAttribute("property");
                String javaType = parameterNode.getStringAttribute("javaType");
                String jdbcType = parameterNode.getStringAttribute("jdbcType");
                String resultMap = parameterNode.getStringAttribute("resultMap");
                String mode = parameterNode.getStringAttribute("mode");
                String typeHandler = parameterNode.getStringAttribute("typeHandler");
                Integer numericScale = parameterNode.getIntAttribute("numericScale");
                ParameterMode modeEnum = resolveParameterMode(mode);
                Class<?> javaTypeClass = resolveClass(javaType);
                JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
                @SuppressWarnings("unchecked")
                Class<? extends TypeHandler<?>> typeHandlerClass = (Class<? extends TypeHandler<?>>) resolveClass(typeHandler);
                ParameterMapping parameterMapping = builderAssistant.buildParameterMapping(parameterClass, property, javaTypeClass, jdbcTypeEnum, resultMap, modeEnum, typeHandlerClass, numericScale);
                parameterMappings.add(parameterMapping);
            }
            builderAssistant.addParameterMap(id, parameterClass, parameterMappings);
        }
    }


    /**
     * 解析当前mapper xml中的所有 {@code <resultMap />} ：遍历{@code list}调用{@link #resultMapElement(XNode)}进行解析
     * @param list  {@code <resultMap />}列表
     * @throws Exception
     */
    private void resultMapElements(List<XNode> list) throws Exception {
        for (XNode resultMapNode : list) {
            try {
                resultMapElement(resultMapNode);
            } catch (IncompleteElementException e) {
                // ignore, it will be retried
            }
        }
    }


    /**
     * 解析当个 {@code <resultMap />} 节点：调用{@link #resultMapElement(XNode, List)}传入<b><u>{@code resultMapNode}</u></b>和<b><u>{@link Collections#emptyList()}（收集该{@code <resultMap/>}下的所有映射{@link ResultMapping}）</u></b>
     *
     * @param resultMapNode {@code <resultMap />}节点
     * @return
     * @throws Exception
     */
    private ResultMap resultMapElement(XNode resultMapNode) throws Exception {
        return resultMapElement(resultMapNode, Collections.<ResultMapping>emptyList());
    }


    /**
     * 解析{@code <resultMap/>}、{@code <association/>}、{@code <collection/>}、{@code <case/>}（{@code <discriminator/>}下的子标签）标签：
     * <ol>
     *     以上四个标签功能分别为：
     *     <li>
     *         {@code <resultMap/>}：指定某个类（属性"type"指定）承载了某个查询的结果集映射关系
     *     </li>
     *     <li>
     *         {@code <association/>}：表示和外层的标签（可以为以上四个标签的任意一个）为一对一关系并指定某个类（属性"javaType"指定）承载了对象属性到数据库字段的映射关系
     *     </li>
     *     <li>
     *         {@code <collection/>}：表示和外层的标签（可以为以上四个标签的任意一个）为一对多关系并指定某个类（属性"ofType"指定）承载了对象属性到数据库字段的映射关系
     *     </li>
     *     <li>
     *         {@code <case/>}：指定哪个类（属性"resultType"指定）来承载外层标签（可以为以上四个标签的任意一个）所表示的映射关系（可以覆盖外层标签的定义）
     *     </li>
     *     可以看到上面四个标签都有一个不同的属性用来指定承载映射关系的类，参考test Package中的{@link org/apache/ibatis/builder/NestedBlogMapper.xml}
     * </ol>
     * 以下是方法逻辑：
     * <ol>
     *     <li>
     *         获取当前标签的"id"属性，如果没有则利用标签内容根据一定规则自动生成一个唯一的id返回
     *     </li>
     *     <li>
     *         调用{@link XNode#getStringAttribute(String, String)}分别尝试获取"type"、"ofType"、"resultType"、"javaType"属性并调用{@link BaseBuilder#resolveClass(String)}解析该属性为{@link Class}对象表示映射关系的承载类
     *     </li>
     *     <li>
     *         尝试获取{@code <resultMap/>}标签中的"extends"和"autoMapping"属性
     *     </li>
     *     <li>
     *         new 一个{@link ArrayList}对象，用来装载当前标签的{@link ResultMapping}对象，将{@code additionalResultMappings}添加到该新new的{@link ArrayList}中（其实只有当前标签是{@code <case/>}时才可能有值，其他三个都是empty）
     *     </li>
     *     <li>
     *         获取当前标签下的所有子标签并遍历：
     *         <ol>
     *             <li>
     *                 判断如果是{@code <constructor/>}标签，调用{@link #processConstructorElement(XNode, Class, List)}传入当前子标签、第二步得到的{@link Class}对象、第四步得到的{@link ArrayList}对象3个参数进行处理（给其{@link ResultMapping}对象打上{@link ResultFlag#CONSTRUCTOR}的标。（注意：在dtd文件中限制了只能由一个{@code <constructor/>}标签）
     *             </li>
     *             <li>
     *                 判断如果是{@code <discriminator/>}标签，调用{@link #processDiscriminatorElement(XNode, Class, List)}传入当前子标签、第二步得到的{@link Class}对象、第四步得到的{@link ArrayList}对象3个参数进行处理（构建一个{@link Discriminator}对象并返回准备设置给当前方法对应的{@link ResultMap}对象的{@link ResultMap#discriminator}，并且将其下{@code <case/>}标签包含的{@link ResultMapping}对象也添加到新new的{@link ArrayList}中。（注意：在dtd文件中限制了只能由一个{@code <discriminator/>}标签）
     *             </li>
     *             <li>
     *                 其他标签（{@code <id/>}、{@code <association/>}、{@code <collection/>}、{@code result}）都会调用{@link #buildResultMappingFromContext(XNode, Class, List)}传入当前子标签、第二步得到的{@link Class}对象、标签的{@link ResultFlag}集合构建一个{@link ResultMapping}对象并添加到第四步得到的{@link ArrayList}对象（如果是{@code <id/>}标签则在执行该动作之前打上{@link ResultFlag#ID}的标识）
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         调用{@link ResultMapResolver#ResultMapResolver(MapperBuilderAssistant, String, Class, String, Discriminator, List, Boolean)}传入{@link #builderAssistant}、第一步得到的id、第二步得到的{@link Class}、第三步可能获取到的extend的值、第五步可能获取到的{@link Discriminator}、第四步创建并在第五步完善的{@link ResultMapping}{@link List}、第三步可能获取到的autoMapping的值一共7个参数构建一个{@link ResultMapResolver}对象，然后：
     *         <pre>
     *         try {
     *             return resultMapResolver.resolve();
     *         } catch (IncompleteElementException e) {
     *             configuration.addIncompleteResultMap(resultMapResolver);
     *             throw e;
     *         }</pre>尝试调用{@link ResultMapResolver#resolve()}解析{@link ResultMap}对象，如果解析过程抛出了{@link IncompleteElementException}则调用成员变量{@link #configuration}的{@link Configuration#addIncompleteResultMap(ResultMapResolver)}传入构建的{@link ResultMapResolver}进行记录
     *     </li>
     * </ol>
     *
     *
     * @param resultMapNode {@code <resultMap/>}、{@code <association/>}、{@code <collection/>}、{@code <case/>}（{@code <discriminator/>}下的子标签）标签
     * @param additionalResultMappings 以{@code <resultMap/>}、{@code <association/>}、{@code <collection/>}或者{@code <case/>}为维度收集{@link ResultMapping}对象：
     *                                 <ol>
     *                                     <li>
     *                                         {@code <resultMap/>}标签下{@code <association/>}、{@code <collection/>}、{@code <constructor/>}下的{@code arg}和{code idArg}、{@code result}都会转换成一个{@link ResultMapping}对象存到一个{@link List}中，另外其下面的{@code <discriminator/>}标签下的所有{@code <case/>}标签对应的{@link ResultMap}对象下的所有{@link ResultMapping}对象也都会加到这个{@link List}中
     *                                     </li>
     *                                     <li>
     *                                         {@code <association/>}、{@code <collection/>}不会继承外层标签的{@link ResultMapping}对象，其下面的{@code <association/>}、{@code <collection/>}、{@code <constructor/>}下的{@code arg}和{code idArg}、{@code result}都会转换成一个{@link ResultMapping}对象存到一个{@link List}中，另外其下面的{@code <discriminator/>}标签下的所有{@code <case/>}标签对应的{@link ResultMap}对象下的所有{@link ResultMapping}对象也都会加到这个{@link List}中
     *                                     </li>
     *                                     <li>
     *                                         {@code <case/>}标签对应的{@link ResultMap}对象会继承外层标签的{@link ResultMapping}对象，而其下面的{@code <association/>}、{@code <collection/>}、{@code <constructor/>}下的{@code arg}和{code idArg}、{@code result}都会转换成一个{@link ResultMapping}对象存到该{@link ResultMap}对象的{@link List}中，另外其下面的{@code <discriminator/>}标签下的所有{@code <case/>}标签对应的{@link ResultMap}对象下的所有{@link ResultMapping}对象也都会加到这个{@link List}中
     *                                     </li>
     *                                 </ol>
     * @return
     * @throws Exception
     */
    private ResultMap resultMapElement(XNode resultMapNode, List<ResultMapping> additionalResultMappings) throws Exception {
        ErrorContext.instance().activity("processing " + resultMapNode.getValueBasedIdentifier());
        String id = resultMapNode.getStringAttribute("id", resultMapNode.getValueBasedIdentifier());
        String type = resultMapNode.getStringAttribute("type", resultMapNode.getStringAttribute(
                "ofType", resultMapNode.getStringAttribute(
                        "resultType", resultMapNode.getStringAttribute("javaType")
                )
        ));
        String extend = resultMapNode.getStringAttribute("extends");
        Boolean autoMapping = resultMapNode.getBooleanAttribute("autoMapping");
        Class<?> typeClass = resolveClass(type);
        Discriminator discriminator = null;
        List<ResultMapping> resultMappings = new ArrayList<>();
        resultMappings.addAll(additionalResultMappings);
        List<XNode> resultChildren = resultMapNode.getChildren();
        for (XNode resultChild : resultChildren) {
            if ("constructor".equals(resultChild.getName())) {
                processConstructorElement(resultChild, typeClass, resultMappings);
            } else if ("discriminator".equals(resultChild.getName())) {
                discriminator = processDiscriminatorElement(resultChild, typeClass, resultMappings);
            } else {
                List<ResultFlag> flags = new ArrayList<>();
                if ("id".equals(resultChild.getName())) {
                    flags.add(ResultFlag.ID);
                }
                resultMappings.add(buildResultMappingFromContext(resultChild, typeClass, flags));
            }
        }
        ResultMapResolver resultMapResolver = new ResultMapResolver(builderAssistant, id, typeClass, extend, discriminator, resultMappings, autoMapping);
        try {
            return resultMapResolver.resolve();
        } catch (IncompleteElementException e) {
            configuration.addIncompleteResultMap(resultMapResolver);
            throw e;
        }
    }

    /**
     *
     * 解析{@code <resultMap/>}标签下的{@code <constructor/>}标签（在dtd文件中限制了只能由一个{@code <constructor/>}标签），获取当前{@code <constructor/>}标签的所有子标签(直接指明了数据库字段到java构造器中入参列表某个参数的映射关系)并遍历执行以下操作：
     * <ol>
     *     <li>
     *         给每一个子标签添加一个{@link ResultFlag#CONSTRUCTOR}的tag，标明当前映射为该{@code resultMapType}中一个构造器的一个参数到数据库某个字段的一个映射
     *     </li>
     *     <li>
     *         检查每一个子标签名字是否为"idArg"，如果是则该添加一个{@link ResultFlag#ID}的tag，表明当前映射的数据库字段端是一个id字段
     *     </li>
     *     <li>
     *         将当前子标签{@link XNode}对象、{@code resultMapType}、子标签的tag集合作为三个参数传入到{@link #buildResultMappingFromContext(XNode, Class, List)}方法中构建一个{@link ResultMapping}对象返回，然后添加到{@code resultMappings}中
     *     </li>
     * </ol>
     *
     * @param resultChild {@code <constructor/>}标签
     * @param resultMapType 映射关系的承载类，即当前{@code <constructor/>}标签的外部标签中的"type"、"ofType"、"resultType"、"javaType"属性
     * @param resultMappings {@link ResultMapping}对象集合
     * @throws Exception
     */
    private void processConstructorElement(XNode resultChild, Class<?> resultMapType, List<ResultMapping> resultMappings) throws Exception {
        List<XNode> argChildren = resultChild.getChildren();
        for (XNode argChild : argChildren) {
            List<ResultFlag> flags = new ArrayList<>();
            flags.add(ResultFlag.CONSTRUCTOR);
            if ("idArg".equals(argChild.getName())) {
                flags.add(ResultFlag.ID);
            }
            resultMappings.add(buildResultMappingFromContext(argChild, resultMapType, flags));
        }
    }

    /**
     * 解析{@code <discriminator/>}标签（在dtd文件中限制了只能由一个{@code <discriminator/>}标签）（{@code <resultMap/>}、{@code <association/>}、{@code <collection/>}、{@code <case/>}标签下都可以有该标签）：
     * <ol>
     *     <li>
     *         获取"column"、"javaType"、"jdbcType"、"typeHandler"属性，调用{@link BaseBuilder#resolveClass(String)}解析"javaType"和"typeHandler"为{@link Class}对象，调用{@link BaseBuilder#resolveJdbcType(String)}解析"jdbcType"为{@link JdbcType}对象
     *     </li>
     *     <li>
     *         新建一个{@link HashMap}对象，然后获取当前{@code <discriminator/>标签}的所有子标签({@code <case/>})并遍历所有子标签执行以下操作：
     *         <ol>
     *             <li>
     *                 获取"value"属性
     *             </li>
     *             <li>
     *                 获取"resultMap"属性（指向一个{@link ResultMap#getId()}），如果获取不到则调用{@link #processNestedResultMappings(XNode, List)}传入当前{@code <case/>}标签和{@code resultMappings}解析为一个新的{@link ResultMap}对象并返回其id
     *             </li>
     *             <li>
     *                 将"value"属性作为key，"resultMap"属性作为value put到新建的{@link HashMap}对象
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#buildDiscriminator(Class, String, Class, JdbcType, Class, Map)}传入{@code resultMapType}、"column"属性值、解析之后的"javaType"、"jdbcType"、"typeHandler"属性、上面新建的{@link HashMap}对象构建一个{@link Discriminator}并返回
     *     </li>
     * </ol>
     *
     * @param context 当前{@code <discriminator/>}标签
     * @param resultMapType 当前{@code <discriminator/>}标签的外层标签的映射关系承载类，由"type"、"ofType"、"resultType"、"javaType"属性来指定
     * @param resultMappings 当前{@code <resultMap/>}标签的所有resultMapping集合
     * @return
     * @throws Exception
     */
    private Discriminator processDiscriminatorElement(XNode context, Class<?> resultMapType, List<ResultMapping> resultMappings) throws Exception {
        String column = context.getStringAttribute("column");
        String javaType = context.getStringAttribute("javaType");
        String jdbcType = context.getStringAttribute("jdbcType");
        String typeHandler = context.getStringAttribute("typeHandler");
        Class<?> javaTypeClass = resolveClass(javaType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        Map<String, String> discriminatorMap = new HashMap<>();
        for (XNode caseChild : context.getChildren()) {
            String value = caseChild.getStringAttribute("value");
            String resultMap = caseChild.getStringAttribute("resultMap", processNestedResultMappings(caseChild, resultMappings));
            discriminatorMap.put(value, resultMap);
        }
        return builderAssistant.buildDiscriminator(resultMapType, column, javaTypeClass, jdbcTypeEnum, typeHandlerClass, discriminatorMap);
    }

    /**
     * 解析传入的{@code <sql/>}节点标签{@link XNode}列表：
     * <ul>
     *     通过成员变量{@link #configuration}的{@link Configuration#getDatabaseId()}检查{@link Configuration#databaseId}是否为null
     *     <li>
     *         是则：调用{@link #sqlElement(List, String)}传入{@code list}和{@link Configuration#databaseId}进行解析
     *     </li>
     *     <li>
     *         否则：调用{@link #sqlElement(List, String)}传入{@code list}和 null 进行解析
     *     </li>
     * </ul>
     *
     * @param list 要解析的{@code <sql/>}节点标签{@link XNode}列表
     * @throws Exception
     */
    private void sqlElement(List<XNode> list) throws Exception {
        if (configuration.getDatabaseId() != null) {
            sqlElement(list, configuration.getDatabaseId());
        }
        sqlElement(list, null);
    }

    /**
     * <ol>
     *     遍历{@code list}，对于每一个迭代的{@link XNode}对象：
     *     <li>
     *         通过{@link XNode#getStringAttribute(String)}获取当前{@link XNode}的"databaseId"属性和"id"属性
     *     </li>
     *     <li>
     *         调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#applyCurrentNamespace(String, boolean)}传入前面获得的id属性值和false，校验该id是否为当前namespace下的绝对id
     *     </li>
     *     <li>
     *          调用{@link #databaseIdMatchesCurrent(String, String, String)}传入上面获取的{@code </sql>}标签的id、databaseId属性和{@link Configuration#databaseId}检查databaseId是否匹配：
     *          <ul>
     *              <li>
     *                  是则：将"id"属性值作为key，当前{@link XNode}对象作为value，put到{@link #sqlFragments}中
     *              </li>
     *              <li>
     *                  否则：什么也不做
     *              </li>
     *          </ul>
     *     </li>
     * </ol>
     *
     * @param list
     * @param requiredDatabaseId
     * @throws Exception
     */
    private void sqlElement(List<XNode> list, String requiredDatabaseId) throws Exception {
        for (XNode context : list) {
            String databaseId = context.getStringAttribute("databaseId");
            String id = context.getStringAttribute("id");
            id = builderAssistant.applyCurrentNamespace(id, false);
            if (databaseIdMatchesCurrent(id, databaseId, requiredDatabaseId)) {
                sqlFragments.put(id, context);
            }
        }
    }

    /**
     * 检查当前{@code </sql>}标签的databaseId是否和{@link Configuration#databaseId}一致：
     *
     * <ol>
     *     <li>
     *         判断{@code requiredDatabaseId}是否为null
     *         <ul>
     *             <li>
     *                 是则：返回{@code requiredDatabaseId}.equals({@code databaseId})
     *             </li>
     *             <li>
     *                 否则：
     *                 <ol>
     *                     <li>
     *                         如果{@code databaseId}不为null，则返回false；否则继续往下
     *                     </li>
     *                     <li>
     *                         如果{@link #sqlFragments}.containsKey({@code id})（已经存在这个id）：
     *                         <ul>
     *                             <li>
     *                                 则：通过{@link #sqlFragments}.get({@code id})获取这个已经存在的sqlFragment {@link XNode}对象，然后通过{@link XNode#getStringAttribute(String)}获取"databaseId"属性，看该属性是否为null，是则返回true，否则返回false
     *                             </li>
     *                             <li>
     *                                 否则：什么也不做，往下走
     *                             </li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                          返回true
     *                     </li>
     *                 </ol>
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param id 当前{@code </sql>}的id属性值
     * @param databaseId 当前{@code </sql>}的databaseId属性值
     * @param requiredDatabaseId 当前{@link Configuration#databaseId}
     * @return
     */
    private boolean databaseIdMatchesCurrent(String id, String databaseId, String requiredDatabaseId) {
        if (requiredDatabaseId != null) {
            return requiredDatabaseId.equals(databaseId);
        } else {
            if (databaseId != null) {
                return false;
            }
            // skip this fragment if there is a previous one with a not null databaseId
            if (this.sqlFragments.containsKey(id)) {
                XNode context = this.sqlFragments.get(id);
                return context.getStringAttribute("databaseId") == null;
            }
        }
        return true;
    }

    /**
     * 解析{@code <arg/>}、{@code <idArg/>}（前两个标签都是{@code <constructor/>}的子标签）、{@code <id/>}、{@code <association/>}、{@code <collection/>}、{@code <result/>}标签为一条映射关系({@link ResultMapping}对象)：
     * <ol>
     *     <li>
     *         根据{@code flags}是否包含{@link ResultFlag#CONSTRUCTOR}来判断当前映射是否为构造器参数映射：如果是则获取当前标签的"name"属性；否则获取当前标签的"property"属性 --> 作为映射关系中java对象字段一端
     *     </li>
     *     <li>
     *         尝试从当前标签获取"column"、"javaType"、"jdbcType"、"select"、"resultMap"、"notNullColumn"、"columnPrefix"、"typeHandler"、"resultSet"、"foreignColumn"、"fetchType"等属性，其中针对以下属性针对处理：
     *         <ol>
     *             <li>
     *                 "resultMap"：如果获取不到该属性则会调用{@link #processNestedResultMappings(XNode, List)}传入{@code context}和{@link Collections#emptyList()}尝试解析当前标签为一个{@link ResultMap}对象然后返回其{@link ResultMap#getId()}
     *             </li>
     *             <li>
     *                 "fetchType"：如果获取不到该属性则获取{@link Configuration#isLazyLoadingEnabled()}作为是否延迟加载配置
     *             </li>
     *             <li>
     *                 "javaType"、"jdbcType"、"typeHandler"：调用{@link BaseBuilder#resolveClass(String)}解析"javaType"、"typeHandler"为{@link Class}对象；调用{@link BaseBuilder#resolveJdbcType(String)}解析"jdbcType"为{@link JdbcType}对象
     *             </li>
     *             <li>
     *                 调用成员变量{@link #builderAssistant}的{@link MapperBuilderAssistant#buildResultMapping(Class, String, String, Class, JdbcType, String, String, String, String, Class, List, String, String, boolean)}传入以上参数构建一个{@link ResultMapping}对象并返回
     *             </li>
     *         </ol>
     *     </li>
     * </ol>
     *
     * @param context 当前需要解析的标签
     * @param resultType 映射关系的承载类（如果是前两个标签，则表示当前标签的外两层标签的"type"、"ofType"、"resultType"、"javaType"属性；如果是后四个标签，则表示当前标签的外层标签的"type"、"ofType"、"resultType"、"javaType"属性）
     * @param flags 当前标签的tag集合（{@link ResultFlag}）
     * @return
     * @throws Exception
     */
    private ResultMapping buildResultMappingFromContext(XNode context, Class<?> resultType, List<ResultFlag> flags) throws Exception {
        String property;
        if (flags.contains(ResultFlag.CONSTRUCTOR)) {
            property = context.getStringAttribute("name");
        } else {
            property = context.getStringAttribute("property");
        }
        String column = context.getStringAttribute("column");
        String javaType = context.getStringAttribute("javaType");
        String jdbcType = context.getStringAttribute("jdbcType");
        String nestedSelect = context.getStringAttribute("select");
        String nestedResultMap = context.getStringAttribute("resultMap",
                processNestedResultMappings(context, Collections.emptyList()));
        String notNullColumn = context.getStringAttribute("notNullColumn");
        String columnPrefix = context.getStringAttribute("columnPrefix");
        String typeHandler = context.getStringAttribute("typeHandler");
        String resultSet = context.getStringAttribute("resultSet");
        String foreignColumn = context.getStringAttribute("foreignColumn");
        boolean lazy = "lazy".equals(context.getStringAttribute("fetchType", configuration.isLazyLoadingEnabled() ? "lazy" : "eager"));
        Class<?> javaTypeClass = resolveClass(javaType);
        Class<? extends TypeHandler<?>> typeHandlerClass = resolveClass(typeHandler);
        JdbcType jdbcTypeEnum = resolveJdbcType(jdbcType);
        return builderAssistant.buildResultMapping(resultType, property, column, javaTypeClass, jdbcTypeEnum, nestedSelect, nestedResultMap, notNullColumn, columnPrefix, typeHandlerClass, flags, resultSet, foreignColumn, lazy);
    }


    /**
     * 处理内嵌的 ResultMappings ：
     * <ul>
     *     如果当前标签是{@code <association/>}、{@code <collection/>}或者{@code <case/>}标签：
     *     <li>
     *         当前标签的"select"属性为null，则调用{@link #resultMapElement(XNode, List)}（伪递归）构建当前标签为一个{@link ResultMap}对象然后调用{@link ResultMap#getId()}返回；否则返回null
     *     </li>
     *     <li>
     *         当前标签的"select"属性不为null，返回null
     *     </li>
     * </ul>
     *
     * @param context 可以进行内嵌的标签（拥有"resultMap"属性，即{@code <association/>}、{@code <collection/>}、{@code <case/>}）
     * @param resultMappings 用来装载属于当前内嵌的{@link ResultMap}对象({@code <association/>}、{@code <collection/>}或者{@code <case/>})的所有{@link ResultMapping}对象的集合
     * @return
     * @throws Exception
     */
    private String processNestedResultMappings(XNode context, List<ResultMapping> resultMappings) throws Exception {
        if ("association".equals(context.getName())
                || "collection".equals(context.getName())
                || "case".equals(context.getName())) {
            if (context.getStringAttribute("select") == null) {
                ResultMap resultMap = resultMapElement(context, resultMappings);
                return resultMap.getId();
            }
        }
        return null;
    }

    /**
     * 绑定 Mapper：
     * <ol>
     *     <li>
     *         调用{@link #builderAssistant}的{@link MapperBuilderAssistant#getCurrentNamespace()}获取当前mapper的"namespace"（mapper全限定名）
     *     </li>
     *     <li>
     *         判断：前一步获取的"namespace" != null
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         调用{@link Resources#classForName(String)}传入"namespace"获得对应的{@link Class}对象（try catch 捕获{@link ClassNotFoundException}异常，catch中什么也不做）。代码为：
     *                         <pre>
     *             try {
     *                 boundType = Resources.classForName(namespace);
     *             } catch (ClassNotFoundException e) {
     *                 //ignore, bound type is not required
     *             }
     *                         </pre>
     *                     </li>
     *                     <li>
     *                         判断：如果前面的步骤得到的{@link Class}对象 != null
     *                         <ul>
     *                             <li>
     *                                 判断：调用{@link #configuration}的{@link Configuration#hasMapper(Class)}传入前面获取到的{@link Class}对象返回的结果是false（{@link Configuration}中还没有加载该Mapper的{@link Class}对象）
     *                                 <ul>
     *                                     <li>
     *                                         true：
     *                                         <ol>
     *                                             <li>
     *                                                 调用{@link #configuration}的{@link Configuration#addLoadedResource(String)}传入 <u><b>字符串"namespace:"+前面获得的namespace变量</b></u>（标明当前mapper对应的class已经加载了）
     *                                             </li>
     *                                             <li>
     *                                                 调用{@link #configuration}的{@link Configuration#addMapper(Class)}传入前面获取的{@link Class}对象绑定mapper对应的{@link Class}对象到{@link Configuration}
     *                                             </li>
     *                                         </ol>
     *                                     </li>
     *                                     <li>
     *                                         false：什么也不做
     *                                     </li>
     *                                 </ul>
     *                             </li>
     *                         </ul>
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false：什么也不做，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     */
    private void bindMapperForNamespace() {
        String namespace = builderAssistant.getCurrentNamespace();
        if (namespace != null) {
            Class<?> boundType = null;
            try {
                boundType = Resources.classForName(namespace);
            } catch (ClassNotFoundException e) {
                //ignore, bound type is not required
            }
            if (boundType != null) {
                if (!configuration.hasMapper(boundType)) {
                    // Spring may not know the real resource name so we set a flag
                    // to prevent loading again this resource from the mapper interface
                    // look at MapperAnnotationBuilder#loadXmlResource
                    configuration.addLoadedResource("namespace:" + namespace);
                    configuration.addMapper(boundType);
                }
            }
        }
    }

}
