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
package org.apache.ibatis.builder.annotation;

import org.apache.ibatis.annotations.*;
import org.apache.ibatis.annotations.Options.FlushCachePolicy;
import org.apache.ibatis.annotations.ResultMap;
import org.apache.ibatis.binding.BindingException;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.builder.IncompleteElementException;
import org.apache.ibatis.builder.MapperBuilderAssistant;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.keygen.Jdbc3KeyGenerator;
import org.apache.ibatis.executor.keygen.KeyGenerator;
import org.apache.ibatis.executor.keygen.NoKeyGenerator;
import org.apache.ibatis.executor.keygen.SelectKeyGenerator;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.mapping.*;
import org.apache.ibatis.parsing.PropertyParser;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.reflection.TypeParameterResolver;
import org.apache.ibatis.scripting.LanguageDriver;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.TypeHandler;
import org.apache.ibatis.type.UnknownTypeHandler;

import java.io.IOException;
import java.io.InputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;
import java.util.*;

/**
 * Mapper 注解构造器，负责解析 Mapper 接口上的注解
 *
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class MapperAnnotationBuilder {

    /**
     * SQL 操作注解集合（{@link Select}.class、{@link Insert}.class、{@link Update}.class、{@link Delete}.class）
     */
    private static final Set<Class<? extends Annotation>> SQL_ANNOTATION_TYPES = new HashSet<>();

    /**
     * SQL 操作提供者注解集合（{@link SelectProvider}.class、{@link InsertProvider}.class、{@link UpdateProvider}.class、{@link DeleteProvider}.class）
     */
    private static final Set<Class<? extends Annotation>> SQL_PROVIDER_ANNOTATION_TYPES = new HashSet<>();

    /**
     * 全局{@link Configuration}对象
     */
    private final Configuration configuration;

    /**
     * 当前mapper的builder助手
     */
    private final MapperBuilderAssistant assistant;

    /**
     * Mapper 接口类
     */
    private final Class<?> type;

    static {
        SQL_ANNOTATION_TYPES.add(Select.class);
        SQL_ANNOTATION_TYPES.add(Insert.class);
        SQL_ANNOTATION_TYPES.add(Update.class);
        SQL_ANNOTATION_TYPES.add(Delete.class);

        SQL_PROVIDER_ANNOTATION_TYPES.add(SelectProvider.class);
        SQL_PROVIDER_ANNOTATION_TYPES.add(InsertProvider.class);
        SQL_PROVIDER_ANNOTATION_TYPES.add(UpdateProvider.class);
        SQL_PROVIDER_ANNOTATION_TYPES.add(DeleteProvider.class);
    }

    /**
     * <ol>
     *     <li>
     *         调用{@code type}的{@link Class#getName()}然后调用结果的{@link String#replace(char, char)}传入'.'和'/'替换全限定名中的"."为"/"然后再拼接上后缀".java (best guess)"，作为本Mapper Java文件的资源名称
     *     </li>
     *     <li>
     *         调用构造器{@link MapperBuilderAssistant#MapperBuilderAssistant(Configuration, String)}传入{@code configuration}和前一步得到的本Mapper Java文件的资源名称构造一个{@link MapperBuilderAssistant}对象然后赋值到{@link #assistant}
     *     </li>
     *     <li>
     *         {@code configuration}赋值到{@link #configuration}
     *     </li>
     *     <li>
     *         {@code type}赋值到{@link #type}
     *     </li>
     * </ol>
     *
     * @param configuration Mybatis {@link Configuration} 对象
     * @param type Mapper接口的{@link Class}对象
     */
    public MapperAnnotationBuilder(Configuration configuration, Class<?> type) {
        String resource = type.getName().replace('.', '/') + ".java (best guess)";
        this.assistant = new MapperBuilderAssistant(configuration, resource);
        this.configuration = configuration;
        this.type = type;
    }

    /**
     * 解析注解
     * <ol>
     *     <li>
     *         调用{@link #type}.toString()获取mapper接口的全限定名字符串
     *     </li>
     *     <li>
     *         调用{@link #configuration}的{@link Configuration#isResourceLoaded(String)}传入步骤一得到的mapper接口全限定名字符串检查当前mapper注解已经解析过了
     *         <ul>
     *             <li>
     *                 还没解析：
     *                 <ol>
     *                     <li>
     *                         调用{@link #loadXmlResource()}进行尝试本mapper的xml解析
     *                     </li>
     *                     <li>
     *                         调用{@link #configuration}的{@link Configuration#addLoadedResource(String)}传入步骤一得到的mapper接口全限定名字符串标注当前mapper对应的接口类（包含注解）已经解析过了
     *                     </li>
     *                     <li>
     *                         调用{@link #assistant}的{@link MapperBuilderAssistant#setCurrentNamespace(String)}传入{@link #type}的{@link Class#getName()}设置当前mapper的namespace
     *                     </li>
     *                     <li>
     *                         调用{@link #parseCache()}解析mapper接口上的{@link CacheNamespace}注解
     *                     </li>
     *                     <li>
     *                         调用{@link #parseCacheRef()}解析mapper接口上的{@link CacheNamespaceRef}注解
     *                     </li>
     *                     <li>
     *                         调用{@link #type}的{@link Class#getMethods()}获取mapper接口的所有方法数组，然后遍历迭代该数组，对于每一个方法对象{@link Method}：如果该方法不是桥方法（!{@link Method#isBridge()}）就调用{@link #parseStatement(Method)}传入该方法对象进行方法上的注解解析；否则什么也不做。（该过程进行try catch 捕获{@link IncompleteElementException}异常，如果捕获到了就调用{@link #configuration}的{@link Configuration#addIncompleteMethod(MethodResolver)}传入通过{@link MethodResolver#MethodResolver(MapperAnnotationBuilder, Method)}构造的当前方法的{@link MethodResolver}对象）
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 解析过了：什么也不做，继续往下走
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link #parsePendingMethods()}对完成解析的方法
     *     </li>
     * </ol>
     */
    public void parse() {
        String resource = type.toString();
        if (!configuration.isResourceLoaded(resource)) {
            loadXmlResource();
            configuration.addLoadedResource(resource);
            assistant.setCurrentNamespace(type.getName());
            parseCache();
            parseCacheRef();
            Method[] methods = type.getMethods();
            for (Method method : methods) {
                try {
                    // issue #237
                    if (!method.isBridge()) {
                        parseStatement(method);
                    }
                } catch (IncompleteElementException e) {
                    configuration.addIncompleteMethod(new MethodResolver(this, method));
                }
            }
        }
        parsePendingMethods();
    }

    /**
     * 调用{@link #configuration}的{@link Configuration#getIncompleteMethods()}获取为解析完的方法集合，然后用 synchronized 关键字以该集合对象为锁对象锁住以下代码块：<br>
     * <ol>
     *     <li>
     *         调用该集合的{@link Collection#iterator()}获取到该集合的迭代器{@link Iterator}对象
     *     </li>
     *     <li>
     *         然后通过{@link Iterator#hasNext()}和{@link Iterator#next()}进行遍历，遍历中调用迭代的每个对象{@link MethodResolver#resolve()}方法进行解析，然后调用{@link Iterator#remove()}移除当前元素，方法片段：
     *         <pre>
     *             while (iter.hasNext()) {
     *                 try {
     *                     iter.next().resolve();
     *                     iter.remove();
     *                 } catch (IncompleteElementException e) {
     *                     // This method is still missing a resource
     *                 }
     *             }
     *         </pre>
     *     </li>
     * </ol>
     */
    private void parsePendingMethods() {
        Collection<MethodResolver> incompleteMethods = configuration.getIncompleteMethods();
        synchronized (incompleteMethods) {
            Iterator<MethodResolver> iter = incompleteMethods.iterator();
            while (iter.hasNext()) {
                try {
                    iter.next().resolve();
                    iter.remove();
                } catch (IncompleteElementException e) {
                    // This method is still missing a resource
                }
            }
        }
    }

    /**
     * 如果当前mapper对应的xml还没有加载过就进行加载："namespace:" + {@link #type}的{@link Class#getName()}作为当前xml在{@link Configuration#loadedResources}中的唯一标识
     * <ul>
     *     检查当前mapper xml是否已经加载过：通过{@link Configuration#isResourceLoaded(String)}方法查看{@link Configuration#loadedResources}中是否包含当前xml的唯一标识
     *     <li>
     *         如果已经加载过，什么也不做，直接返回
     *     </li>
     *     <li>
     *         将{@link #type}的全限定名中的"."替换为"/"并加上".xml"后缀作为相对类路径资源名称并尝试在所有的classpath上寻找该资源(通过{@link #type}.getResourceAsStream()和{@link Resources#getResourceAsStream(ClassLoader, String)})：
     *         <ul>
     *             <li>
     *                 如果找到资源，则分别将xml资源对应的{@link InputStream}、{@link Configuration}对象、xml文件相对类路径、{@link Configuration#getSqlFragments()}、mapper的全限定名共5个参数传入到{@link XMLMapperBuilder#XMLMapperBuilder(InputStream, Configuration, String, Map, String)}构建{@link XMLMapperBuilder}对象，
     *                 然后调用{@link XMLMapperBuilder#parse()}方法进行解析
     *             </li>
     *             <li>
     *                 如果找不到资源，什么也不做，直接返回
     *             </li>
     *         </ul>
     *     </li>
     * </ul>
     *
     */
    private void loadXmlResource() {
        // Spring may not know the real resource name so we check a flag
        // to prevent loading again a resource twice
        // this flag is set at XMLMapperBuilder#bindMapperForNamespace
        if (!configuration.isResourceLoaded("namespace:" + type.getName())) {
            String xmlResource = type.getName().replace('.', '/') + ".xml";
            // #1347
            InputStream inputStream = type.getResourceAsStream("/" + xmlResource);
            if (inputStream == null) {
                // Search XML mapper that is not in the module but in the classpath.
                try {
                    inputStream = Resources.getResourceAsStream(type.getClassLoader(), xmlResource);
                } catch (IOException e2) {
                    // ignore, resource is not required
                }
            }
            if (inputStream != null) {
                XMLMapperBuilder xmlParser = new XMLMapperBuilder(inputStream, assistant.getConfiguration(), xmlResource, configuration.getSqlFragments(), type.getName());
                xmlParser.parse();
            }
        }
    }

    /**
     * 解析 {@link CacheNamespace} 注解，创建{@link org.apache.ibatis.cache.Cache}主键给当前mapper：
     * <ol>
     *     <li>
     *         调用{@link #type}的{@link Class#getAnnotation(Class)}传入{@link CacheNamespace}.class获取mapper接口上定义的{@link CacheNamespace}注解对象
     *     </li>
     *     <li>
     *         判断：前一步获取的注解对象 != null
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         获取{@link CacheNamespace#size()}如果为0则设置为null、获取{@link CacheNamespace#flushInterval()} 如果为0则设置为null、获取{@link CacheNamespace#properties()}得到一个{@link Property}数组传入到{@link #convertToProperties(Property[])}转化成一个{@link Properties}对象
     *                     </li>
     *                     <li>
     *                         调用{@link #assistant}的{@link MapperBuilderAssistant#useNewCache(Class, Class, Long, Integer, boolean, boolean, Properties)}传入{@link CacheNamespace#implementation()}、{@link CacheNamespace#eviction()}、前一步处理后的{@link CacheNamespace#flushInterval()}、前一步处理后的{@link CacheNamespace#size()}、{@link CacheNamespace#readWrite()}、{@link CacheNamespace#blocking()} 、前一步处理后的{@link CacheNamespace#properties()} 进行{@link org.apache.ibatis.cache.Cache}对象构建
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false：什么也不做，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     */
    private void parseCache() {
        CacheNamespace cacheDomain = type.getAnnotation(CacheNamespace.class);
        if (cacheDomain != null) {
            Integer size = cacheDomain.size() == 0 ? null : cacheDomain.size();
            Long flushInterval = cacheDomain.flushInterval() == 0 ? null : cacheDomain.flushInterval();
            Properties props = convertToProperties(cacheDomain.properties());
            assistant.useNewCache(cacheDomain.implementation(), cacheDomain.eviction(), flushInterval, size, cacheDomain.readWrite(), cacheDomain.blocking(), props);
        }
    }

    /**
     * 将 {@link Property} 注解数组，转换成 {@link Properties} 对象（      例如可以在注解{@link CacheNamespace}中作以下定义：{@code @CacheNamespace(properties = {@Property(name = "a", value = "a"), @Property(name = "b", value = "b")})}      ）
     * <ol>
     *     <li>
     *         如果{@code properties}.length == 0：直接return null，本方法结束；否则什么也不做，继续往下走
     *     </li>
     *     <li>
     *         new 一个{@link Properties}对象，然后遍历迭代{@code properties}数组，对于每一个迭代的元素（{@link Property}注解对象）：
     *         <ol>
     *             <li>
     *                 调用{@link PropertyParser#parse(String, Properties)}传入当前迭代元素{@link Property#value()}和{@link #configuration}的{@link Configuration#getVariables()}对字符串中的"${}"Token进行解析替换成对应的变量
     *             </li>
     *             <li>
     *                 调用刚new的{@link Properties}对象的{@link Properties#put(Object, Object)}传入当前迭代元素{@link Property#name()}和前一步进行Token替换之后的{@link Property#value()}进行键值对设置
     *             </li>
     *         </ol>
     *     </li>
     *     <li>
     *         返回第2步处理得到的{@link Properties}对象
     *     </li>
     * </ol>
     *
     * @param properties {@link Property} 注解数组
     * @return {@link Properties} 对象
     */
    private Properties convertToProperties(Property[] properties) {
        if (properties.length == 0) {
            return null;
        }
        Properties props = new Properties();
        for (Property property : properties) {
            props.setProperty(property.name(), PropertyParser.parse(property.value(), configuration.getVariables()));
        }
        return props;
    }


    /**
     * 解析 {@link CacheNamespaceRef} 注解，根据namespace查找指定的{@link org.apache.ibatis.cache.Cache}设置给当前mapper：
     * <ol>
     *     <li>
     *         调用{@link #type}的{@link Class#getAnnotation(Class)}传入{@link CacheNamespaceRef}.class获取mapper接口上定义的{@link CacheNamespaceRef}注解对象
     *     </li>
     *     <li>
     *         判断：前一步获取的注解对象 != null
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         调用{@link CacheNamespaceRef#value()}和{@link CacheNamespaceRef#name()}获取到一个{@link Class}对象"refType"（{@link org.apache.ibatis.cache.Cache}的namespace作为类全限定名字符串对应的Class）和一个字符串"refName"（{@link org.apache.ibatis.cache.Cache}的namespace）
     *                     </li>
     *                     <li>
     *                         判断如果refType == void.class 并且 refName.isEmpty()，抛出异常{@link BuilderException}；否则什么也不做，继续往下走
     *                     </li>
     *                     <li>
     *                         判断如果refType != void.class 并且 !refName.isEmpty()，抛出异常{@link BuilderException}；否则什么也不做，继续往下走
     *                     </li>
     *                     <li>
     *                         如果refType != void.class：
     *                         <ul>
     *                             <li>
     *                                 true：调用{@link #assistant}的{@link MapperBuilderAssistant#useCacheRef(String)}传入refType的{@link Class#getName()}，本方法结束
     *                             </li>
     *                             <li>
     *                                 false：调用{@link #assistant}的{@link MapperBuilderAssistant#useCacheRef(String)}传入refName，本方法结束
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
     */
    private void parseCacheRef() {
        CacheNamespaceRef cacheDomainRef = type.getAnnotation(CacheNamespaceRef.class);
        if (cacheDomainRef != null) {
            Class<?> refType = cacheDomainRef.value();
            String refName = cacheDomainRef.name();
            if (refType == void.class && refName.isEmpty()) {
                throw new BuilderException("Should be specified either value() or name() attribute in the @CacheNamespaceRef");
            }
            if (refType != void.class && !refName.isEmpty()) {
                throw new BuilderException("Cannot use both value() and name() attribute in the @CacheNamespaceRef");
            }
            String namespace = (refType != void.class) ? refType.getName() : refName;
            assistant.useCacheRef(namespace);
        }
    }

    /**
     * 解析其它注解，返回 resultMapId 属性，逻辑上，和 {@link XMLMapperBuilder#resultMapElement(XNode, List)} 是类似的。
     * <ol>
     *     <li>调用{@link #getReturnType(Method)}传入{@code method}获取方法的返回值实际类型{@link Class}对象</li>
     *     <li>调用{@code method}的{@link Method#getAnnotation(Class)}分别传入{@link ConstructorArgs}.class、{@link Results}.class、{@link TypeDiscriminator}.class ，尝试获取方法上可能声明了的这三个注解对象</li>
     *     <li>调用{@link #generateResultMapName(Method)}传入{@code method}获取 "resultMapId"</li>
     *     <li>调用{@link #argsIf(ConstructorArgs)}传入第二步获取的{@link ConstructorArgs}对象，调用{@link #resultsIf(Results)}传入第二步获取的{@link Results}对象，保证这两个对象不为null</li>
     *     <li>调用{@link #applyResultMap(String, Class, Arg[], Result[], TypeDiscriminator)}传入 第3步获取的"resultMapId"、第一步获取的返回值{@link Class}对象、前一步进行预防null处理之后的{@link ConstructorArgs#value()}、前一步进行预防null处理之后的{@link Results#value()}、第3步获取的{@link TypeDiscriminator}注解对象 五个参数构建{@link org.apache.ibatis.mapping.ResultMap}对象</li>
     *     <li>return 第三步获取的"resultMapId"，方法结束</li>
     * </ol>
     *
     * @param method 方法
     * @return resultMapId
     */
    private String parseResultMap(Method method) {
        Class<?> returnType = getReturnType(method);
        ConstructorArgs args = method.getAnnotation(ConstructorArgs.class);
        Results results = method.getAnnotation(Results.class);
        TypeDiscriminator typeDiscriminator = method.getAnnotation(TypeDiscriminator.class);
        String resultMapId = generateResultMapName(method);
        applyResultMap(resultMapId, returnType, argsIf(args), resultsIf(results), typeDiscriminator);
        return resultMapId;
    }

    /**
     * 生成 resultMapId：优先获取用户通过{@link Results}定义的；然后自动生成
     * <ol>
     *     <li>
     *         调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link Results}.class尝试获取方法上可能定义了的{@link Results}注解对象
     *         <ul>
     *             如果获取到的注解对象不是null 并且 调用{@link Results#id()}的{@link String#isEmpty()}为false
     *             <li>true：调用{@link #type}的{@link Class#getName()}拼接"."拼接{@link Results#id()}并return，方法结束</li>
     *             <li>false：什么也不做，继续往下走</li>
     *         </ul>
     *     </li>
     *     <li>
     *         来到这里，说明用户没有声明该id，所以根据下面步骤自动生成
     *     </li>
     *     <li>
     *         new 一个 {@link StringBuilder}对象，然后根据{@code method}的{@link Method#getParameterTypes()}获取方法参数类型数组并遍历迭代该数组，对于每一个参数类型对象{@link Class}：先拼接字符串"-"到前面刚new的{@link StringBuilder}中，然后再拼接当前元素的{@link Class#getSimpleName()}到其中
     *     </li>
     *     <li>
     *         判断前一步的{@link StringBuilder}对象的{@link StringBuilder#length()} < 1：
     *         <ul>
     *             <li>true：说明该方法没有参数，拼接"-void"到该{@link StringBuilder}对象</li>
     *             <li>false：什么也不做，继续往下走</li>
     *         </ul>
     *     </li>
     *     <li>拼接{@link #type}的{@link Class#getName()}拼接"."拼接{@code method}的{@link Method#getName()}拼接前面步骤构建好的{@link StringBuilder}对象（类型+方法名+方法参数类型列表）并返回拼接结果，方法结束</li>
     * </ol>
     *
     * @param method 方法对象
     * @return resultMapId
     */
    private String generateResultMapName(Method method) {
        Results results = method.getAnnotation(Results.class);
        if (results != null && !results.id().isEmpty()) {
            return type.getName() + "." + results.id();
        }
        StringBuilder suffix = new StringBuilder();
        for (Class<?> c : method.getParameterTypes()) {
            suffix.append("-");
            suffix.append(c.getSimpleName());
        }
        if (suffix.length() < 1) {
            suffix.append("-void");
        }
        return type.getName() + "." + method.getName() + suffix;
    }

    /**
     * 构建{@link org.apache.ibatis.mapping.ResultMap}对象：
     * <ol>
     *     <li>new 一个泛型为{@link ResultMapping}的{@link ArrayList}对象</li>
     *     <li>调用{@link #applyConstructorArgs(Arg[], Class, List)}传入{@code args}、{@code returnType}、第一步new的用来装载{@link ResultMapping}的{@link ArrayList}容器 共三个参数，处理"构造参数的mapping"并将其添加到第一步new的容器中</li>
     *     <li>调用{@link #applyResults(Result[], Class, List)} 传入{@code results}、{@code returnType}、第一步new的用来装载{@link ResultMapping}的{@link ArrayList}容器 共三个参数，处理"普通属性的mapping"并将其添加到第一步new的容器中</li>
     *     <li>调用{@link #applyDiscriminator(String, Class, TypeDiscriminator)}传入{@code resultMapId}、{@code returnType}、{@code discriminator}尝试构建一个{@link Discriminator}对象</li>
     *     <li>调用{@link #assistant}的{@link MapperBuilderAssistant#addResultMap(String, Class, String, Discriminator, List, Boolean)}传入 {@code resultMapId}、{@code returnType}、null、前一步获得的{@link Discriminator}对象、第一步new的{@link ArrayList}对象、null 共6个参数进行{@link org.apache.ibatis.mapping.ResultMap}构建</li>
     *     <li>调用{@link #createDiscriminatorResultMaps(String, Class, TypeDiscriminator)}传入 {@code resultMapId}、{@code returnType}、第4步获取的{@link Discriminator}对象 三个参数尝试构建{@link Discriminator}包含的{@link org.apache.ibatis.mapping.ResultMap}对象</li>
     * </ol>
     *
     * @param resultMapId resultMap的id
     * @param returnType
     * @param args {@link ConstructorArgs#value()}
     * @param results {@link Results#value()}
     * @param discriminator {@link TypeDiscriminator}对象
     */
    private void applyResultMap(String resultMapId, Class<?> returnType, Arg[] args, Result[] results, TypeDiscriminator discriminator) {
        List<ResultMapping> resultMappings = new ArrayList<>();
        applyConstructorArgs(args, returnType, resultMappings);
        applyResults(results, returnType, resultMappings);
        Discriminator disc = applyDiscriminator(resultMapId, returnType, discriminator);
        // TODO add AutoMappingBehaviour
        assistant.addResultMap(resultMapId, returnType, null, disc, resultMappings, null);
        createDiscriminatorResultMaps(resultMapId, returnType, discriminator);
    }

    /**
     * 解析{@link TypeDiscriminator}注解并构建其中的{@link Case}注解对应的{@link org.apache.ibatis.mapping.ResultMap}对象
     * <ul>
     *     判断{@code discriminator} != null
     *     <li>
     *         true：
     *         <ol>
     *             <li>调用{@code discriminator}的{@link TypeDiscriminator#cases()}获取一个{@link Case}数组</li>
     *             <li>
     *                 遍历迭代获得的{@link Case}数组，对于每一个迭代的{@link Case}对象：
     *                 <ol>
     *                     <li>
     *                         拼接{@code resultMapId}拼接"-"拼接当前迭代的{@link Case}对象的{@link Case#value()}作为当前"case"指向的{@link ResultMap}的id
     *                     </li>
     *                     <li>new 一个泛型为{@link ResultMapping}的{@link ArrayList}容器</li>
     *                     <li>调用{@link #applyConstructorArgs(Arg[], Class, List)}传入 当前迭代对象{@link Case#constructArgs()}、{@code resultType}、前面new的{@link ArrayList}容器 共三个参数，处理"构造参数的mapping"并将其添加到刚n
     *                     <li>调用{@link #applyResults(Result[], Class, List)} 传入 当前迭代对象{@link Case#results()}、{@code resultType}、前面new的{@link ArrayList}容器 共三个参数，处理"普通属性的mapping"并将其添加到刚new的容器中</li>
     *                     <li>调用{@link #assistant}的{@link MapperBuilderAssistant#addResultMap(String, Class, String, Discriminator, List, Boolean)}传入  前面拼接得到的{@link ResultMap}id、当前迭代的{@link Case}对象的{@link Case#type()}、{@code resultMapId}、null、前面new的{@link ArrayList}容器、null  共6个参数进行{@link org.apache.ibatis.mapping.ResultMap}对象的构建</li>
     *                 </ol>
     *             </li>
     *         </ol>
     *     </li>
     *     <li>false：什么也不做，方法结束</li>
     * </ul>
     *
     * @param resultMapId resultMap的id
     * @param resultType
     * @param discriminator 方法上可能定义了的{@link TypeDiscriminator}注解
     */
    private void createDiscriminatorResultMaps(String resultMapId, Class<?> resultType, TypeDiscriminator discriminator) {
        if (discriminator != null) {
            for (Case c : discriminator.cases()) {
                String caseResultMapId = resultMapId + "-" + c.value();
                List<ResultMapping> resultMappings = new ArrayList<>();
                // issue #136
                applyConstructorArgs(c.constructArgs(), resultType, resultMappings);
                applyResults(c.results(), resultType, resultMappings);
                // TODO add AutoMappingBehaviour
                assistant.addResultMap(caseResultMapId, c.type(), resultMapId, null, resultMappings, null);
            }
        }
    }

    /**
     * 创建 {@link Discriminator} 对象，和 {@link XMLMapperBuilder#processDiscriminatorElement(XNode, Class, List)} 方法的逻辑是一致的
     * <ul>
     *     判断{@code discriminator} != null
     *     <li>
     *         true：调用{@link #assistant}的{@link MapperBuilderAssistant#buildDiscriminator(Class, String, Class, JdbcType, Class, Map)}传入
     *         <ol>
     *             <li>{@code resultType}</li>
     *             <li>{@code discriminator}的{@link TypeDiscriminator#column()}</li>
     *             <li>{@code discriminator}的{@link TypeDiscriminator#javaType()} == void.class ? {@link String}.class : {@code discriminator}的{@link TypeDiscriminator#javaType()}</li>
     *             <li>{@code discriminator}的{@link TypeDiscriminator#jdbcType()} == {@link JdbcType#UNDEFINED} ? null : {@code discriminator}的{@link TypeDiscriminator#jdbcType()}</li>
     *             <li>{@code discriminator}的{@link TypeDiscriminator#typeHandler()} == {@link UnknownTypeHandler}.class ? null : {@code discriminator}的{@link TypeDiscriminator#typeHandler()}  （强转表达式结果为{@code Class<? extends TypeHandler<?>>}）</li>
     *             <li>new 一个泛型为{@code <String, String>}的{@link HashMap}对象，遍历{@link TypeDiscriminator#cases()}，对于遍历中每一个迭代的{@link Case}注解对象：将{@link Case#value()}作为key，{@code resultMapId}拼接"-"拼接{@link Case#value()}作为value，put到new的{@link HashMap}中，传入该{@link HashMap}对象</li>
     *         </ol>
     *         共6个参数构建一个{@link Discriminator}对象并return，方法结束
     *     </li>
     *     <li>false：什么也不做，return null，方法结束</li>
     * </ul>
     *
     * @param resultMapId resultMap的id
     * @param resultType
     * @param discriminator {@link TypeDiscriminator}注解对象
     * @return
     */
    private Discriminator applyDiscriminator(String resultMapId, Class<?> resultType, TypeDiscriminator discriminator) {
        if (discriminator != null) {
            String column = discriminator.column();
            Class<?> javaType = discriminator.javaType() == void.class ? String.class : discriminator.javaType();
            JdbcType jdbcType = discriminator.jdbcType() == JdbcType.UNDEFINED ? null : discriminator.jdbcType();
            @SuppressWarnings("unchecked")
            Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>)
                    (discriminator.typeHandler() == UnknownTypeHandler.class ? null : discriminator.typeHandler());
            Case[] cases = discriminator.cases();
            Map<String, String> discriminatorMap = new HashMap<>();
            for (Case c : cases) {
                String value = c.value();
                String caseResultMapId = resultMapId + "-" + value;
                discriminatorMap.put(value, caseResultMapId);
            }
            return assistant.buildDiscriminator(resultType, column, javaType, jdbcType, typeHandler, discriminatorMap);
        }
        return null;
    }

    /**
     * 解析方法上的 SQL 操作相关的注解
     * <ol>
     *     <li>
     *         调用{@link #getParameterType(Method)}传入{@code method}获取方法的参数类型、调用{@link #getLanguageDriver(Method)}传入{@code method}获取{@link LanguageDriver}对象
     *     </li>
     *     <li>
     *         调用{@link #getSqlSourceFromAnnotations(Method, Class, LanguageDriver)}传入{@code method}、第一步获取的方法的参数类型、第一步获取的{@link LanguageDriver}对象 三个参数，构建一个{@link SqlSource}对象
     *     </li>
     *     <li>
     *         判断前一步获取的{@link SqlSource}对象 != null：
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link Options}.class获取{@link Options}注解实例
     *                     </li>
     *                     <li>
     *                         设置各种默认值：
     *                         <ol>
     *                             <li>调用{@link #type}的{@link Class#getName()}拼接"."拼接{@code method}的{@link Method#getName()}作为当前准备构建的{@link MappedStatement}的id</li>
     *                             <li>fetchSize = null、timeout = null、statementType = {@link StatementType#PREPARED}、resultSetType = null、sqlCommandType = 调用{@link #getSqlCommandType(Method)}传入{@code method}的结果、flushCache = 变量"sqlCommandType" != {@link SqlCommandType#SELECT}、useCache = 变量"sqlCommandType" == {@link SqlCommandType#SELECT}、keyProperty = null、keyColumn = null、声明一个keyGenerator变量为初始化</li>
     *                         </ol>
     *                     </li>
     *                     <li>
     *                         判断 {@link SqlCommandType#INSERT}.equals(前一步设置的变量"sqlCommandType") 或者 {@link SqlCommandType#UPDATE}.equals(前一步设置的变量"sqlCommandType")
     *                         <ul>
     *                             <li>
     *                                 true：调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link SelectKey}.class尝试获取方法上可能声明了的{@link SelectKey}注解对象
     *                                 <ul>
     *                                     判断 获得的注解对象 != null
     *                                     <li>
     *                                         true：调用{@link #handleSelectKeyAnnotation(SelectKey, String, Class, LanguageDriver)}传入 获取到的{@link SelectKey}注解对象、前面拼接的{@link MappedStatement}的id、调用{@link #getParameterType(Method)}传入{@code method}得到的{@link Class}对象、第一步获取到的{@link LanguageDriver}对象 四个参数构建一个{@link KeyGenerator}对象暂存到"keyGenerator'；调用{@link SelectKey}注解对象的{@link SelectKey#keyProperty()}暂存到"keyProperty"
     *                                     </li>
     *                                     <li>
     *                                         false：判断前面获取的{@link Options}注解实例 == null
     *                                         <ul>
     *                                             <li>
     *                                                 true：判断{@link #configuration}的{@link Configuration#isUseGeneratedKeys()}：
     *                                                 <ul>
     *                                                     <li>true：暂存{@link Jdbc3KeyGenerator#INSTANCE}到"keyGenerator"</li>
     *                                                     <li>false：暂存{@link NoKeyGenerator#INSTANCE}到"keyGenerator"</li>
     *                                                 </ul>
     *                                             </li>
     *                                             <li>
     *                                                 false：
     *                                                 <ol>
     *                                                     <li>
     *                                                         判断该{@link Options}的{@link Options#useGeneratedKeys()}
     *                                                         <ul>
     *                                                             <li>true：暂存{@link Jdbc3KeyGenerator#INSTANCE}到"keyGenerator"</li>
     *                                                             <li>false：暂存{@link NoKeyGenerator#INSTANCE}到"keyGenerator"</li>
     *                                                         </ul>
     *                                                     </li>
     *                                                     <li>
     *                                                         暂存该{@link Options}的{@link Options#keyProperty()} 到"keyProperty"、暂存该{@link Options}的{@link Options#keyColumn()} 到"keyColumn"
     *                                                     </li>
     *                                                 </ol>
     *                                             </li>
     *                                         </ul>
     *                                     </li>
     *                                 </ul>
     *                             </li>
     *                             <li>
     *                                 false：暂存"keyGenerator"为{@link NoKeyGenerator#INSTANCE}
     *                             </li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         判断前面获取的{@link Options}注解对象 != null
     *                         <ul>
     *                             <li>
     *                                 true：
     *                                 <ol>
     *                                     <li>
     *                                         如果{@link FlushCachePolicy#TRUE}.equals({@link Options#flushCache()})则覆盖true到3.true.2步骤中设置的"flushCache"变量；如果{@link FlushCachePolicy#FALSE}.equals({@link Options#flushCache()})则覆盖false到3.true.2步骤中设置的"flushCache"变量；否则什么也不做，继续往下走
     *                                     </li>
     *                                     <li>{@link Options#useCache()}覆盖3.true.2步骤中设置的"useCache"变量</li>
     *                                     <li>当{@link Options#fetchSize()}大于-1或者等于{@link Integer#MIN_VALUE}的时候，其覆盖3.true.2步骤中设置的"fetchSize"变量；否则设置null到"fetchSize"变量</li>
     *                                     <li>当{@link Options#timeout()}大于-1的时候，其覆盖3.true.2步骤中设置的"timeout"变量；否则设置null到"timeout"变量</li>
     *                                     <li>{@link Options#statementType()}覆盖3.true.2步骤中设置的"statementType"变量</li>
     *                                     <li>{@link Options#resultSetType()}覆盖3.true.2步骤中设置的"resultSetType"变量</li>
     *                                 </ol>
     *                             </li>
     *                             <li>false：什么也不做，继续往下走</li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link ResultMap}.class尝试获取方法上可能定义了的{@link ResultMap}注解对象
     *                         <ul>
     *                             判断该对象!=null
     *                             <li>
     *                                 true：调用{@link ResultMap#value()}得到一个{@link String}数组，然后new一个{@link StringBuilder}，遍历该{@link String}数组，以逗号作为分隔符将所有字符串拼接到该{@link StringBuilder}，并暂存{@link StringBuilder#toString()}到变量"resultMapId"
     *                             </li>
     *                             <li>
     *                                 false：判断3.true.2中设置的变量"sqlCommandType"=={@link SqlCommandType#SELECT}
     *                                 <ul>
     *                                     <li>true：调用{@link #parseResultMap(Method)}传入{@code method}将返回结果暂存到变量"resultMapId"</li>
     *                                     <li>false：什么也不做，继续往下走</li>
     *                                 </ul>
     *                             </li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         调用{@link #assistant}的{@link MapperBuilderAssistant#addMappedStatement(String, SqlSource, StatementType, SqlCommandType, Integer, Integer, String, Class, String, Class, ResultSetType, boolean, boolean, boolean, KeyGenerator, String, String, String, LanguageDriver, String)}传入：
     *                         <ol>
     *                             <li>3.true.2.1中拼接的"mappedStatement的id"</li>
     *                             <li>第2步构建的{@link SqlSource}对象</li>
     *                             <li>变量"statementType"</li>
     *                             <li>变量"sqlCommandType"</li>
     *                             <li>变量"fetchSize"</li>
     *                             <li>变量"timeout"</li>
     *                             <li>null</li>
     *                             <li>第一步获得的参数类型{@link Class}对象</li>
     *                             <li>第5步解析{@link org.apache.ibatis.mapping.ResultMap}得到的变量"resultMapId"</li>
     *                             <li>调用{@link #getReturnType(Method)}传入{@code method}解析到的返回值类型{@link Class}对象</li>
     *                             <li>变量"resultSetType"</li>
     *                             <li>变量"flushCache"</li>
     *                             <li>变量"useCache"</li>
     *                             <li>false</li>
     *                             <li>第3步中暂存的变量"keyGenerator"</li>
     *                             <li>变量"keyProperty"</li>
     *                             <li>变量"keyColumn"</li>
     *                             <li>null</li>
     *                             <li>第一步获得的{@link LanguageDriver}对象</li>
     *                             <li>3.true.1中获取的{@link Options}注解对象 != null ? 调用{@link #nullOrEmpty(String)}传入该{@link Options#resultSets()}返回的结果 : null</li>
     *                         </ol>
     *                         共20个参数构建一个{@link MappedStatement}对象，本方法结束
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false：什么也不做，方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param method 方法
     */
    @SuppressWarnings("ConstantConditions")
    void parseStatement(Method method) {
        Class<?> parameterTypeClass = getParameterType(method);
        LanguageDriver languageDriver = getLanguageDriver(method);
        SqlSource sqlSource = getSqlSourceFromAnnotations(method, parameterTypeClass, languageDriver);
        if (sqlSource != null) {
            Options options = method.getAnnotation(Options.class);
            final String mappedStatementId = type.getName() + "." + method.getName();
            Integer fetchSize = null;
            Integer timeout = null;
            StatementType statementType = StatementType.PREPARED;
            ResultSetType resultSetType = null;
            SqlCommandType sqlCommandType = getSqlCommandType(method);
            boolean isSelect = sqlCommandType == SqlCommandType.SELECT;
            boolean flushCache = !isSelect;
            boolean useCache = isSelect;

            KeyGenerator keyGenerator;
            String keyProperty = null;
            String keyColumn = null;
            if (SqlCommandType.INSERT.equals(sqlCommandType) || SqlCommandType.UPDATE.equals(sqlCommandType)) {
                // first check for SelectKey annotation - that overrides everything else
                SelectKey selectKey = method.getAnnotation(SelectKey.class);
                if (selectKey != null) {
                    keyGenerator = handleSelectKeyAnnotation(selectKey, mappedStatementId, getParameterType(method), languageDriver);
                    keyProperty = selectKey.keyProperty();
                } else if (options == null) {
                    keyGenerator = configuration.isUseGeneratedKeys() ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
                } else {
                    keyGenerator = options.useGeneratedKeys() ? Jdbc3KeyGenerator.INSTANCE : NoKeyGenerator.INSTANCE;
                    keyProperty = options.keyProperty();
                    keyColumn = options.keyColumn();
                }
            } else {
                keyGenerator = NoKeyGenerator.INSTANCE;
            }

            if (options != null) {
                if (FlushCachePolicy.TRUE.equals(options.flushCache())) {
                    flushCache = true;
                } else if (FlushCachePolicy.FALSE.equals(options.flushCache())) {
                    flushCache = false;
                }
                useCache = options.useCache();
                fetchSize = options.fetchSize() > -1 || options.fetchSize() == Integer.MIN_VALUE ? options.fetchSize() : null; //issue #348
                timeout = options.timeout() > -1 ? options.timeout() : null;
                statementType = options.statementType();
                resultSetType = options.resultSetType();
            }

            String resultMapId = null;
            ResultMap resultMapAnnotation = method.getAnnotation(ResultMap.class);
            if (resultMapAnnotation != null) {
                String[] resultMaps = resultMapAnnotation.value();
                StringBuilder sb = new StringBuilder();
                for (String resultMap : resultMaps) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append(resultMap);
                }
                resultMapId = sb.toString();
            } else if (isSelect) {
                resultMapId = parseResultMap(method);
            }

            assistant.addMappedStatement(
                    mappedStatementId,
                    sqlSource,
                    statementType,
                    sqlCommandType,
                    fetchSize,
                    timeout,
                    // ParameterMapID
                    null,
                    parameterTypeClass,
                    resultMapId,
                    getReturnType(method),
                    resultSetType,
                    flushCache,
                    useCache,
                    // TODO gcode issue #577
                    false,
                    keyGenerator,
                    keyProperty,
                    keyColumn,
                    // DatabaseID
                    null,
                    languageDriver,
                    // ResultSets
                    options != null ? nullOrEmpty(options.resultSets()) : null);
        }
    }

    /**
     * 获得 {@link LanguageDriver} 对象
     * <ol>
     *     <li>
     *         调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link Lang}.class获取方法上可能存在的{@link Lang}注解对象
     *     </li>
     *     <li>
     *         判断获得的注解对象是否为null
     *         <ul>
     *             <li>
     *                 是：直接调用{@link #assistant}的{@link MapperBuilderAssistant#getLanguageDriver(Class)}传入null获取对应的{@link LanguageDriver}并return，本方法结束
     *             </li>
     *             <li>
     *                 否：直接调用{@link #assistant}的{@link MapperBuilderAssistant#getLanguageDriver(Class)}传入{@link Lang#value()}获取对应的{@link LanguageDriver}并return，本方法结束
     *             </li>
     *         </ul>
     *     </li>
     * </ol>
     *
     * @param method 方法
     * @return {@link LanguageDriver} 对象
     */
    private LanguageDriver getLanguageDriver(Method method) {
        Lang lang = method.getAnnotation(Lang.class);
        Class<? extends LanguageDriver> langClass = null;
        if (lang != null) {
            langClass = lang.value();
        }
        return assistant.getLanguageDriver(langClass);
    }

    /**
     * 获得参数的类型（过滤{@link RowBounds}和{@link ResultHandler}类型的参数如果还有都参数就返回{@link ParamMap}.class；如果是单参数就返回该参数类型）
     * <ol>
     *     <li>
     *         调用{@code method}的{@link Method#getParameterTypes()}获取参数类型数组{@link Class}[]
     *     </li>
     *     <li>
     *         声明一个{@link Class}类型的变量"parameterType"用来装载要返回的"参数类型"并赋值为null
     *     </li>
     *     <li>
     *         遍历迭代参数类型数组，对于每一个迭代的参数类型对象{@link Class}：判断 !{@link RowBounds}.class.isAssignableFrom(当前迭代的元素) && !{@link ResultHandler}.class.isAssignableFrom(当前迭代的元素)
     *         <ul>
     *             <li>
     *                 如果变量"parameterType"还是null：赋值当前迭代的元素到该变量；否则赋值{@link ParamMap}.class给它
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         返回变量"parameterType"
     *     </li>
     * </ol>
     *
     * @param method 方法
     * @return 参数的类型
     */
    private Class<?> getParameterType(Method method) {
        Class<?> parameterType = null;
        Class<?>[] parameterTypes = method.getParameterTypes();
        for (Class<?> currentParameterType : parameterTypes) {
            if (!RowBounds.class.isAssignableFrom(currentParameterType) && !ResultHandler.class.isAssignableFrom(currentParameterType)) {
                if (parameterType == null) {
                    parameterType = currentParameterType;
                } else {
                    // issue #135
                    parameterType = ParamMap.class;
                }
            }
        }
        return parameterType;
    }

    /**
     * 根据一定规则获取方法的返回类型（包括泛型的解析）
     * <ol>
     *     <li>
     *         调用{@code method}的{@link Method#getReturnType()}获取返回值类型对象{@link Class}暂存到"returnType"
     *     </li>
     *     <li>
     *         调用{@link TypeParameterResolver#resolveReturnType(Method, Type)}传入{@code method}和{@link #type}获得该方法返回值类型的{@link Type}对象并暂存到"resolvedReturnType"（解析填充了其中的泛型参数）
     *     </li>
     *     <li>
     *         判断前一步获取的方法返回值类型的{@link Type}对象"resolvedReturnType" instanceof {@link Class}
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         将变量"resolvedReturnType"强转为{@link Class}类型然后覆盖到变量"returnType"
     *                     </li>
     *                     <li>
     *                         判断"returnType"的{@link Class#isArray()}
     *                         <ul>
     *                             <li>true：调用"returnType"的{@link Class#getComponentType()}再次覆盖到"returnType"</li>
     *                             <li>false：什么也不做，继续往下走</li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         判断void.class.equals(变量"returnType")
     *                         <ul>
     *                             <li>true：调用{@code method}的{@link Method#getAnnotation(Class)}传入{@link ResultType}.class尝试获取方法可能声明了的{@link ResultType}注解对象，如果获取到的对象不是null：则调用{@link ResultType#value()}再次覆盖变量"returnType"；否则什么也不做</li>
     *                             <li>false：什么也不做，继续往下走</li>
     *                         </ul>
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false：判断前一步获取的方法返回值类型的{@link Type}对象"resolvedReturnType" instanceof {@link ParameterizedType}
     *                 <ul>
     *                     <li>
     *                         true：
     *                         <ol>
     *                             <li>
     *                                 将变量"resolvedReturnType"强转为{@link ParameterizedType}类型然后暂存到变量"parameterizedType"
     *                             </li>
     *                             <li>
     *                                 调用变量"parameterizedType"的{@link ParameterizedType#getRawType()}参数化类型对象的原生类型{@link Class}对象
     *                             </li>
     *                             <li>
     *                                 判断 {@link Collection}.class.isAssignableFrom(获取到的原生类型{@link Class}对象) 或者 {@link Cursor}.class.isAssignableFrom(获取到的原生类型{@link Class}对象)
     *                                 <ul>
     *                                     <li>
     *                                         true：
     *                                         <ol>
     *                                             <li>
     *                                                 调用变量"parameterizedType"的{@link ParameterizedType#getActualTypeArguments()}获取到其类型参数数组，判断获得的类型参数列表 != null 并且 长度length == 1：
     *                                                 <ul>
     *                                                     <li>
     *                                                         true：直接获取类型参数数组索引为0的第一个元素，然后看这个元素是否instanceof {@link Class}：如果是就将这个元素强转为{@link Class}类型然后覆盖到变量"returnType"；否则再判断这个
     *                                                         元素是否instanceof {@link ParameterizedType}：是则强转这个元素为{@link ParameterizedType}然后调用{@link ParameterizedType#getRawType()}覆盖到变量"returnType"；否则
     *                                                         再判断这个元素是否instanceof {@link GenericArrayType}：是则强转这个元素为{@link GenericArrayType}，然后调用{@link GenericArrayType#getGenericComponentType()}然后将结果
     *                                                         强转{@link Class}类型，最后调用{@link Array#newInstance(Class, int)}传入该强转之后的{@link Class}类型对象和0覆盖到变量"returnType"；否则什么都不做，继续往下走
     *                                                     </li>
     *                                                     <li>false：什么也不做，继续往下走</li>
     *                                                 </ul>
     *                                             </li>
     *                                         </ol>
     *                                     </li>
     *                                     <li>
     *                                         false：调用{@code method}的{@link Method#isAnnotationPresent(Class)}传入{@link MapKey}.class判断方法上有没有声明了该注解 并且 {@link Map}.class.isAssignableFrom(获取到的原生类型{@link Class}对象)
     *                                         <ul>
     *                                             <li>
     *                                                 true：
     *                                                 <ol>
     *                                                     <li>
     *                                                         调用前面暂存的变量的"parameterizedType"的{@link ParameterizedType#getActualTypeArguments()}获取解析好的类型实参数组
     *                                                     </li>
     *                                                     <li>
     *                                                         判断该类型实参数组 != null 并且 其length == 2：
     *                                                         <ul>
     *                                                             <li>true：获取类型实参数组的第二个元素（索引为1），判断该元素是否 instanceof {@link Class}：是则直接强转给元素为{@link Class}类型并覆盖到变量"returnType"；否则再判断该元素是否 instanceof {@link ParameterizedType}类型：是则强转该元素为{@link ParameterizedType}类型然后调用{@link ParameterizedType#getRawType()}获取其原生类型然后强转结果为{@link Class}类型再覆盖到变量"returnType"；否则什么也不做，继续往下走</li>
     *                                                             <li>false：什么也不做，继续往下走</li>
     *                                                         </ul>
     *                                                     </li>
     *                                                 </ol>
     *                                             </li>
     *                                             <li>
     *                                                 false：判断 {@link Optional}.class.equals(获取到的原生类型{@link Class}对象)
     *                                                 <ul>
     *                                                     <li>true：前面暂存的变量的"parameterizedType"的{@link ParameterizedType#getActualTypeArguments()}获取解析好的类型实参数组，并根据索引0直接获取第一个类型实参元素，然后判断该类型对象是否 instanceof {@link Class}：是则直接将该元素强转为{@link Class}对象然后覆盖到变量"returnType"；否则什么也不做，继续往下走</li>
     *                                                     <li>false：什么也不做，继续往下走</li>
     *                                                 </ul>
     *                                             </li>
     *                                         </ul>
     *                                     </li>
     *                                 </ul>
     *                             </li>
     *                         </ol>
     *                     </li>
     *                     <li>false：什么也不做，继续往下走</li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>return 前一步得到的变量"returnType"，方法结束</li>
     * </ol>
     *
     * @param method 方法对象{@link Method}
     * @return 返回类型
     */
    private Class<?> getReturnType(Method method) {
        Class<?> returnType = method.getReturnType();
        Type resolvedReturnType = TypeParameterResolver.resolveReturnType(method, type);
        if (resolvedReturnType instanceof Class) {
            returnType = (Class<?>) resolvedReturnType;
            if (returnType.isArray()) {
                returnType = returnType.getComponentType();
            }
            // gcode issue #508
            if (void.class.equals(returnType)) {
                ResultType rt = method.getAnnotation(ResultType.class);
                if (rt != null) {
                    returnType = rt.value();
                }
            }
        } else if (resolvedReturnType instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) resolvedReturnType;
            Class<?> rawType = (Class<?>) parameterizedType.getRawType();
            if (Collection.class.isAssignableFrom(rawType) || Cursor.class.isAssignableFrom(rawType)) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                if (actualTypeArguments != null && actualTypeArguments.length == 1) {
                    Type returnTypeParameter = actualTypeArguments[0];
                    if (returnTypeParameter instanceof Class<?>) {
                        returnType = (Class<?>) returnTypeParameter;
                    } else if (returnTypeParameter instanceof ParameterizedType) {
                        // (gcode issue #443) actual type can be a also a parameterized type
                        returnType = (Class<?>) ((ParameterizedType) returnTypeParameter).getRawType();
                    } else if (returnTypeParameter instanceof GenericArrayType) {
                        Class<?> componentType = (Class<?>) ((GenericArrayType) returnTypeParameter).getGenericComponentType();
                        // (gcode issue #525) support List<byte[]>
                        returnType = Array.newInstance(componentType, 0).getClass();
                    }
                }
            } else if (method.isAnnotationPresent(MapKey.class) && Map.class.isAssignableFrom(rawType)) {
                // (gcode issue 504) Do not look into Maps if there is not MapKey annotation
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                if (actualTypeArguments != null && actualTypeArguments.length == 2) {
                    Type returnTypeParameter = actualTypeArguments[1];
                    if (returnTypeParameter instanceof Class<?>) {
                        returnType = (Class<?>) returnTypeParameter;
                    } else if (returnTypeParameter instanceof ParameterizedType) {
                        // (gcode issue 443) actual type can be a also a parameterized type
                        returnType = (Class<?>) ((ParameterizedType) returnTypeParameter).getRawType();
                    }
                }
            } else if (Optional.class.equals(rawType)) {
                Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                Type returnTypeParameter = actualTypeArguments[0];
                if (returnTypeParameter instanceof Class<?>) {
                    returnType = (Class<?>) returnTypeParameter;
                }
            }
        }

        return returnType;
    }


    /**
     * 从方法注解中，获得 {@link SqlSource} 对象
     * <ol>
     *     <li>
     *         调用{@link #getSqlAnnotationType(Method)}尝试获取{@code method}上可能存在的{@link #SQL_ANNOTATION_TYPES}集合中的注解类型{@link Class}
     *     </li>
     *     <li>
     *         调用{@link #getSqlProviderAnnotationType(Method)}尝试获取{@code method}上可能存在的{@link #SQL_PROVIDER_ANNOTATION_TYPES}集合中的注解类型{@link Class}
     *     </li>
     *     <li>
     *         判断第一步获取到的注解类型 != null：
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>
     *                         判断第二步获取的注解类型 != null：
     *                         <ul>
     *                             <li>
     *                                 true：直接抛出冲突异常{@link BindingException}
     *                             </li>
     *                             <li>
     *                                 false：什么也不做，继续往下走
     *                             </li>
     *                         </ul>
     *                     </li>
     *                     <li>
     *                         调用{@code method}的{@link Method#getAnnotation(Class)}传入第一步获得的注解类型获取注解对象{@link Annotation}
     *                     </li>
     *                     <li>
     *                         直接调用该注解对象.getClass().getMethod("value")获得该注解对象的value()方法{@link Method}，然后调用{@link Method#invoke(Object, Object...)}传入该注解对象返回一个{@link Object}对象，然后再强转为{@link String}数组对象
     *                     </li>
     *                     <li>
     *                         调用{@link #buildSqlSourceFromStrings(String[], Class, LanguageDriver)}传入前一步获得的{@link String}数组对象、{@code parameterType}、{@code languageDriver}三个参数，构建一个{@link SqlSource}对象并return，本方法结束
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>
     *                 false：判断第二步获得的注解类型 != null：
     *                 <ul>
     *                     <li>
     *                         true：
     *                         <ol>
     *                             <li>
     *                                 调用{@code method}的{@link Method#getAnnotation(Class)}传入第二步获得的注解类型获取注解对象{@link Annotation}
     *                             </li>
     *                             <li>
     *                                 调用构造器{@link ProviderSqlSource#ProviderSqlSource(Configuration, Object, Class, Method)}传入{@link #assistant}的{@link MapperBuilderAssistant#getConfiguration()}、前一步得到的{@link Annotation}对象、{@link #type}、{@code method} 四个参数，构建一个{@link ProviderSqlSource}对象并return，本方法结束
     *                             </li>
     *                         </ol>
     *                     </li>
     *                     <li>
     *                         false：什么也不做，继续往下走
     *                     </li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         return null，本方法结束
     *     </li>
     *     <u><b>（注：整个方法内容都被try catch进行{@link Exception}捕获，然后抛出{@link BuilderException}异常）</b></u>
     * </ol>
     *
     * @param method
     * @param parameterType
     * @param languageDriver
     * @return
     */
    private SqlSource getSqlSourceFromAnnotations(Method method, Class<?> parameterType, LanguageDriver languageDriver) {
        try {
            Class<? extends Annotation> sqlAnnotationType = getSqlAnnotationType(method);
            Class<? extends Annotation> sqlProviderAnnotationType = getSqlProviderAnnotationType(method);
            if (sqlAnnotationType != null) {
                if (sqlProviderAnnotationType != null) {
                    throw new BindingException("You cannot supply both a static SQL and SqlProvider to method named " + method.getName());
                }
                Annotation sqlAnnotation = method.getAnnotation(sqlAnnotationType);
                final String[] strings = (String[]) sqlAnnotation.getClass().getMethod("value").invoke(sqlAnnotation);
                return buildSqlSourceFromStrings(strings, parameterType, languageDriver);
            } else if (sqlProviderAnnotationType != null) {
                Annotation sqlProviderAnnotation = method.getAnnotation(sqlProviderAnnotationType);
                return new ProviderSqlSource(assistant.getConfiguration(), sqlProviderAnnotation, type, method);
            }
            return null;
        } catch (Exception e) {
            throw new BuilderException("Could not find value method on SQL annotation.  Cause: " + e, e);
        }
    }


    /**
     *  创建 {@link SqlSource} 对象：
     *  <ol>
     *      <li>
     *          new 一个final 的{@link StringBuilder}对象，循环遍历{@code strings}数组，对于数组中每一个字符串元素，调用{@link StringBuilder#append(String)}当前元素然后再{@link StringBuilder#append(String)}一个空格" "到前面构建的{@link StringBuilder}对象中
     *      </li>
     *      <li>
     *          调用{@code languageDriver}的{@link LanguageDriver#createSqlSource(Configuration, String, Class)}（{@link org.apache.ibatis.scripting.xmltags.XMLLanguageDriver#createSqlSource(Configuration, String, Class)}）传入{@link #configuration}、前一步拼接后的{@link StringBuilder#toString()}.trim()、{@code parameterTypeClass} 三个参数，构建一个{@link SqlSource}对象并返回，本方法结束
     *      </li>
     *  </ol>
     *
     * @param strings
     * @param parameterTypeClass
     * @param languageDriver
     * @return
     */
    private SqlSource buildSqlSourceFromStrings(String[] strings, Class<?> parameterTypeClass, LanguageDriver languageDriver) {
        final StringBuilder sql = new StringBuilder();
        for (String fragment : strings) {
            sql.append(fragment);
            sql.append(" ");
        }
        return languageDriver.createSqlSource(configuration, sql.toString().trim(), parameterTypeClass);
    }

    /**
     * 获得方法对应的 SQL 命令类型：
     * <ol>
     *     <li>
     *         调用{@link #getSqlAnnotationType(Method)}传入{@code method}尝试获取方法上可能定义了的{@link #SQL_ANNOTATION_TYPES}中的任意一个注解类型{@link Class}对象并暂存
     *     </li>
     *     <li>
     *         判断前面获取的{@link Class}对象 == null：
     *         <ul>
     *             <li>
     *                 true：
     *                 <ol>
     *                     <li>调用{@link #getSqlProviderAnnotationType(Method)}传入{@code method}获取对应的"Provider"注解：如果获取到的注解类型对象是null，直接返回{@link SqlCommandType#UNKNOWN}，本方法结束；否则继续往下走</li>
     *                     <li>
     *                         如果前面获取的注解类型对象是{@link SelectProvider}.class则暂存{@link Select}.class、是{@link InsertProvider}.class则暂存{@link Insert}.class、是{@link UpdateProvider}.class则暂存{@link Update}.class、是{@link DeleteProvider}.class则暂存{@link Delete}.class
     *                     </li>
     *                 </ol>
     *             </li>
     *             <li>false：什么也不做，继续往下走</li>
     *         </ul>
     *     </li>
     *     <li>
     *         调用{@link SqlCommandType#valueOf(String)}传入前面所有步骤之后暂存到的注解类型对象{@link Class#getSimpleName()}.toUpperCase({@link Locale#ENGLISH})得到对应的{@link SqlCommandType}对象并return，方法结束
     *     </li>
     * </ol>
     *
     * @param method 方法
     * @return SQL 命令类型
     */
    private SqlCommandType getSqlCommandType(Method method) {
        Class<? extends Annotation> type = getSqlAnnotationType(method);

        if (type == null) {
            type = getSqlProviderAnnotationType(method);

            if (type == null) {
                return SqlCommandType.UNKNOWN;
            }

            if (type == SelectProvider.class) {
                type = Select.class;
            } else if (type == InsertProvider.class) {
                type = Insert.class;
            } else if (type == UpdateProvider.class) {
                type = Update.class;
            } else if (type == DeleteProvider.class) {
                type = Delete.class;
            }
        }

        return SqlCommandType.valueOf(type.getSimpleName().toUpperCase(Locale.ENGLISH));
    }

    /**
     * 调用{@link #chooseAnnotationType(Method, Set)}传入{@code method}和{@link #SQL_ANNOTATION_TYPES}（获取{@code method}上可能存在{@link #SQL_ANNOTATION_TYPES}集合中的注解的{@link Class}对象）
     *
     * @param method 方法
     * @return 查到的注解
     */
    private Class<? extends Annotation> getSqlAnnotationType(Method method) {
        return chooseAnnotationType(method, SQL_ANNOTATION_TYPES);
    }

    /**
     * 调用{@link #chooseAnnotationType(Method, Set)}传入{@code method}和{@link #SQL_PROVIDER_ANNOTATION_TYPES}（获取{@code method}上可能存在{@link #SQL_PROVIDER_ANNOTATION_TYPES}集合中的注解的{@link Class}对象）
     *
     * @param method 方法
     * @return 查到的注解
     */
    private Class<? extends Annotation> getSqlProviderAnnotationType(Method method) {
        return chooseAnnotationType(method, SQL_PROVIDER_ANNOTATION_TYPES);
    }

    /**
     * 获得方法上定义的指定注解类型：遍历迭代{@code types}，对于迭代每一个注解类型对象{@link Class}：
     * <ol>
     *     <li>
     *         调用{@code method}的{@link Method#getAnnotation(Class)}传入当前迭代的注解类型对象{@link Class}获取对应的注解对象{@link Annotation}：如果获取到的注解对象不是null则直接返回当前迭代的注解类型{@link Class}，本方法结束；否则继续进入循环下一迭代
     *     </li>
     * </ol>
     * 循环结束了本方法还没结束，直接return null
     *
     * @param method 方法
     * @param types 指定的注解类型
     * @return 查到的注解类型
     */
    private Class<? extends Annotation> chooseAnnotationType(Method method, Set<Class<? extends Annotation>> types) {
        for (Class<? extends Annotation> type : types) {
            Annotation annotation = method.getAnnotation(type);
            if (annotation != null) {
                return type;
            }
        }
        return null;
    }

    /**
     *  将{@link Results}注解中的{@link Result}[]注解数组，解析成对应的 {@link ResultMapping} 对象们，并添加到 {@code resultMappings} 中。和 {@link XMLMapperBuilder#buildResultMappingFromContext(XNode, Class, List)}  方法是一致的
     *  <ol>
     *      遍历迭代{@code results}数组，对于每一个迭代的{@link Result}对象：
     *      <li>
     *          new 一个泛型为{@link ResultFlag}的{@link ArrayList}对象，然后判断{@link Result#id()}是否为true：为true则添加{@link ResultFlag#ID}到该集合对象中；否则什么也不做，继续往下走
     *      </li>
     *      <li>获取{@link Result#typeHandler()}并判断其是否 == {@link UnknownTypeHandler}.class：是则暂存null到"typeHandler"变量；否则暂存取{@link Result#typeHandler()}到"typeHandler"变量</li>
     *      <li>
     *          调用{@link #assistant}的{@link MapperBuilderAssistant#buildResultMapping(Class, String, String, Class, JdbcType, String, String, String, String, Class, List, String, String, boolean)}传入：
     *          <ol>
     *              <li>{@code resultType}</li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Result#property()}</li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Result#column()} </li>
     *              <li>{@link Result#javaType()} == void.class ? null : {@link Result#javaType()}</li>
     *              <li>{@link Result#jdbcType()} == {@link JdbcType#UNDEFINED} ? null : {@link Result#jdbcType()}</li>
     *              <li>调用{@link #hasNestedSelect(Result)}传入当前迭代的{@link Result}对象判断是否含有nested select：有则传入{@link #nestedSelectId(Result)}传入当前迭代的{@link Result}对象的结果；否则传入null </li>
     *              <li>null</li>
     *              <li>null</li>
     *              <li>null</li>
     *              <li>第二步暂存的"typeHandler"变量</li>
     *              <li>第一步构建好的{@link ArrayList}对象</li>
     *              <li>null</li>
     *              <li>null</li>
     *              <li>调用{@link #isLazy(Result)}传入当前迭代的{@link Result}注解对象</li>
     *          </ol>
     *          共14个参数构建一个{@link ResultMapping}对象
     *      </li>
     *      <li>将前一步构建的{@link ResultMapping}对象添加到{@code resultMappings}中，然后进入下一迭代，直至循环结束，方法结束</li>
     *  </ol>
     *
     * @param results {@link Results#value()}
     * @param resultType
     * @param resultMappings 承载{@link ResultMapping}对象的集合
     */
    private void applyResults(Result[] results, Class<?> resultType, List<ResultMapping> resultMappings) {
        for (Result result : results) {
            List<ResultFlag> flags = new ArrayList<>();
            if (result.id()) {
                flags.add(ResultFlag.ID);
            }
            @SuppressWarnings("unchecked")
            Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>)
                    ((result.typeHandler() == UnknownTypeHandler.class) ? null : result.typeHandler());
            ResultMapping resultMapping = assistant.buildResultMapping(
                    resultType,
                    nullOrEmpty(result.property()),
                    nullOrEmpty(result.column()),
                    result.javaType() == void.class ? null : result.javaType(),
                    result.jdbcType() == JdbcType.UNDEFINED ? null : result.jdbcType(),
                    hasNestedSelect(result) ? nestedSelectId(result) : null,
                    null,
                    null,
                    null,
                    typeHandler,
                    flags,
                    null,
                    null,
                    isLazy(result));
            resultMappings.add(resultMapping);
        }
    }

    /**
     * 获得内嵌的查询编号：
     * <ol>
     *     <li>
     *         调用{@code result}的{@link Result#one()}尝试获取{@code result}对象中定义了的{@link One}注解对象，然后调用{@link One#select()}获取"nested select id"，判断该id长度length() < 1：
     *         <ul>
     *             <li>true：{@link Result}中没有定义{@link One}使用的是默认的{@link One}对象（默认的{@link One}对象中的默认{@link One#select()}是空字符串）或者定义了{@link One}注解但是没有定义{@link One#select()}，获取{@link Result#many()}作为"nested select id"覆盖前面的值</li>
     *             <li>false：什么也不做，继续往下走</li>
     *         </ul>
     *     </li>
     *     <li>
     *         判断前面获得的"nested select id"是否contains(".")
     *         <ul>
     *             <li>是：调用{@link #type}的{@link Class#getName()}拼接"."拼接该"nested select id"作为新的"nested select id"（绝对id）</li>
     *             <li>否：什么也不做，继续往下走</li>
     *         </ul>
     *     </li>
     *     <li>return 经过前面所有步骤处理之后的"nested select id"，方法结束</li>
     * </ol>
     *
     * @param result {@link Result} 注解
     * @return 查询编号
     */
    private String nestedSelectId(Result result) {
        String nestedSelect = result.one().select();
        if (nestedSelect.length() < 1) {
            nestedSelect = result.many().select();
        }
        if (!nestedSelect.contains(".")) {
            nestedSelect = type.getName() + "." + nestedSelect;
        }
        return nestedSelect;
    }

    /**
     * 判断是否懒加载（判断{@link Result}注解中的{@link One}或者{@link Many}中是否定义了{@link FetchType#LAZY}）
     * <ol>
     *     <li>
     *         调用{@link #configuration}的{@link Configuration#isLazyLoadingEnabled()}暂存到变量"isLazy"
     *     </li>
     *     <li>
     *         如果{@code result}的{@link Result#one()}的{@link One#select()}.length() > 0 并且 {@link FetchType#DEFAULT} != {@code result}的{@link Result#one()}的{@link One#fetchType()}
     *         <ul>
     *             <li>true：调用{@code result}的{@link Result#one()}的{@link One#fetchType()}：如果得到的结果 == {@link FetchType#LAZY}，覆盖true到前面的变量"isLazy"；否则覆盖false到前面的变量"isLazy"</li>
     *             <li>
     *                 false：判断  {@code result}的{@link Result#many()}的{@link Many#select()}.length() > 0 并且 {@link FetchType#DEFAULT} != {@code result}的{@link Result#many()}的{@link Many#fetchType()}
     *                 <ul>
     *                     <li>true：调用{@code result}的{@link Result#one()}的{@link One#fetchType()}：如果得到的结果 == {@link FetchType#LAZY}，覆盖true到前面的变量"isLazy"；否则覆盖false到前面的变量"isLazy"</li>
     *                     <li>false：什么也不做，继续往下走</li>
     *                 </ul>
     *             </li>
     *         </ul>
     *     </li>
     *     <li>return 第一步的变量"isLazy"，本方法结束</li>
     * </ol>
     *
     * @param result {@link Result} 注解
     * @return 是否懒加载
     */
    private boolean isLazy(Result result) {
        boolean isLazy = configuration.isLazyLoadingEnabled();
        if (result.one().select().length() > 0 && FetchType.DEFAULT != result.one().fetchType()) {
            isLazy = result.one().fetchType() == FetchType.LAZY;
        } else if (result.many().select().length() > 0 && FetchType.DEFAULT != result.many().fetchType()) {
            isLazy = result.many().fetchType() == FetchType.LAZY;
        }
        return isLazy;
    }

    /**
     * 判断是否有内嵌的查询：
     * <ol>
     *     <li>
     *         判断{@code result}的{@link Result#one()}的{@link One#select()}.length() > 0 && {@code result}的{@link Result#many()}的{@link Many#select()}.length() > 0
     *         <ul>
     *             <li>true：直接抛出异常{@link BuilderException}（不能在{@link Result}里面同时定义{@link One}和{@link Many}）</li>
     *             <li>false：什么也不做，继续往下走</li>
     *         </ul>
     *     </li>
     *     <li>return {@code result}的{@link Result#one()}的{@link One#select()}.length() > 0 || {@code result}的{@link Result#many()}的{@link Many#select()}.length() > 0（在{@link Result}中定义了{@link One}或者{@link Many}）</li>
     * </ol>
     *
     * @param result {@link Result} 注解
     * @return 是否有内嵌的查询
     */
    private boolean hasNestedSelect(Result result) {
        if (result.one().select().length() > 0 && result.many().select().length() > 0) {
            throw new BuilderException("Cannot use both @One and @Many annotations in the same @Result");
        }
        return result.one().select().length() > 0 || result.many().select().length() > 0;
    }

    /**
     *  将{@link ConstructorArgs}注解中的{@link Arg}[]注解数组，解析成对应的 {@link ResultMapping} 对象们，并添加到 {@code resultMappings} 中。和 {@link XMLMapperBuilder#processConstructorElement(XNode, Class, List)} 方法是一致的
     *  <ol>
     *      遍历迭代{@code args}数组，对于每一个迭代的{@link Arg}对象：
     *      <li>
     *          new 一个泛型为{@link ResultFlag}的{@link ArrayList}对象，然后添加{@link ResultFlag#CONSTRUCTOR}到该集合对象中，然后再判断{@link Arg#id()}是否为true：为true则添加{@link ResultFlag#ID}到该集合对象中；否则什么也不做，继续往下走
     *      </li>
     *      <li>获取{@link Arg#typeHandler()}并判断其是否 == {@link UnknownTypeHandler}.class：是则暂存null到"typeHandler"变量；否则暂存取{@link Arg#typeHandler()}到"typeHandler"变量</li>
     *      <li>
     *          调用{@link #assistant}的{@link MapperBuilderAssistant#buildResultMapping(Class, String, String, Class, JdbcType, String, String, String, String, Class, List, String, String, boolean)}传入：
     *          <ol>
     *              <li>{@code resultType}</li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Arg#name()}</li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Arg#column()} </li>
     *              <li>{@link Arg#javaType()} == void.class ? null : {@link Arg#javaType()}</li>
     *              <li>{@link Arg#jdbcType()} == {@link JdbcType#UNDEFINED} ? null : {@link Arg#jdbcType()}</li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Arg#select()} </li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Arg#resultMap()} </li>
     *              <li>null</li>
     *              <li>经过{@link #nullOrEmpty(String)}处理之后的{@link Arg#columnPrefix()} </li>
     *              <li>第二步暂存的"typeHandler"变量</li>
     *              <li>第一步构建好的{@link ArrayList}对象</li>
     *              <li>null</li>
     *              <li>null</li>
     *              <li>false</li>
     *          </ol>
     *          共14个参数构建一个{@link ResultMapping}对象
     *      </li>
     *      <li>将前一步构建的{@link ResultMapping}对象添加到{@code resultMappings}中，然后进入下一迭代，直至循环结束，方法结束</li>
     *  </ol>
     *
     * @param args {@link ConstructorArgs#value()}
     * @param resultType
     * @param resultMappings 承载{@link ResultMapping}对象的集合
     */
    private void applyConstructorArgs(Arg[] args, Class<?> resultType, List<ResultMapping> resultMappings) {
        for (Arg arg : args) {
            List<ResultFlag> flags = new ArrayList<>();
            flags.add(ResultFlag.CONSTRUCTOR);
            if (arg.id()) {
                flags.add(ResultFlag.ID);
            }
            @SuppressWarnings("unchecked")
            Class<? extends TypeHandler<?>> typeHandler = (Class<? extends TypeHandler<?>>)
                    (arg.typeHandler() == UnknownTypeHandler.class ? null : arg.typeHandler());
            ResultMapping resultMapping = assistant.buildResultMapping(
                    resultType,
                    nullOrEmpty(arg.name()),
                    nullOrEmpty(arg.column()),
                    arg.javaType() == void.class ? null : arg.javaType(),
                    arg.jdbcType() == JdbcType.UNDEFINED ? null : arg.jdbcType(),
                    nullOrEmpty(arg.select()),
                    nullOrEmpty(arg.resultMap()),
                    null,
                    nullOrEmpty(arg.columnPrefix()),
                    typeHandler,
                    flags,
                    null,
                    null,
                    false);
            resultMappings.add(resultMapping);
        }
    }

    /**
     * 如果{@code value} == null 或者 {@code value}.trim().length() == 0 则 return null；否则return {@code value}
     *
     * @param value
     * @return
     */
    private String nullOrEmpty(String value) {
        return value == null || value.trim().length() == 0 ? null : value;
    }

    /**
     * 如果{@code results}是null则return 一个新new的长度为0的{@link Result}类型的空数组；否则return {@link Results#value()}
     *
     * @param results {@link Results}注解对象
     * @return
     */
    private Result[] resultsIf(Results results) {
        return results == null ? new Result[0] : results.value();
    }

    /**
     * 如果{@code args}是null则return 一个新new的长度为0的{@link Arg}类型的空数组；否则return {@link ConstructorArgs#value()}
     *
     * @param args {@link ConstructorArgs}注解对象
     * @return
     */
    private Arg[] argsIf(ConstructorArgs args) {
        return args == null ? new Arg[0] : args.value();
    }


    /**
     * 解析{@link SelectKey}为一个{@link SelectKeyGenerator}对象并返回：
     * <ol>
     *     <li>
     *         拼接{@code baseStatementId}+{@link SelectKeyGenerator#SELECT_KEY_SUFFIX}作为当前{@link SelectKeyGenerator}的id
     *     </li>
     *     <li>
     *         调用{@link #buildSqlSourceFromStrings(String[], Class, LanguageDriver)}传入 {@link SelectKey#statement()}、{@code parameterTypeClass}、{@code languageDriver} 三个参数创建当前{@link SelectKeyGenerator}的{@link SqlSource}对象
     *     </li>
     *     <li>
     *         调用{@link #assistant}的{@link MapperBuilderAssistant#addMappedStatement(String, SqlSource, StatementType, SqlCommandType, Integer, Integer, String, Class, String, Class, ResultSetType, boolean, boolean, boolean, KeyGenerator, String, String, String, LanguageDriver, String)}传入：
     *         <ol>
     *             <li>第一步拼接的id</li>
     *             <li>第二步获得的{@link SqlSource}对象</li>
     *             <li>{@link SelectKey#statementType()}</li>
     *             <li>{@link SqlCommandType#SELECT}</li>
     *             <li>null</li>
     *             <li>null</li>
     *             <li>null</li>
     *             <li>{@code parameterTypeClass}</li>
     *             <li>null</li>
     *             <li>{@link SelectKey#resultType()}</li>
     *             <li>null</li>
     *             <li>false</li>
     *             <li>false</li>
     *             <li>false</li>
     *             <li>{@link NoKeyGenerator#INSTANCE}</li>
     *             <li>{@link SelectKey#keyProperty()}</li>
     *             <li>{@link SelectKey#keyColumn()}</li>
     *             <li>null</li>
     *             <li>{@code languageDriver}</li>
     *             <li>null</li>
     *         </ol>
     *         共20个参数构建一个{@link MappedStatement}对象并缓存到{@link Configuration}
     *     </li>
     *     <li>调用{@link Configuration#getMappedStatement(String, boolean)}传入第一步拼接的id和false获取刚构建的{@link MappedStatement}对象</li>
     *     <li>调用构造器{@link SelectKeyGenerator#SelectKeyGenerator(MappedStatement, boolean)}传入获取到的{@link MappedStatement}对象和{@link SelectKey#before()}构建一个{@link SelectKeyGenerator}对象</li>
     *     <li>调用{@link Configuration#addKeyGenerator(String, KeyGenerator)}传入第一步拼接的id和构建好的{@link SelectKeyGenerator}对象将其缓存到{@link Configuration}中</li>
     *     <li>return {@link SelectKeyGenerator}对象，方法结束</li>
     * </ol>
     *
     * @param selectKeyAnnotation
     * @param baseStatementId
     * @param parameterTypeClass
     * @param languageDriver
     * @return
     */
    private KeyGenerator handleSelectKeyAnnotation(SelectKey selectKeyAnnotation, String baseStatementId, Class<?> parameterTypeClass, LanguageDriver languageDriver) {
        String id = baseStatementId + SelectKeyGenerator.SELECT_KEY_SUFFIX;
        Class<?> resultTypeClass = selectKeyAnnotation.resultType();
        StatementType statementType = selectKeyAnnotation.statementType();
        String keyProperty = selectKeyAnnotation.keyProperty();
        String keyColumn = selectKeyAnnotation.keyColumn();
        boolean executeBefore = selectKeyAnnotation.before();

        // defaults
        boolean useCache = false;
        KeyGenerator keyGenerator = NoKeyGenerator.INSTANCE;
        Integer fetchSize = null;
        Integer timeout = null;
        boolean flushCache = false;
        String parameterMap = null;
        String resultMap = null;
        ResultSetType resultSetTypeEnum = null;

        SqlSource sqlSource = buildSqlSourceFromStrings(selectKeyAnnotation.statement(), parameterTypeClass, languageDriver);
        SqlCommandType sqlCommandType = SqlCommandType.SELECT;

        assistant.addMappedStatement(id, sqlSource, statementType, sqlCommandType, fetchSize, timeout, parameterMap, parameterTypeClass, resultMap, resultTypeClass, resultSetTypeEnum,
                flushCache, useCache, false,
                keyGenerator, keyProperty, keyColumn, null, languageDriver, null);

        // 获得 SelectKeyGenerator 的编号，格式为 `${namespace}.${id}`
        id = assistant.applyCurrentNamespace(id, false);
        MappedStatement keyStatement = configuration.getMappedStatement(id, false);
        SelectKeyGenerator answer = new SelectKeyGenerator(keyStatement, executeBefore);
        configuration.addKeyGenerator(id, answer);
        return answer;
    }

}
