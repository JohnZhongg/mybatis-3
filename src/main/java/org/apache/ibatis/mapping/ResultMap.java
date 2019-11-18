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
package org.apache.ibatis.mapping;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;
import org.apache.ibatis.reflection.ParamNameUtil;
import org.apache.ibatis.session.Configuration;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * 标签{@code <resultMap/>}一对一的对象
 *
 * @author Clinton Begin
 */
public class ResultMap {

    /**
     * {@link Configuration} 对象
     */
    private Configuration configuration;

    /**
     * 唯一标识
     */
    private String id;
    /**
     * 对应的{@link Class}对象，{@link ResultMapping}的承载类
     */
    private Class<?> type;
    /**
     * 当前{@link ResultMap}对象的所有{@link ResultMapping}对象集合，包含下面的{@link #idResultMappings}
     */
    private List<ResultMapping> resultMappings;
    /**
     * id{@link ResultMapping}对象集合(由{@code <id/>}标签和{@code <idArg/>}标签构建出来的{@link ResultMapping}对象)
     */
    private List<ResultMapping> idResultMappings;
    /**
     * 构造方法参数的{@link ResultMapping}集合（使用{@code <constructor/>}包起来的标签，和{@link #propertyResultMappings}互斥）
     */
    private List<ResultMapping> constructorResultMappings;
    /**
     * 属性的{@link ResultMapping}集合（未使用{@code <constructor/>}包起来的标签，和{@link #constructorResultMappings}互斥）
     */
    private List<ResultMapping> propertyResultMappings;
    /**
     * 所有映射的数据库的字段集合
     */
    private Set<String> mappedColumns;
    /**
     * 所有映射的Java对象的属性集合（无论是id或者非id的、构造器参数或者对象属性指向的java端字段映射名称都包含在里面）
     */
    private Set<String> mappedProperties;
    /**
     * Discriminator 对象
     */
    private Discriminator discriminator;
    /**
     * 是否有内嵌的 ResultMap
     */
    private boolean hasNestedResultMaps;
    /**
     * 是否有内嵌的查询
     */
    private boolean hasNestedQueries;
    /**
     * 是否开启自动匹配
     *
     * 如果设置这个属性，MyBatis将会为这个ResultMap开启或者关闭自动映射。这个属性会覆盖全局的属性 autoMappingBehavior。默认值为：unset。
     */
    private Boolean autoMapping;

    private ResultMap() {
    }

    public static class Builder {

        private static final Log log = LogFactory.getLog(Builder.class);

        private ResultMap resultMap = new ResultMap();

        /**
         * 调用{@link Builder#Builder(Configuration, String, Class, List, Boolean)}传入{@code configuration}、{@code id}、{@code type}、{@code resultMappings}、null
         *
         * @param configuration
         * @param id
         * @param type
         * @param resultMappings
         */
        public Builder(Configuration configuration, String id, Class<?> type, List<ResultMapping> resultMappings) {
            this(configuration, id, type, resultMappings, null);
        }

        /**
         * 设置{@code configuration}、{@code id}、{@code type}、{@code resultMappings}、{@code autoMapping}到内部成员变量{@link #resultMap}的{@link ResultMap#configuration}、{@link ResultMap#id}、{@link ResultMap#type}、{@link ResultMap#resultMappings}、{@link ResultMap#autoMapping}
         *
         * @param configuration
         * @param id
         * @param type
         * @param resultMappings
         * @param autoMapping
         */
        public Builder(Configuration configuration, String id, Class<?> type, List<ResultMapping> resultMappings, Boolean autoMapping) {
            resultMap.configuration = configuration;
            resultMap.id = id;
            resultMap.type = type;
            resultMap.resultMappings = resultMappings;
            resultMap.autoMapping = autoMapping;
        }

        /**
         * 设置{@code discriminator}给内部成员变量{@link #resultMap}的{@link ResultMap#discriminator}
         *
         * @param discriminator
         * @return
         */
        public Builder discriminator(Discriminator discriminator) {
            resultMap.discriminator = discriminator;
            return this;
        }

        public Class<?> type() {
            return resultMap.type;
        }

        /**
         * 构造{@link ResultMap}对象：
         * <ol>
         *     <li>
         *         如果成员变量{@link #resultMap}的{@link ResultMap#id}是null，直接抛出异常
         *     </li>
         *     <li>
         *         分别初始化成员变量{@link #resultMap}的{@link ResultMap#mappedColumns}、{@link ResultMap#mappedProperties}为{@link HashSet}；{@link ResultMap#idResultMappings}、{@link ResultMap#constructorResultMappings}、{@link ResultMap#propertyResultMappings}为{@link ArrayList}
         *     </li>
         *     <li>
         *         遍历成员变量{@link #resultMap}的{@link ResultMap#resultMappings}：
         *         <ol>
         *             <li>
         *                 "成员变量{@link #resultMap}的{@link ResultMap#hasNestedQueries} || 当前{@link ResultMapping#getNestedQueryId()} != null" 设置给成员变量{@link #resultMap}的{@link ResultMap#hasNestedQueries}<br>
         *                 "成员变量{@link #resultMap}的{@link ResultMap#hasNestedResultMaps} || (当前{@link ResultMapping#getNestedResultMapId()} != null && 当前{@link ResultMapping#getResultSet()} == null)" 设置给成员变量{@link #resultMap}的{@link ResultMap#hasNestedResultMaps}
         *             </li>
         *             <li>
         *                 获取当前{@link ResultMapping#getColumn()}，如果获取到的值不是null：
         *                 <ul>
         *                     <li>
         *                         不是null则将该值转换成大写之后添加到{@link ResultMap#mappedColumns}
         *                     </li>
         *                     <li>
         *                         如果是null则判断{@link ResultMapping#isCompositeResult()}：
         *                         <ul>
         *                             <li>
         *                                 如果为true：则遍历{@link ResultMapping#getComposites()}作为一个composite的{@link ResultMapping}对象然后调用{@link ResultMapping#getColumn()}获取composite的column值，不为null的值全部转为大写添加到{@link ResultMap#mappedColumns}
         *                             </li>
         *                             <li>
         *                                 为false就什么都不做
         *                             </li>
         *                         </ul>
         *                     </li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 通过{@link ResultMapping#getProperty()}获取property，如果不为null则添加到{@link ResultMap#mappedProperties}
         *             </li>
         *             <li>
         *                 调用{@link ResultMapping#getFlags()}.contains({@link ResultFlag#CONSTRUCTOR})来判断当前{@link ResultMapping}对象是否打了{@code <constructor/>}的标：
         *                 <ul>
         *                     <li>
         *                         如果打了{@code <constructor/>}的标，则将当前{@link ResultMapping}对象添加到{@link ResultMap#constructorResultMappings}中，然后如果{@link ResultMapping#getProperty()}不为null则收集到一个容器中
         *                     </li>
         *                     <li>
         *                         如果没打，则将当前{@link ResultMapping}对象添加到{@link ResultMap#propertyResultMappings}中
         *                     </li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 调用{@link ResultMapping#getFlags()}.contains({@link ResultFlag#ID})来判断当前{@link ResultMapping}对象是否打了{@code <idArg/>}的标：如果打了就添加到{@link ResultMap#idResultMappings}；否则不做任何动作
         *             </li>
         *         </ol>
         *     </li>
         *     <li>
         *         判断{@link ResultMap#idResultMappings}是否为empty：如果将{@link ResultMap#resultMappings}的所有元素都添加到{@link ResultMap#idResultMappings}中；否则什么也不做
         *     </li>
         *     <li>
         *         判断前面3.4步骤中收集到的经过{@code <constructor/>}打标的{@link ResultMapping}的property容器是否为空：
         *         <ul>
         *             <li>
         *                 不为空则调用{@link #argNamesOfMatchingConstructor(List)}传入该容器寻找所有参数都在和该property容器内的参数名称完全相等，且其所有参数类型和容器内对应的{@link ResultMapping}的javaType都相等的构造函数的真实参数列表（包含{@link Param}中定义的名称），然后将{@link ResultMap#constructorResultMappings}的这些{@link ResultMapping}按照返回的参数列表的索引为止进行排序
         *             </li>
         *         </ul>
         *     </li>
         *     <li>
         *         最后将{@link ResultMap#resultMappings}、{@link ResultMap#idResultMappings}、{@link ResultMap#constructorResultMappings}、{@link ResultMap#propertyResultMappings}、{@link ResultMap#mappedColumns}通过{@link Collections#unmodifiableList(List)}同一转换成不可修改的集合
         *     </li>
         *     <li>
         *         返回成员变量{@link #resultMap}
         *     </li>
         * </ol>
         *
         * @return ResultMap 对象
         */
        public ResultMap build() {
            if (resultMap.id == null) {
                throw new IllegalArgumentException("ResultMaps must have an id");
            }
            resultMap.mappedColumns = new HashSet<>();
            resultMap.mappedProperties = new HashSet<>();
            resultMap.idResultMappings = new ArrayList<>();
            resultMap.constructorResultMappings = new ArrayList<>();
            resultMap.propertyResultMappings = new ArrayList<>();
            final List<String> constructorArgNames = new ArrayList<>();
            for (ResultMapping resultMapping : resultMap.resultMappings) {
                resultMap.hasNestedQueries = resultMap.hasNestedQueries || resultMapping.getNestedQueryId() != null;
                resultMap.hasNestedResultMaps = resultMap.hasNestedResultMaps || (resultMapping.getNestedResultMapId() != null && resultMapping.getResultSet() == null);
                final String column = resultMapping.getColumn();
                if (column != null) {
                    resultMap.mappedColumns.add(column.toUpperCase(Locale.ENGLISH));
                } else if (resultMapping.isCompositeResult()) {
                    for (ResultMapping compositeResultMapping : resultMapping.getComposites()) {
                        final String compositeColumn = compositeResultMapping.getColumn();
                        if (compositeColumn != null) {
                            resultMap.mappedColumns.add(compositeColumn.toUpperCase(Locale.ENGLISH));
                        }
                    }
                }
                final String property = resultMapping.getProperty();
                if (property != null) {
                    resultMap.mappedProperties.add(property);
                }
                if (resultMapping.getFlags().contains(ResultFlag.CONSTRUCTOR)) {
                    resultMap.constructorResultMappings.add(resultMapping);
                    if (resultMapping.getProperty() != null) {
                        constructorArgNames.add(resultMapping.getProperty());
                    }
                } else {
                    resultMap.propertyResultMappings.add(resultMapping);
                }
                if (resultMapping.getFlags().contains(ResultFlag.ID)) {
                    resultMap.idResultMappings.add(resultMapping);
                }
            }
            if (resultMap.idResultMappings.isEmpty()) {
                resultMap.idResultMappings.addAll(resultMap.resultMappings);
            }
            if (!constructorArgNames.isEmpty()) {
                final List<String> actualArgNames = argNamesOfMatchingConstructor(constructorArgNames);
                if (actualArgNames == null) {
                    throw new BuilderException("Error in result map '" + resultMap.id
                            + "'. Failed to find a constructor in '"
                            + resultMap.getType().getName() + "' by arg names " + constructorArgNames
                            + ". There might be more info in debug log.");
                }
                resultMap.constructorResultMappings.sort((o1, o2) -> {
                    int paramIdx1 = actualArgNames.indexOf(o1.getProperty());
                    int paramIdx2 = actualArgNames.indexOf(o2.getProperty());
                    return paramIdx1 - paramIdx2;
                });
            }
            resultMap.resultMappings = Collections.unmodifiableList(resultMap.resultMappings);
            resultMap.idResultMappings = Collections.unmodifiableList(resultMap.idResultMappings);
            resultMap.constructorResultMappings = Collections.unmodifiableList(resultMap.constructorResultMappings);
            resultMap.propertyResultMappings = Collections.unmodifiableList(resultMap.propertyResultMappings);
            resultMap.mappedColumns = Collections.unmodifiableSet(resultMap.mappedColumns);
            return resultMap;
        }

        /**
         * 传入当前{@link ResultMap}中声明的{@code <constructor/>}包裹的{@code <idArg/>和<arg/>}标签的"name"属性值集合，在当前{@link ResultMap#type}所有构造函数中寻找其所有参数名和{@code <idArg/>和<arg/>}标签中定义的所有参数名都相等并且类型匹配的构造函数：
         * <ol>
         *     <li>
         *         调用成员变量{@link #resultMap}.type.getDeclaredConstructors()获取当前映射集合承载类{@link ResultMap#type}中声明的所有{@link Constructor}对象集合
         *     </li>
         *     <li>
         *         遍历{@link Constructor}对象集合：
         *         <ol>
         *             <li>
         *                 通过{@link Constructor#getParameterTypes()}获取当前迭代的构造函数的所有参数类型数组，判断{@code constructorArgNames}的长度是否和前面获得的数组长度相等：
         *                 <ul>
         *                     <li>
         *                         相等则调用{@link #getArgNames(Constructor)}获取当前迭代构造函数的参数名称列表（包含{@link Param}声明的），然后判断{@code constructorArgNames}是否包含了获取到的所有参数名并且类型匹配（调用{@link #argTypesMatch(List, Class[], List)}传入{@code constructorArgNames}、上面获取到的构造函数参数类型数组、前面获取到的构造函数参数名称列表 3个参数检查类型是否都匹配）：
         *                         <ul>
         *                             <li>
         *                                 如果当前构造函数的所有参数名都被{@code constructorArgNames}包含并且类型匹配，则返回上面获取到的当前迭代构造函数的参数名称列表
         *                             </li>
         *                             <li>
         *                                 否则什么都不做，进入下一个构造函数迭代
         *                             </li>
         *                         </ul>
         *                     </li>
         *                     <li>
         *                         不等则什么都不做，进入下一个构造函数迭代
         *                     </li>
         *                 </ul>
         *             </li>
         *         </ol>
         *     </li>
         *     <li>
         *         如果遍历完了之后当前方法还没有返回，则返回null表示没有找到合适的构造函数
         *     </li>
         * </ol>
         *
         * @param constructorArgNames 当前{@link ResultMap}中声明的{@code <constructor/>}包裹的{@code <idArg/>和<arg/>}标签的"name"属性值集合
         * @return
         */
        private List<String> argNamesOfMatchingConstructor(List<String> constructorArgNames) {
            Constructor<?>[] constructors = resultMap.type.getDeclaredConstructors();
            for (Constructor<?> constructor : constructors) {
                Class<?>[] paramTypes = constructor.getParameterTypes();
                if (constructorArgNames.size() == paramTypes.length) {
                    List<String> paramNames = getArgNames(constructor);
                    if (constructorArgNames.containsAll(paramNames) && argTypesMatch(constructorArgNames, paramTypes, paramNames)) {
                        return paramNames;
                    }
                }
            }
            return null;
        }

        /**
         * 判断在{@code <constructor/>}中声明的所有{@link ResultMapping}对象的"javaType"是否和传入的{@code paramTypes}按位一致：
         * <ol>
         *      <li>
         *          遍历传入的{@code constructorArgNames}：
         *          <ol>
         *              <li>
         *                  记录当前遍历index为{@code <constructor/>}中声明的参数index，然后取当前遍历的元素（{@code <constructor/>}标签所包裹的元素的name属性，即在xml文件中进行声明的构造函数的参数名）作为{@code paramNames}的元素取其在{@code paramNames}中对应的index，该index为一个真实的构造函数对象的参数列表的index，在{@code paramTypes}中获取该index对应的java类型
         *              </li>
         *              <li>
         *                  取上面记录的{@code <constructor/>}中声明的参数index作为{@link #resultMap}.constructorResultMappings.get(index)获取到对应的{@link ResultMapping}对象然后调用{@link ResultMapping#getJavaType()}获取到在{@code <constructor/>}标签中声明的"javaType"
         *              </li>
         *              <li>
         *                  至此，上面分别获取到了在{@code <constructor/>}中声明的当前参数的声明的java类型和实际的java类型，对两者调用equals方法进行比较，只要遇到不想等的就返回false，否则继续遍历迭代，直到循环结束
         *              </li>
         *          </ol>
         *      </li>
         *      <li>
         *          遍历结束方法都没有返回则返回true
         *      </li>
         * </ol>
         *
         * @param constructorArgNames 被{@code <constructor/>}标签声明的构造方法参数名列表
         * @param paramTypes 实际的一个构造方法的参数类型列表
         * @param paramNames 实际的一个构造方法的参数名列表
         * @return 是否符合
         */
        private boolean argTypesMatch(final List<String> constructorArgNames,
                                      Class<?>[] paramTypes, List<String> paramNames) {
            for (int i = 0; i < constructorArgNames.size(); i++) {
                Class<?> actualType = paramTypes[paramNames.indexOf(constructorArgNames.get(i))];
                Class<?> specifiedType = resultMap.constructorResultMappings.get(i).getJavaType();
                if (!actualType.equals(specifiedType)) {
                    if (log.isDebugEnabled()) {
                        log.debug("While building result map '" + resultMap.id
                                + "', found a constructor with arg names " + constructorArgNames
                                + ", but the type of '" + constructorArgNames.get(i)
                                + "' did not match. Specified: [" + specifiedType.getName() + "] Declared: ["
                                + actualType.getName() + "]");
                    }
                    return false;
                }
            }
            return true;
        }

        /**
         * 获得传入的构造方法{@code constructor}的所有参数名：
         * <ol>
         *     <li>
         *         先使用{@link Constructor#getParameterAnnotations()}获取传入构造函数上的所有参数的所有注解，返回结果是一个二维数组，无论有没有注解，数组的第一维长度绝对和参数数量相等，如果其中的一个参数没有注解，则对应的第一维数组的元素是一个长度为0的空数组
         *     </li>
         *     <li>
         *         遍历第一维数组（对应构造函数的参数列表）：
         *         <lo>
         *             <li>
         *                 遍历当前第一维数组元素对应的第二维数组（当前参数的注解列表），如果当前注解 {@code annotation instanceof }{@link Param} 则将使用({@link Param}annotation)将该注解强制转换为{@link Param}类型，然后调用{@link Param#value()}获取注解中设置的参数名 然后break
         *             </li>
         *             <li>
         *                 如果上面的步骤没有获取注解名称，则判断成员变量{@link #resultMap}的{@link ResultMap#configuration}的{@link Configuration#isUseActualParamName()}是否true：
         *                 <ul>
         *                     <li>
         *                         如果是则判断之前是否已经取到了该构造函数的真实参数名称列表：如果取到了就按照当前参数的index按位获取改参数名称；否则就调用{@link ParamNameUtil#getParamNames(Constructor)}传入该构造函数获取其真实参数名称列表之后再取
         *                     </li>
         *                     <li>
         *                         如果否就什么都不做
         *                     </li>
         *                 </ul>
         *             </li>
         *             <li>
         *                 如果上面的步骤都还获得不到参数名，则使用"arg"拼接当前参数的index作为当前参数的参数名
         *             </li>
         *             <li>
         *                 将收集到的参数名列表进行返回（肯定非null，要么是长度位0的空列表）
         *             </li>
         *         </lo>
         *     </li>
         * </ol>
         *
         * @param constructor 构造方法
         * @return 参数名数组
         */
        private List<String> getArgNames(Constructor<?> constructor) {
            List<String> paramNames = new ArrayList<>();
            List<String> actualParamNames = null;
            final Annotation[][] paramAnnotations = constructor.getParameterAnnotations();
            int paramCount = paramAnnotations.length;
            for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
                String name = null;
                for (Annotation annotation : paramAnnotations[paramIndex]) {
                    if (annotation instanceof Param) {
                        name = ((Param) annotation).value();
                        break;
                    }
                }
                if (name == null && resultMap.configuration.isUseActualParamName()) {
                    if (actualParamNames == null) {
                        actualParamNames = ParamNameUtil.getParamNames(constructor);
                    }
                    //暂时不明白这里为什么还要做一个这样的判断，理论上这个不是一定成立的么
                    if (actualParamNames.size() > paramIndex) {
                        name = actualParamNames.get(paramIndex);
                    }
                }
                paramNames.add(name != null ? name : "arg" + paramIndex);
            }
            return paramNames;
        }
    }

    public String getId() {
        return id;
    }

    public boolean hasNestedResultMaps() {
        return hasNestedResultMaps;
    }

    public boolean hasNestedQueries() {
        return hasNestedQueries;
    }

    public Class<?> getType() {
        return type;
    }

    public List<ResultMapping> getResultMappings() {
        return resultMappings;
    }

    public List<ResultMapping> getConstructorResultMappings() {
        return constructorResultMappings;
    }

    public List<ResultMapping> getPropertyResultMappings() {
        return propertyResultMappings;
    }

    public List<ResultMapping> getIdResultMappings() {
        return idResultMappings;
    }

    public Set<String> getMappedColumns() {
        return mappedColumns;
    }

    public Set<String> getMappedProperties() {
        return mappedProperties;
    }

    public Discriminator getDiscriminator() {
        return discriminator;
    }

    public void forceNestedResultMaps() {
        hasNestedResultMaps = true;
    }

    public Boolean getAutoMapping() {
        return autoMapping;
    }

}
