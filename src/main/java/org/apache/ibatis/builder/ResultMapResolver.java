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

import org.apache.ibatis.mapping.Discriminator;
import org.apache.ibatis.mapping.ResultMap;
import org.apache.ibatis.mapping.ResultMapping;

import java.util.List;

/**
 * {@link ResultMap} 解析器
 *
 * @author Eduardo Macarron
 */
public class ResultMapResolver {

    private final MapperBuilderAssistant assistant;
    /**
     * ResultMap 编号
     */
    private final String id;
    /**
     * 类型
     */
    private final Class<?> type;
    /**
     * 继承自哪个 ResultMap
     */
    private final String extend;
    /**
     * Discriminator 对象
     */
    private final Discriminator discriminator;
    /**
     * ResultMapping 集合
     */
    private final List<ResultMapping> resultMappings;
    /**
     * 是否自动匹配，会覆盖全局设置({@code <settings/>})中的"autoMappingBehavior"属性
     */
    private final Boolean autoMapping;

    /**
     * 设置成员变量：{@link #assistant}、{@link #id}、{@link #type}、{@link #extend}、{@link #discriminator}、{@link #resultMappings}、{@link #autoMapping}
     *
     * @param assistant
     * @param id
     * @param type
     * @param extend
     * @param discriminator
     * @param resultMappings
     * @param autoMapping
     */
    public ResultMapResolver(MapperBuilderAssistant assistant, String id, Class<?> type, String extend, Discriminator discriminator, List<ResultMapping> resultMappings, Boolean autoMapping) {
        this.assistant = assistant;
        this.id = id;
        this.type = type;
        this.extend = extend;
        this.discriminator = discriminator;
        this.resultMappings = resultMappings;
        this.autoMapping = autoMapping;
    }

    /**
     * 调用成员变量{@link #assistant}的{@link MapperBuilderAssistant#addResultMap(String, Class, String, Discriminator, List, Boolean)}传入成员变量：{@link #id}、{@link #type}、{@link #extend}、{@link #discriminator}、{@link #resultMappings}、{@link #autoMapping}
     * 构建{@link ResultMap}对象并记录到{@link org.apache.ibatis.session.Configuration}中然后返回
     *
     * @return {@link ResultMap}对象
     */
    public ResultMap resolve() {
        return assistant.addResultMap(this.id, this.type, this.extend, this.discriminator, this.resultMappings, this.autoMapping);
    }

}