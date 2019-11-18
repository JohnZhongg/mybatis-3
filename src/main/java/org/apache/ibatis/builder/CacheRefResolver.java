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

/**
 * Cache id 解析器（根据cache唯一标识解析为一个{@link Cache}对象）
 *
 * @author Clinton Begin
 */
public class CacheRefResolver {

    private final MapperBuilderAssistant assistant;
    private final String cacheRefNamespace;

    public CacheRefResolver(MapperBuilderAssistant assistant, String cacheRefNamespace) {
        this.assistant = assistant;
        this.cacheRefNamespace = cacheRefNamespace;
    }

    /**
     * 调用{@link #assistant}的{@link MapperBuilderAssistant#useCacheRef(String)}方法传入{@link #cacheRefNamespace}解析该namespace指向的{@link Cache}对象并返回
     *
     * @return
     */
    public Cache resolveCacheRef() {
        return assistant.useCacheRef(cacheRefNamespace);
    }

}