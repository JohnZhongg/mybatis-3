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
package org.apache.ibatis.scripting;

import java.util.HashMap;
import java.util.Map;

/**
 * {@link LanguageDriver} 注解表
 *
 * @author Frank D. Martinez [mnesarco]
 */
public class LanguageDriverRegistry {

    /**
     * {@link LanguageDriver} 映射：<br>
     *     key: {@link Class} <? extends LanguageDriver> <br>
     *     value: {@link LanguageDriver}实例
     */
    private final Map<Class<? extends LanguageDriver>, LanguageDriver> LANGUAGE_DRIVER_MAP = new HashMap<>();
    /**
     * 默认{@link LanguageDriver}的{@link Class}对象
     */
    private Class<? extends LanguageDriver> defaultDriverClass;

    /**
     * 如果{@code cls}是null，抛出异常；如果{@link #LANGUAGE_DRIVER_MAP}中没有包含当前{@code cls}，则{@link #LANGUAGE_DRIVER_MAP}.put(cls, cls.newInstance())
     *
     * @param cls
     */
    public void register(Class<? extends LanguageDriver> cls) {
        if (cls == null) {
            throw new IllegalArgumentException("null is not a valid Language Driver");
        }
        if (!LANGUAGE_DRIVER_MAP.containsKey(cls)) {
            try {
                LANGUAGE_DRIVER_MAP.put(cls, cls.newInstance());
            } catch (Exception ex) {
                throw new ScriptingException("Failed to load language driver for " + cls.getName(), ex);
            }
        }
    }

    /**
     * 如果{@code instance}是null，抛出异常；如果{@link #LANGUAGE_DRIVER_MAP}中没有包含当前{@code instance.getClass()}，则{@link #LANGUAGE_DRIVER_MAP}.put(instance.getClass(), instance)
     *
     * @param instance
     */
    public void register(LanguageDriver instance) {
        if (instance == null) {
            throw new IllegalArgumentException("null is not a valid Language Driver");
        }
        // 添加到 LANGUAGE_DRIVER_MAP 中
        Class<? extends LanguageDriver> cls = instance.getClass();
        if (!LANGUAGE_DRIVER_MAP.containsKey(cls)) {
            LANGUAGE_DRIVER_MAP.put(cls, instance);
        }
    }

    /**
     * {@link #LANGUAGE_DRIVER_MAP}.get(cls)
     *
     * @param cls
     * @return
     */
    public LanguageDriver getDriver(Class<? extends LanguageDriver> cls) {
        return LANGUAGE_DRIVER_MAP.get(cls);
    }

    /**
     * 调用{@link #getDefaultDriver()}获取{@link #defaultDriverClass}
     *
     * @return
     */
    public LanguageDriver getDefaultDriver() {
        return getDriver(getDefaultDriverClass());
    }

    public Class<? extends LanguageDriver> getDefaultDriverClass() {
        return defaultDriverClass;
    }

    /**
     * 先调用{@link #register(Class)}注册然后设置 {@link #defaultDriverClass}
     *
     * @param defaultDriverClass 默认的 LanguageDriver 类
     */
    public void setDefaultDriverClass(Class<? extends LanguageDriver> defaultDriverClass) {
        register(defaultDriverClass);
        this.defaultDriverClass = defaultDriverClass;
    }

}