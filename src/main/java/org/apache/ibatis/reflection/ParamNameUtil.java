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
package org.apache.ibatis.reflection;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;

/**
 * 方法参数名称工具类（调用jdk底层方法获取方法参数类{@link Parameter#getName()}获取参数名称）
 * @author JohnZhong
 */
public class ParamNameUtil {
    /**
     * 调用{@link #getParameterNames(Executable)}方法获取参数名称，获取的名称是arg01，arg02...如果是无参可执行对象，则返回一个长度为0的空列表
     * @param method 成员方法
     * @return 返回参数名List
     */
    public static List<String> getParamNames(Method method) {
        return getParameterNames(method);
    }

    /**
     * 调用{@link #getParameterNames(Executable)}方法获取参数名称，获取的名称是arg01，arg02...如果是无参可执行对象，则返回一个长度为0的空列表
     * @param constructor 构造方法
     * @return 返回参数名List
     */
    public static List<String> getParamNames(Constructor<?> constructor) {
        return getParameterNames(constructor);
    }

    /**
     * 传入一个{@link Executable}对象{@code executable}，然后调用{@link Executable#getParameters()}获得一个{@link Parameter}数组，然后依次循环所有元素调用jdk底层方法{@link Parameter#getName()}获取方法参数名称，获取的名称是arg01，arg02...如果是无参可执行对象，则返回一个长度为0的空数组
     * @param executable 可执行的类（Constructor和Method）
     * @return 返回参数名称List
     */
    private static List<String> getParameterNames(Executable executable) {
        final List<String> names = new ArrayList<>();
        final Parameter[] params = executable.getParameters();
        for (Parameter param : params) {
            names.add(param.getName());
        }
        return names;
    }

    private ParamNameUtil() {
        super();
    }
}
