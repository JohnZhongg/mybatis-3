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

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.binding.MapperMethod.ParamMap;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ResultHandler;
import org.apache.ibatis.session.RowBounds;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 方法参数解析器
 * @author JohnZhong
 */
public class ParamNameResolver {

    private static final String GENERIC_NAME_PREFIX = "param";

    /**
     * <p>
     * The key is the index and the value is the name of the parameter.<br />
     * The name is obtained from {@link Param} if specified. When {@link Param} is not specified,
     * the parameter index is used. Note that this index could be different from the actual index
     * when the method has special parameters (i.e. {@link RowBounds} or {@link ResultHandler}).
     * </p>
     * <ul>
     * <li>aMethod(@Param("M") int a, @Param("N") int b) -&gt; {{0, "M"}, {1, "N"}}</li>
     * <li>aMethod(int a, int b) -&gt; {{0, "0"}, {1, "1"}}</li>
     * <li>aMethod(int a, RowBounds rb, int b) -&gt; {{0, "0"}, {2, "1"}}</li>
     * </ul>
     */
    private final SortedMap<Integer, String> names;

    /**
     * 当前ParamNameResolver所对应的Method参数列表是否用到了{@link Param}注解
     */
    private boolean hasParamAnnotation;

    public ParamNameResolver(Configuration config, Method method) {
        //1、获取方法的所有参数类型
        final Class<?>[] paramTypes = method.getParameterTypes();
        //2、获取方法的参数列表的注解数组（二维的，一个方法有多个参数，一个参数有多个注解）
        final Annotation[][] paramAnnotations = method.getParameterAnnotations();
        //3、创建key有序map容器
        final SortedMap<Integer, String> map = new TreeMap<>();
        //4、方法参数个数
        int paramCount = paramAnnotations.length;
        //5、get names from @Param annotations
        for (int paramIndex = 0; paramIndex < paramCount; paramIndex++) {
            // 5.1、skip special parameters
            if (isSpecialParameter(paramTypes[paramIndex])) {
                continue;
            }
            String name = null;
            // 5.2、循环读取当前参数的所有注解，并找到@Param注解为止
            for (Annotation annotation : paramAnnotations[paramIndex]) {
                //如果当前注解是Param
                if (annotation instanceof Param) {
                    hasParamAnnotation = true;
                    //读取该注解的value值到name并且跳出读取注解的循环
                    name = ((Param) annotation).value();
                    break;
                }
            }
            // 5.3、如果根据注解拿不到参数名（没有定义Param注解；Param注解的value没有默认值，如果定义了该注解，没有对value赋值，是不能通过编译的），则使用默认的命名：
            //      <1> 如果开启了useActualParamName，则使用arg01,arg02这样的命名
            //      <2> 否则使用当前参数在当前方法参数列表中的索引作为参数名
            if (name == null) {
                // @Param was not specified.
                if (config.isUseActualParamName()) {
                    name = getActualParamName(method, paramIndex);
                }
                if (name == null) {
                    // use the parameter index as the name ("0", "1", ...)
                    // gcode issue #71
                    //使用当前参数在当前Method的参数列表中去掉了两个特殊参数之后再进行排序的索引位置作为name（即names的value），使用去掉特殊参数之前的索引位置(即实际的索引位置paramIndex)作为index（即names的key），参考下面示例：
                    // testMethod(RowBounds rs, String s1, String str2) --> names: {1:"0",2:"1"}
                    name = String.valueOf(map.size());
                }
            }
            map.put(paramIndex, name);
        }
        names = Collections.unmodifiableSortedMap(map);
    }

    /**
     * 获取真实的参数名，获取的名称是arg01，arg02...
     * @param method 方法类
     * @param paramIndex 方法参数真实的索引位置
     * @return
     */
    private String getActualParamName(Method method, int paramIndex) {
        return ParamNameUtil.getParamNames(method).get(paramIndex);
    }

    /**
     * 判断方法参数是否为特殊类型 {@link RowBounds} and {@link ResultHandler} 或者其子类
     * @param clazz 方法参数的类型
     * @return
     */
    private static boolean isSpecialParameter(Class<?> clazz) {
        return RowBounds.class.isAssignableFrom(clazz) || ResultHandler.class.isAssignableFrom(clazz);
    }

    /**
     * Returns parameter names referenced by SQL providers.<br>
     *
     * 获取当前Method的所有参数名称，{@link ParamNameResolver#names}的values转成数组
     */
    public String[] getNames() {
        return names.values().toArray(new String[0]);
    }

    /**
     * <p>
     * A single non-special parameter is returned without a name.
     * Multiple parameters are named using the naming rule.
     * In addition to the default names, this method also adds the generic names (param1, param2,
     * ...).
     * </p>
     * TODO : 待看到引用处再细看 <br>
     * 单元测试：{@link ParamNameResolverTest#test01()}
     * @param args ParamNameResolver对应的Method的实际参数列表（可能包含了那两个特殊的参数）
     * @return
     */
    public Object getNamedParams(Object[] args) {
        final int paramCount = names.size();
        //1、如果传进来的实际参数列表是null或者本方法无参数（过滤了两个特殊参数），直接返回null
        if (args == null || paramCount == 0) {
            return null;
        //2、如果当前方法列表没有使用Param注解（？）并且参数列表只有一个参数，直接根据参数的实际索引位置（即names的key）获取传进来的实际参数列表中对应的参数值进行返回
        } else if (!hasParamAnnotation && paramCount == 1) {
            return args[names.firstKey()];
        } else {
            final Map<String, Object> param = new ParamMap<>();
            int i = 0;
            // 遍历当前Method的所有参数（names），将方法参数名称和实际参数值建立映射关系作为Map返回
            for (Map.Entry<Integer, String> entry : names.entrySet()) {
                // 使用方法参数的name（即names的value）作为key，根据方法参数的实际索引位置（即names的key）定位到传进来的实际方法参数值列表中对应的参数值并以此为value
                param.put(entry.getValue(), args[entry.getKey()]);
                // add generic param names (param1, param2, ...)
                final String genericParamName = GENERIC_NAME_PREFIX + String.valueOf(i + 1);
                // ensure not to overwrite parameter named with @Param
                if (!names.containsValue(genericParamName)) {
                    param.put(genericParamName, args[entry.getKey()]);
                }
                i++;
            }
            return param;
        }
    }
}
