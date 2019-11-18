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
package org.apache.ibatis.io;


import org.apache.ibatis.type.TypeHandler;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * @author: honphan.john
 * @date: 2019-08-30 11:32
 * @description:
 */
public class ResolverUtilTest {
    private ResolverUtil<Long> resolverUtil = new MyResolverUtil();

    @Test
    public void getClasses() {
    }

    @Test
    public void getClassLoader() {
    }

    @Test
    public void setClassLoader() {
    }

    @Test
    public void findImplementations() throws NoSuchFieldException {

        ResolverUtil<Long> implementations = this.resolverUtil.findImplementations(TypeHandler.class, "org.apache.ibatis.type");
        Set classes = implementations.getClasses();
        classes.add(Integer.class);
        //classes.add((Class<ResolverUtil>) BaseTypeHandler.class);
        //System.out.println();

        A<Long> a = new A<>();
        a.testMethod(A.class);

    }

    @Test
    public void findAnnotated() {
    }

    @Test
    public void find() {
        ResolverUtil<Object> objectResolverUtil = new ResolverUtil<>();
        ResolverUtil<Object> objectResolverUtil1 = objectResolverUtil.find(new ResolverUtil.Test() {
            @Override
            public boolean matches(Class<?> type) {
                return true;
            }
        }, "org.apache.ibatis.io");
        Set<Class<?>> classes = objectResolverUtil1.getClasses();
        System.out.println();
    }

    @Test
    public void getPackagePath() {
    }

    @Test
    public void addIfMatching() {
    }
}

class MyResolverUtil extends ResolverUtil<Long> {

}

class A<T> {
    public Set<Class<? extends T>> set = new HashSet<>();

    public void testMethod(Class<? /*extends T*/> clazz) {
        set.add((Class<T>) clazz);
    }
}