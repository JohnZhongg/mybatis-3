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

import org.apache.ibatis.session.Configuration;
import org.junit.Test;

/**
 * @author: honphan.john
 * @date: 2019-08-26 13:42
 * @description:
 */
public class ParamNameResolverTest {

    @Test
    public void test01() throws NoSuchMethodException {
        Configuration configuration = new Configuration();
        configuration.setUseActualParamName(false);
        ParamNameResolver paramNameResolver = new ParamNameResolver(configuration, this.getClass().getDeclaredMethod("testMethod"/*, RowBounds.class, String.class*/, String.class));
        Object[] objects = {null, "sa", new ArrayUtil()};
        Object namedParams = paramNameResolver.getNamedParams(objects);
        System.out.println();
    }

    public void testMethod(/*RowBounds rs, String s1, */String str2) {

    }

}
