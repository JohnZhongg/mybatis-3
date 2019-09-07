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

import org.apache.ibatis.io.Resources;

/**
 * To check the existence of version dependent classes.
 */
public class Jdk {

    /**
     * <code>true</code> if <code>java.lang.reflect.Parameter</code> is available.
     * @deprecated Since 3.5.0, Will remove this field at feature(next major version up)
     */
    @Deprecated
    public static final boolean parameterExists;

    static {
        boolean available = false;
        try {
            //Information about method parameters. A Parameter provides information about method parameters, including its name and modifiers. It also provides an alternate means of obtaining attributes for the parameter.
            //Since:1.8（所以如果jdk不是1.8在启动程序的时候，类加载器在加载当前Jdk这个类的时候，会run这个static block，报错）
            Resources.classForName("java.lang.reflect.Parameter");
            available = true;
        } catch (ClassNotFoundException e) {
            // ignore
        }
        parameterExists = available;
    }

    /**
     * @deprecated Since 3.5.0, Will remove this field at feature(next major version up)
     */
    @Deprecated
    public static final boolean dateAndTimeApiExists;

    static {
        boolean available = false;
        try {
            //同上，需要jdk1.8
            Resources.classForName("java.time.Clock");
            available = true;
        } catch (ClassNotFoundException e) {
            // ignore
        }
        dateAndTimeApiExists = available;
    }

    /**
     * @deprecated Since 3.5.0, Will remove this field at feature(next major version up)
     */
    @Deprecated
    public static final boolean optionalExists;

    static {
        boolean available = false;
        try {
            //同上，需要jdk1.8
            Resources.classForName("java.util.Optional");
            available = true;
        } catch (ClassNotFoundException e) {
            // ignore
        }
        optionalExists = available;
    }

    private Jdk() {
        super();
    }
}
