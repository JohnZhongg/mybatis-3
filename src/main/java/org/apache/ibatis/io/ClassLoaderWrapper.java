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

import java.io.InputStream;
import java.net.URL;

/***
 * A class to wrap access to multiple class loaders making them work as one<br>
 * 本类将多个类加载器地访问方式包装起来，让它们地使用看起来像是一个
 *
 * @author Clinton Begin
 */
public class ClassLoaderWrapper {

    /**
     * 默认类加载器，在{@link Resources#setDefaultClassLoader(ClassLoader)}进行赋值或者{@link Resources#getDefaultClassLoader()}进行取值
     */
    ClassLoader defaultClassLoader;
    /**
     * 系统类加载器，在构造器{@link ClassLoaderWrapper#ClassLoaderWrapper()}中初始化赋值了
     */
    ClassLoader systemClassLoader;

    ClassLoaderWrapper() {
        try {
            systemClassLoader = ClassLoader.getSystemClassLoader();
        } catch (SecurityException ignored) {
            // AccessControlException on Google App Engine
        }
    }

    /***
     * 直接调用{@link ClassLoaderWrapper#getResourceAsURL(String, ClassLoader)}和{@link ClassLoaderWrapper#getClassLoaders(ClassLoader)}，本方法没有指定
     * ClassLoader，即最优先被用来加载资源的类加载器，所有第一个用来加载资源的是{@link ClassLoaderWrapper#defaultClassLoader}
     *
     * @param resource - the resource to locate
     * @return the resource or null
     */
    public URL getResourceAsURL(String resource) {
        return getResourceAsURL(resource, getClassLoaders(null));
    }

    /**
     * 直接调用{@link ClassLoaderWrapper#getResourceAsURL(String, ClassLoader)}和{@link ClassLoaderWrapper#getClassLoaders(ClassLoader)}，并指定了自定义的
     * ClassLoader，即最优先被用来加载资源的类加载器
     *
     * @param resource    - the resource to find
     * @param classLoader - the first classloader to try
     * @return the stream or null
     */
    public URL getResourceAsURL(String resource, ClassLoader classLoader) {
        return getResourceAsURL(resource, getClassLoaders(classLoader));
    }

    /***
     * 直接调用{@link ClassLoaderWrapper#getResourceAsStream(String, ClassLoader)}和{@link ClassLoaderWrapper#getClassLoaders(ClassLoader)}，本方法没有指定
     * ClassLoader，即最优先被用来加载资源的类加载器，所有第一个用来加载资源的是{@link ClassLoaderWrapper#defaultClassLoader}
     *
     * @param resource 要加载的资源（类路径）
     * @return 加载的资源或者null
     */
    public InputStream getResourceAsStream(String resource) {
        return getResourceAsStream(resource, getClassLoaders(null));
    }

    /***
     * 直接调用{@link ClassLoaderWrapper#getResourceAsStream(String, ClassLoader)}和{@link ClassLoaderWrapper#getClassLoaders(ClassLoader)}，并指定了自定义的
     * ClassLoader，即最优先被用来加载资源的类加载器
     *
     * @param resource 要加载的资源（类路径）
     * @param classLoader 最优先用来加载资源的类加载器
     * @return 加载的资源或者null
     */
    public InputStream getResourceAsStream(String resource, ClassLoader classLoader) {
        return getResourceAsStream(resource, getClassLoaders(classLoader));
    }

    /***
     * 直接调用{@link ClassLoaderWrapper#classForName(String, ClassLoader[])}和{@link ClassLoaderWrapper#getClassLoaders(ClassLoader)}，本方法没有指定了类加载器数组中的<br>
     * 自定义类加载器，即第一个用来加载类的类加载器是默认加载器{@link ClassLoaderWrapper#defaultClassLoader}
     *
     * @param name 要加载的类全限定名
     * @return 加载的类
     * @throws ClassNotFoundException Duh.
     */
    public Class<?> classForName(String name) throws ClassNotFoundException {
        return classForName(name, getClassLoaders(null));
    }

    /**
     * 直接调用{@link ClassLoaderWrapper#classForName(String, ClassLoader[])}和{@link ClassLoaderWrapper#getClassLoaders(ClassLoader)}，本方法指定了类加载器数组中的<br>
     * 自定义类加载器，即最优先被用来根据类全限定名加载类的类加载器
     *
     * @param name  要加载的类全限定名
     * @param classLoader  最优先使用的类加载器
     * @return 加载的类
     * @throws ClassNotFoundException Duh.
     */
    public Class<?> classForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
        return classForName(name, getClassLoaders(classLoader));
    }

    /**
     * 传入一个类的全限定名和类加载器数组，然后正向遍历类加载器数组，如果遍历的当前类加载器不为null，<br>
     * 就使用当前的类加载器在其对应的类路径寻找资源，如果类加载器或者寻找的资源的是null，就使用下一个类加载器<br>
     * 继续寻找，直到得到的资源不是null，就返回找到的资源；或者所有类加载器遍历完之后得到的资源都是null，则返回null<br>
     *
     * @param resource 要加载的资源（类路径）
     * @param classLoader  类加载器数组
     * @return the resource or null
     */
    InputStream getResourceAsStream(String resource, ClassLoader[] classLoader) {
        for (ClassLoader cl : classLoader) {
            if (null != cl) {
                // try to find the resource as passed
                InputStream returnValue = cl.getResourceAsStream(resource);
                // now, some class loaders want this leading "/", so we'll add it and try again if we didn't find the resource
                if (null == returnValue) {
                    returnValue = cl.getResourceAsStream("/" + resource);
                }
                if (null != returnValue) {
                    return returnValue;
                }
            }
        }
        return null;
    }

    /**
     * 传入一个类的全限定名和类加载器数组，然后正向遍历类加载器数组，如果遍历的当前类加载器不为null，<br>
     * 就使用当前的类加载器在其对应的类路径寻找资源，如果类加载器或者寻找的资源的是null，就使用下一个类加载器<br>
     * 继续寻找，直到得到的资源不是null，就返回找到的资源；或者所有类加载器遍历完之后得到的资源都是null，则返回null<br>
     *
     * @param resource 要加载的资源（类路径）
     * @param classLoader 类加载器数组
     * @return the resource or null
     */
    URL getResourceAsURL(String resource, ClassLoader[] classLoader) {
        URL url;
        for (ClassLoader cl : classLoader) {
            if (null != cl) {
                // look for the resource as passed in...
                url = cl.getResource(resource);
                // ...but some class loaders want this leading "/", so we'll add it
                // and try again if we didn't find the resource
                if (null == url) {
                    url = cl.getResource("/" + resource);
                }
                // "It's always in the last place I look for it!"
                // ... because only an idiot would keep looking for it after finding it, so stop looking already.
                if (null != url) {
                    return url;
                }
            }
        }
        // didn't find it anywhere.
        return null;
    }

    /**
     * 传入一个类的全限定名和类加载器数组，然后正向遍历类加载器数组，如果遍历的当前类加载器不为null，<br>
     * 就使用当前的类加载器加载类，如果类加载器或者加载的类的是null，就使用下一个类加载器加载类，直到<br>
     * 加载的类不是null，或者所有类加载器遍历完之后加载该类都是得到null，如果是后者则抛出异常{@link ClassNotFoundException}。
     *
     * @param name 要加载的类的全限定名
     * @param classLoader 类加载器数组
     * @return 类加载器的加载的Class
     * @throws ClassNotFoundException - Remember the wisdom of Judge Smails: Well, the world needs ditch diggers, too.
     */
    Class<?> classForName(String name, ClassLoader[] classLoader) throws ClassNotFoundException {
        for (ClassLoader cl : classLoader) {
            if (null != cl) {
                try {
                    Class<?> c = Class.forName(name, true, cl);
                    if (null != c) {
                        return c;
                    }
                } catch (ClassNotFoundException e) {
                    // we'll ignore this until all classloaders fail to locate the class
                }
            }
        }
        throw new ClassNotFoundException("Cannot find class: " + name);
    }

    /**
     * 传入一个自定义的类加载器，返回一个类加载器数组，这个数组的元素分别是（正序排序）：<br>
     * 1. 传入的自定义类加载器<br>
     * 2. 默认的类加载器<br>
     * 3. 当前线程的类加载器<br>
     * 4. 当前类的类加载器<br>
     * 5. 系统类加载器<br>
     *
     * @param classLoader 自定义类加载器
     * @return 类加载器数组
     */
    ClassLoader[] getClassLoaders(ClassLoader classLoader) {
        return new ClassLoader[]{
                classLoader,
                defaultClassLoader,
                Thread.currentThread().getContextClassLoader(),
                getClass().getClassLoader(),
                systemClassLoader};
    }

}
