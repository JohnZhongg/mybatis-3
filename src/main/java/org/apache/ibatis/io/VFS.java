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

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Provides a very simple API for accessing resources within an application server.
 *
 * <p>
 *     这里的VFS相对于操作系统层面的VFS是更高级的，操作系统的VFS是对硬件（硬盘）中的文件系统进行了封装（文件夹+文件）；这里的VFS是对不同结构的数据进行了封装构建成了一个虚拟的文件系统（命名空间+文件），
 *     例如：File、Jar、Zip、File、FTP等，我们对于这些结构的数据的底层访问方式都是不同的，但是它们有着的共性就是存在"命名空间+文件"的抽象结构，这里的VFS的作用就是封装底层对于不同类型的文件的不同访问
 *     操作（例如一个Jar文件的字节流，应该如何读取，用什么工具类来读取）成一个统一的访问API。参考apache的一个VFS：<a href='https://commons.apache.org/proper/commons-vfs/filesystems.html'>apache common vfs</a>
 * </p>
 *
 * @author Ben Gunter
 */
public abstract class VFS {
    private static final Log log = LogFactory.getLog(VFS.class);

    /**
     * 内置VFS实现数组，有2个：{@link JBoss6VFS}和{@link DefaultVFS}
     */
    public static final Class<?>[] IMPLEMENTATIONS = {JBoss6VFS.class, DefaultVFS.class};

    /**
     * 自定义VFS实现集合，通过{@link #addImplClass(Class)}方法来添加
     */
    public static final List<Class<? extends VFS>> USER_IMPLEMENTATIONS = new ArrayList<>();

    /**
     * VFS单例实例持有者
     */
    private static class VFSHolder {
        /**
         * VFS单例实例<br>
         * 参考{@link VFSHolder#createVFS()}
         */
        static final VFS INSTANCE = createVFS();

        /**
         * 先从自定义VFS实现开始正向遍历，然后再遍历内置VFS实现，然后调用实现的无参构造进行实例化，直到实例化的VFS不是NULL并且是有效的
         * @return
         */
        @SuppressWarnings("unchecked")
        static VFS createVFS() {
            // Try the user implementations first, then the built-ins
            List<Class<? extends VFS>> impls = new ArrayList<>();
            impls.addAll(USER_IMPLEMENTATIONS);
            impls.addAll(Arrays.asList((Class<? extends VFS>[]) IMPLEMENTATIONS));

            // Try each implementation class until a valid one is found
            VFS vfs = null;
            for (int i = 0; vfs == null || !vfs.isValid(); i++) {
                Class<? extends VFS> impl = impls.get(i);
                try {
                    vfs = impl.newInstance();
                    if (vfs == null || !vfs.isValid()) {
                        if (log.isDebugEnabled()) {
                            log.debug("VFS implementation " + impl.getName() +
                                    " is not valid in this environment.");
                        }
                    }
                } catch (InstantiationException e) {
                    log.error("Failed to instantiate " + impl, e);
                    return null;
                } catch (IllegalAccessException e) {
                    log.error("Failed to instantiate " + impl, e);
                    return null;
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("Using VFS adapter " + vfs.getClass().getName());
            }

            return vfs;
        }
    }

    /**
     * 获取{@link VFS}的单例实例对象，如果当前的环境找不到{@link VFS}的实现，本方法将返回NULL。
     * 具体参考{@link VFSHolder#INSTANCE}
     */
    public static VFS getInstance() {
        return VFSHolder.INSTANCE;
    }

    /**
     * 添加特定的Class到{@link VFS}的实现列表中。通过本方法添加的Class将会以它们添加的顺序来实例，且顺序在所有的内置实现之前
     *
     * @param clazz 要添加的{@link VFS}的实现
     */
    public static void addImplClass(Class<? extends VFS> clazz) {
        if (clazz != null) {
            USER_IMPLEMENTATIONS.add(clazz);
        }
    }

    /**
     * 使用当前线程的类加载器根据类的全限定名加载Class对象，如果找不到就返回null
     *
     * @param className 类全限定名
     * @return 返回加载到的类
     */
    protected static Class<?> getClass(String className) {
        try {
            return Thread.currentThread().getContextClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
            if (log.isDebugEnabled()) {
                log.debug("Class not found: " + className);
            }
            return null;
        }
    }

    /**
     * 根据方法名称和参数类型数组获得传入Class的Method，如果找不到就返回null
     *
     * @param clazz 要查找的Method所属Class
     * @param methodName 要查找的方法名
     * @param parameterTypes 方法参数列表类型
     */
    protected static Method getMethod(Class<?> clazz, String methodName, Class<?>... parameterTypes) {
        if (clazz == null) {
            return null;
        }
        try {
            return clazz.getMethod(methodName, parameterTypes);
        } catch (SecurityException e) {
            log.error("Security exception looking for method " + clazz.getName() + "." + methodName + ".  Cause: " + e);
            return null;
        } catch (NoSuchMethodException e) {
            log.error("Method not found " + clazz.getName() + "." + methodName + "." + methodName + ".  Cause: " + e);
            return null;
        }
    }

    /**
     * 在一个对象上执行一个Method，然后返回它返回的任意对象
     *
     * @param method 要执行的方法
     * @param object 那些方法所属的实例对象或者一个Class(是为了那些静态方法准备的)
     * @param parameters 方法的参数值列表
     * @return 方法返回的任意值
     * @throws IOException If I/O errors occur
     * @throws RuntimeException If anything else goes wrong
     */
    @SuppressWarnings("unchecked")
    protected static <T> T invoke(Method method, Object object, Object... parameters)
            throws IOException, RuntimeException {
        try {
            return (T) method.invoke(object, parameters);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            if (e.getTargetException() instanceof IOException) {
                throw (IOException) e.getTargetException();
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * 传入一个类路径，然后使用当前线程的类加载器去所有的类根路径下查找该类路径，然后将所有查找到的类路径的绝对路径封装成{@link URL}集合
     *
     * @param path 要查找的类路径.
     * @return A list of {@link URL}s, as returned by {@link ClassLoader#getResources(String)}.
     * @throws IOException If I/O errors occur
     */
    protected static List<URL> getResources(String path) throws IOException {
        return Collections.list(Thread.currentThread().getContextClassLoader().getResources(path));
    }

    /**
     * 如果{@link VFS}的实现在当前环境下是有效的，则返回true
     * @return
     */
    public abstract boolean isValid();

    /**
     * TODO: 待详细看
     * 传入一个URL和URL所对应的类路径（URL是通过{@link #getResources(String)}得到的），然后递归查找这个URL下所有以传入的类路径开头的
     * 包、类的全限定名以及一些资源文件的类路径名称的集合，并返回
     *
     * @param url The URL that identifies the resource to list.
     * @param forPath The path to the resource that is identified by the URL. Generally, this is the
     *            value passed to {@link #getResources(String)} to get the resource URL.
     * @return A list containing the names of the child resources.
     * @throws IOException If I/O errors occur
     */
    protected abstract List<String> list(URL url, String forPath) throws IOException;

    /**
     * 传入一个类路径，调用{@link #getResources(String)}到所有的类根目录下查找该类路径的绝对路径并封装成URL，然后调用<br>
     * {@link #list(URL, String)}递归查找所有找到的类路径下的包、类的全限定名以及一些资源文件的类路径名称，封装到一个集合中返回
     *
     * @param path 要加载的类路径
     * @return A list containing the names of the child resources.
     * @throws IOException If I/O errors occur
     */
    public List<String> list(String path) throws IOException {
        List<String> names = new ArrayList<>();
        for (URL url : getResources(path)) {
            System.out.println(url);
            names.addAll(list(url, path));
        }
        return names;
    }
}
