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
import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * <p>ResolverUtil is used to locate classes that are available in the/a class path and meet
 * arbitrary conditions. The two most common conditions are that a class implements/extends
 * another class, or that is it annotated with a specific annotation. However, through the use
 * of the {@link Test} class it is possible to search using arbitrary conditions.</p>
 *
 * <p>A ClassLoader is used to locate all locations (directories and jar files) in the class
 * path that contain classes within certain packages, and then to load those classes and
 * check them. By default the ClassLoader returned by
 * {@code Thread.currentThread().getContextClassLoader()} is used, but this can be overridden
 * by calling {@link #setClassLoader(ClassLoader)} prior to invoking any of the {@code find()}
 * methods.</p>
 *
 * <p>General searches are initiated by calling the
 * {@link #find(Test, String)} ()} method and supplying
 * a package name and a Test instance. This will cause the named package <b>and all sub-packages</b>
 * to be scanned for classes that meet the test. There are also utility methods for the common
 * use cases of scanning multiple packages for extensions of particular classes, or classes
 * annotated with a specific annotation.</p>
 *
 * <p>The standard usage pattern for the ResolverUtil class is as follows:</p>
 *
 * <pre>
 * ResolverUtil&lt;ActionBean&gt; resolver = new ResolverUtil&lt;ActionBean&gt;();
 * resolver.findImplementation(ActionBean.class, pkg1, pkg2);
 * resolver.find(new CustomTest(), pkg1);
 * resolver.find(new CustomTest(), pkg2);
 * Collection&lt;ActionBean&gt; beans = resolver.getClasses();
 * </pre>
 *
 * TODO:搞不懂为什么要带这个泛型，唯一可能的用处是在{@link #matches}的类型那里起到限制元素的父类的作用，可以看到在{@link #addIfMatching(Test, String)}中有唯一的对这个{@link #matches}添加元素的
 *      地方，但是经过测试，添加元素那行代码对于元素的强转限制是不起作用的（只要是Class都可以add成功），不知道是不是jdk版本的问题，参考单元测试{@link org.apache.ibatis.io.ResolverUtilTest#findImplementations()}
 *
 * @author Tim Fennell
 */
public class ResolverUtil<T> {
    /**
     * An instance of Log to use for logging in this class.
     */
    private static final Log log = LogFactory.getLog(ResolverUtil.class);

    /**
     * A simple interface that specifies how to test classes to determine if they
     * are to be included in the results produced by the ResolverUtil.<br>
     * 译：<br>
     * 一个定义了{@link ResolverUtil}怎样去校验一个类的简单接口，这将决定了
     * 被校验的类是否回被纳入到{@link ResolverUtil}所产生的结果中（{@link #matches}）
     *
     */
    public interface Test {
        /**
         * Will be called repeatedly with candidate classes. Must return True if a class
         * is to be included in the results, false otherwise.<br>
         *
         * @param type
         * @return
         */
        boolean matches(Class<?> type);
    }

    /**
     * A Test that checks to see if each class is assignable to the provided class. Note
     * that this test will match the parent type itself if it is presented for matching.
     */
    public static class IsA implements Test {
        private Class<?> parent;

        /** Constructs an IsA test using the supplied Class as the parent class/interface. */
        public IsA(Class<?> parentType) {
            this.parent = parentType;
        }

        /** Returns true if type is assignable to the parent type supplied in the constructor. */
        @Override
        public boolean matches(Class<?> type) {
            return type != null && parent.isAssignableFrom(type);
        }

        @Override
        public String toString() {
            return "is assignable to " + parent.getSimpleName();
        }
    }

    /**
     * A Test that checks to see if each class is annotated with a specific annotation. If it
     * is, then the test returns true, otherwise false.
     */
    public static class AnnotatedWith implements Test {
        private Class<? extends Annotation> annotation;

        /** Constructs an AnnotatedWith test for the specified annotation type. */
        public AnnotatedWith(Class<? extends Annotation> annotation) {
            this.annotation = annotation;
        }

        /** Returns true if the type is annotated with the class provided to the constructor. */
        @Override
        public boolean matches(Class<?> type) {
            return type != null && type.isAnnotationPresent(annotation);
        }

        @Override
        public String toString() {
            return "annotated with @" + annotation.getSimpleName();
        }
    }

    /** The set of matches being accumulated. */
    private Set<Class<? extends T>> matches = new HashSet<>();

    /**
     * The ClassLoader to use when looking for classes. If null then the ClassLoader returned
     * by Thread.currentThread().getContextClassLoader() will be used.
     */
    private ClassLoader classloader;

    /**
     * Provides access to the classes discovered so far. If no calls have been made to
     * any of the {@code find()} methods, this set will be empty.
     *
     * @return the set of classes that have been discovered.
     */
    public Set<Class<? extends T>> getClasses() {
        return matches;
    }

    /**
     * Returns the classloader that will be used for scanning for classes. If no explicit
     * ClassLoader has been set by the calling, the context class loader will be used.
     *
     * @return the ClassLoader that will be used to scan for classes
     */
    public ClassLoader getClassLoader() {
        return classloader == null ? Thread.currentThread().getContextClassLoader() : classloader;
    }

    /**
     * Sets an explicit ClassLoader that should be used when scanning for classes. If none
     * is set then the context classloader will be used.
     *
     * @param classloader a ClassLoader to use when scanning for classes
     */
    public void setClassLoader(ClassLoader classloader) {
        this.classloader = classloader;
    }

    /**
     * Attempts to discover classes that are assignable to the type provided. In the case
     * that an interface is provided this method will collect implementations. In the case
     * of a non-interface class, subclasses will be collected.  Accumulated classes can be
     * accessed by calling {@link #getClasses()}.
     *
     * @param parent the class of interface to find subclasses or implementations of
     * @param packageNames one or more package names to scan (including subpackages) for classes
     */
    public ResolverUtil<T> findImplementations(Class<?> parent, String... packageNames) {
        if (packageNames == null) {
            return this;
        }

        Test test = new IsA(parent);
        for (String pkg : packageNames) {
            find(test, pkg);
        }

        return this;
    }

    /**
     * Attempts to discover classes that are annotated with the annotation. Accumulated
     * classes can be accessed by calling {@link #getClasses()}.
     *
     * @param annotation the annotation that should be present on matching classes
     * @param packageNames one or more package names to scan (including subpackages) for classes
     */
    public ResolverUtil<T> findAnnotated(Class<? extends Annotation> annotation, String... packageNames) {
        if (packageNames == null) {
            return this;
        }

        Test test = new AnnotatedWith(annotation);
        for (String pkg : packageNames) {
            find(test, pkg);
        }

        return this;
    }

    /**
     * 查找指定包下的满足{@link Test}实现类的约束条件的类：<br>
     *
     * 本方法接收一个{@link Test}的实现类（用来指定要查找的类的约束条件）和一个包名，然后调用{@link VFS#list(String)}来获取
     * 该包名对应的绝对路径下（包括其子孙路径）的所有资源名称，然后选取以".class"结尾的资源名称即类的全限定名，调用{@link #addIfMatching(Test, String)}
     * 方法传入{@code test}和获得的类全限定名做之后的处理。
     *  <hr>
     * 调用本方法之后可以通过{@link #getClasses()}来获取找到的符合条件的类
     *
     * @param test an instance of {@link Test} that will be used to filter classes
     * @param packageName the name of the package from which to start scanning for
     *        classes, e.g. {@code net.sourceforge.stripes}
     */
    public ResolverUtil<T> find(Test test, String packageName) {
        String path = getPackagePath(packageName);

        try {
            List<String> children = VFS.getInstance().list(path);
            for (String child : children) {
                if (child.endsWith(".class")) {
                    addIfMatching(test, child);
                }
            }
        } catch (IOException ioe) {
            log.error("Could not read package: " + packageName, ioe);
        }

        return this;
    }

    /**
     * 将一个包名转换成一个类路径以便在调用
     * {@link ClassLoader#getResources(String)}的时候可以调用。（即将所有'.'转成'/'）
     *
     * @param packageName 要转换的包名
     */
    protected String getPackagePath(String packageName) {
        return packageName == null ? null : packageName.replace('.', '/');
    }

    /**
     * 本方法接受一个{@link Test}的子类和一个类的全限定名，首先调用{@link #getClassLoader()}获取类加载器<br>
     * 根据全限定名加载类的Class对象之后调用{@link Test#matches(Class)}方法校验该类是否符合约束，符合就<br>
     * 将该类添加到集合{@link #matches}，可以通过{@link #getClasses()}来进行访问
     *
     * @param test the test used to determine if the class matches
     * @param fqn the fully qualified name of a class
     */
    @SuppressWarnings("unchecked")
    protected void addIfMatching(Test test, String fqn) {
        try {
            //获取类的全限定名
            String externalName = fqn.substring(0, fqn.indexOf('.')).replace('/', '.');
            ClassLoader loader = getClassLoader();
            //URL resource = loader.getResource("");
            //System.out.println(resource.getPath());
            if (log.isDebugEnabled()) {
                log.debug("Checking to see if class " + externalName + " matches criteria [" + test + "]");
            }

            Class<?> type = loader.loadClass(externalName);
            if (test.matches(type)) {
                matches.add((Class<T>) type);
            }
        } catch (Throwable t) {
            log.warn("Could not examine class '" + fqn + "'" + " due to a " +
                    t.getClass().getName() + " with message: " + t.getMessage());
        }
    }
}