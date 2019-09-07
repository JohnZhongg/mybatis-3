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
package org.apache.ibatis.logging;

import org.apache.ibatis.logging.commons.JakartaCommonsLoggingImpl;
import org.apache.ibatis.logging.jdk14.Jdk14LoggingImpl;
import org.apache.ibatis.logging.log4j.Log4jImpl;
import org.apache.ibatis.logging.log4j2.Log4j2Impl;
import org.apache.ibatis.logging.nologging.NoLoggingImpl;
import org.apache.ibatis.logging.stdout.StdOutImpl;

import java.lang.reflect.Constructor;

/**
 * {@link Log} 工厂类
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public final class LogFactory {

    /**
     * Marker to be used by logging implementations that support markers
     */
    public static final String MARKER = "MYBATIS";

    /**
     * 使用的 Log 的构造方法
     */
    private static Constructor<? extends Log> logConstructor;

    static {
        // 按顺序尝试初始化logConstructor属性（尝试初始化各个日志框架），一旦成功就不再初始化，这里的方法都是同步的，非多线程
        tryImplementation(LogFactory::useSlf4jLogging);
        tryImplementation(LogFactory::useCommonsLogging);
        tryImplementation(LogFactory::useLog4J2Logging);
        tryImplementation(LogFactory::useLog4JLogging);
        tryImplementation(LogFactory::useJdkLogging);
        tryImplementation(LogFactory::useNoLogging);
    }

    private LogFactory() {
        // disable construction
    }

    /**
     * 传入打印日志的类构建{@link Log}对象并返回（将传入的{@code aClass}转化成全限定名之后调用{@link #getLog(String)}}）
     *
     * @param aClass 打印日志的类
     * @return
     */
    public static Log getLog(Class<?> aClass) {
        return getLog(aClass.getName());
    }

    /**
     * 调用{@link #logConstructor}的{@link Constructor#newInstance(Object...)}方法，传入日志打印类的全限定名构建{@link Log}并返回
     *
     * @param logger
     * @return
     */
    public static Log getLog(String logger) {
        try {
            return logConstructor.newInstance(logger);
        } catch (Throwable t) {
            throw new LogException("Error creating logger for logger " + logger + ".  Cause: " + t, t);
        }
    }

    /**
     * 用于给用户传入自定义的{@link Log}实现类
     * @param clazz
     */
    public static synchronized void useCustomLogging(Class<? extends Log> clazz) {
        setImplementation(clazz);
    }

    /**
     * 1、调用{@link #setImplementation(Class)}设置{@link org.apache.ibatis.logging.slf4j.Slf4jImpl#Slf4jImpl(String)}为{@link #logConstructor}
     */
    public static synchronized void useSlf4jLogging() {
        setImplementation(org.apache.ibatis.logging.slf4j.Slf4jImpl.class);
    }

    /**
     * 2、调用{@link #setImplementation(Class)}设置{@link JakartaCommonsLoggingImpl#JakartaCommonsLoggingImpl(String)}为{@link #logConstructor}
     */
    public static synchronized void useCommonsLogging() {
        setImplementation(org.apache.ibatis.logging.commons.JakartaCommonsLoggingImpl.class);
    }

    /**
     * 3、调用{@link #setImplementation(Class)}设置{@link Log4jImpl#Log4jImpl(String)}为{@link #logConstructor}
     */
    public static synchronized void useLog4JLogging() {
        setImplementation(org.apache.ibatis.logging.log4j.Log4jImpl.class);
    }

    /**
     * 4、调用{@link #setImplementation(Class)}设置{@link Log4j2Impl#Log4j2Impl(String)}为{@link #logConstructor}
     */
    public static synchronized void useLog4J2Logging() {
        setImplementation(org.apache.ibatis.logging.log4j2.Log4j2Impl.class);
    }

    /**
     * 5、调用{@link #setImplementation(Class)}设置{@link Jdk14LoggingImpl#Jdk14LoggingImpl(String)}为{@link #logConstructor}
     */
    public static synchronized void useJdkLogging() {
        setImplementation(org.apache.ibatis.logging.jdk14.Jdk14LoggingImpl.class);
    }


    /**
     * 调用{@link #setImplementation(Class)}设置{@link StdOutImpl#StdOutImpl(String)}为{@link #logConstructor}
     */
    public static synchronized void useStdOutLogging() {
        setImplementation(org.apache.ibatis.logging.stdout.StdOutImpl.class);
    }

    /**
     * 6、调用{@link #setImplementation(Class)}设置{@link NoLoggingImpl#NoLoggingImpl(String)}为{@link #logConstructor}
     */
    public static synchronized void useNoLogging() {
        setImplementation(org.apache.ibatis.logging.nologging.NoLoggingImpl.class);
    }

    /**
     * 如果{@link #logConstructor}是null，则执行传入的函数行为() -> return void;
     * <p>
     *     注意，这里的入参{@code runnable}仅仅是让编程函数化而已，函数内部直接调用了{@code runnable}的{@link Runnable#run()}方法，并没有多线程
     * </p>
     *
     * @param runnable
     */
    private static void tryImplementation(Runnable runnable) {
        if (logConstructor == null) {
            try {
                runnable.run();
            } catch (Throwable t) {
                // ignore
            }
        }
    }

    /**
     * 根据传入的{@link Log}的实现类{@code implClass}获取其携带一个{@link String}类型参数（该参数为要打印日志的类的全限定名）构造函数并设置给{@link #logConstructor}
     *
     * @param implClass {@link Log}的实现类
     */
    private static void setImplementation(Class<? extends Log> implClass) {
        try {
            // 获得参数为 String 的构造方法
            Constructor<? extends Log> candidate = implClass.getConstructor(String.class);
            // 创建 Log 对象
            Log log = candidate.newInstance(LogFactory.class.getName());
            // 如果在日志框架中配置了debug级别
            if (log.isDebugEnabled()) {
                log.debug("Logging initialized using '" + implClass + "' adapter.");
            }
            // 创建成功，意味着可以使用，设置为 logConstructor
            logConstructor = candidate;
        } catch (Throwable t) {
            throw new LogException("Error setting Log implementation.  Cause: " + t, t);
        }
    }

}
