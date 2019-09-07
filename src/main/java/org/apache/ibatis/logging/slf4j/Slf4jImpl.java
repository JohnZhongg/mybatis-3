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
package org.apache.ibatis.logging.slf4j;

import org.apache.ibatis.logging.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.spi.LocationAwareLogger;

/**
 * 适配Slf4j日志框架
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public class Slf4jImpl implements Log {

    private Log log;

    /**
     * 分别用{@link Slf4jLocationAwareLoggerImpl}和{@link Slf4jLoggerImpl}适配Slf4j框架中对于Slf4j框架{@link Logger}类的不同实现：先检测{@link Logger}是否1.6版本，即测试
     * {@link LocationAwareLogger#log(Marker, String, int, String, Object[], Throwable)}的方法是否存在，存在则使用{@link Slf4jLocationAwareLoggerImpl}进行适配，
     * 否则使用{@link Slf4jLoggerImpl}适配其他版本
     *
     * @param clazz
     */
    public Slf4jImpl(String clazz) {
        // 使用Slf4j日志框架的Logger工厂获取Logger对象
        Logger logger = LoggerFactory.getLogger(clazz);
        // 判断Logger对象是否是1.6版本以上的LocationAwareLogger：如果是则使用Slf4jLocationAwareLoggerImpl适配并返回；否则使用Slf4jLoggerImpl适配
        if (logger instanceof LocationAwareLogger) {
            try {
                // check for slf4j >= 1.6 method signature
                logger.getClass().getMethod("log", Marker.class, String.class, int.class, String.class, Object[].class, Throwable.class);
                log = new Slf4jLocationAwareLoggerImpl((LocationAwareLogger) logger);
                return;
            } catch (SecurityException | NoSuchMethodException e) {
                // fail-back to Slf4jLoggerImpl
            }
        }

        // Logger is not LocationAwareLogger or slf4j version < 1.6
        log = new Slf4jLoggerImpl(logger);
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public void error(String s, Throwable e) {
        log.error(s, e);
    }

    @Override
    public void error(String s) {
        log.error(s);
    }

    @Override
    public void debug(String s) {
        log.debug(s);
    }

    @Override
    public void trace(String s) {
        log.trace(s);
    }

    @Override
    public void warn(String s) {
        log.warn(s);
    }

}