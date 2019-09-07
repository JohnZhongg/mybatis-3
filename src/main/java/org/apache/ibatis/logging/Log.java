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

/**
 * MyBatis Log 接口
 *
 * @author Clinton Begin
 */
public interface Log {

    boolean isDebugEnabled();

    boolean isTraceEnabled();

    void error(String s, Throwable e);

    void error(String s);

    /**
     * 在mybatis中，一般都会在调用本方法之前判断{@link Log}级别是否是DEBUG级别，是才会记录日志。这里和其他级别日志不太一样。
     * mybatis是否调用日志类记录日志是日志类可以输出日志的前提，而mybatis记录日志到日志组件之后日志组件输不输出又是另外一回事了，
     * 例如日志框架的日志输出级别设置为ERROR，而mybatis中没有在记录日志之前获取日志框架的日志级别而记录了warning的日志到日志框架，但是日志框架照样不会输出。
     *
     * @param s
     */
    void debug(String s);

    void trace(String s);

    void warn(String s);

}