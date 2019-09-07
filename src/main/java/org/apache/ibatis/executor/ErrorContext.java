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
package org.apache.ibatis.executor;

/**
 * 错误上下文
 *
 * @author Clinton Begin
 */
public class ErrorContext {

    /**
     * 行分隔符，调用{@link System#getProperty(String, String)}获取"line.separator"的value（第一个入参为key，第二个入参为默认值），可以事先通过{@link System#setProperty(String, String)}进行设置，如果没有设置则会使用默认值
     */
    private static final String LINE_SEPARATOR = System.getProperty("line.separator", "\n");
    /**
     * static的用来存储所有{@link ErrorContext}对象的{@link ThreadLocal}对象
     */
    private static final ThreadLocal<ErrorContext> LOCAL = new ThreadLocal<>();

    /**
     * 用来支撑{@link ErrorContext}称为一条链表（栈）的变量，参考{@link #store()}和{@link #recall()}
     */
    private ErrorContext stored;
    /**
     * The error may exist in。参考{@link ErrorContext#toString()}
     */
    private String resource;
    /**
     * The error occurred whil。参考{@link ErrorContext#toString()}
     */
    private String activity;
    /**
     * The error may involve(可能涉及的对象)。参考{@link ErrorContext#toString()}
     */
    private String object;
    /**
     * 一些需要记录的信息。参考{@link ErrorContext#toString()}
     */
    private String message;
    /**
     * 涉及的sql。参考{@link ErrorContext#toString()}
     */
    private String sql;
    /**
     * 错误的原因。参考{@link ErrorContext#toString()}
     */
    private Throwable cause;

    private ErrorContext() {
    }

    /**
     * 从{@link #LOCAL}获取当前线程对应的{@link ErrorContext}实例，如果没有则新建一个并设置进{@link #LOCAL}然后返回该实例对象
     *
     * @return
     */
    public static ErrorContext instance() {
        ErrorContext context = LOCAL.get();
        if (context == null) {
            context = new ErrorContext();
            LOCAL.set(context);
        }
        return context;
    }

    /**
     * new 一个新的{@link ErrorContext}实例，将当前对象设置为新实例的{@link #stored}成员变量，并将新实例设置到{@link #LOCAL}与当前线程绑定然后返回新实例
     * （相当于将当前这个相对旧的实例挂靠到一个新建的实例，这个动作可以循环进行下去，形成一个单向链表）示例：
     * <pre>
     * in same Thread：
     * 前置条件：
     * ThreadLocal ---thread id---> errorContext1
     * 执行动作：
     * errorContext1.store()
     * 结果:
     * ThreadLocal ---thread id---> newErrorContext ---stored---> errorContext1
     * </pre>
     * @return
     */
    public ErrorContext store() {
        ErrorContext newContext = new ErrorContext();
        newContext.stored = this;
        LOCAL.set(newContext);
        return LOCAL.get();
    }

    /**
     * 获取当前对象的{@link #stored}：如果为null，返回{@link #LOCAL}.get()，通常就是当前对象；如果不为null，则将获得的{@link #stored}设置到{@link #LOCAL}，然后将当前对象的{@link #stored}变量置为null
     * （相当于一个弹栈操作，在此方法前调用{@link #store()}进行压栈操作，然后调用本方法），示例：
     * <pre>
     * in same Thread:
     * 前置条件：
     * ThreadLocal ---thread id---> errorContext1 ---stored---> errorContext2 ---stored---> errorContext3
     * 执行动作:
     * errorContext1.recall()
     * 结果：
     * ThreadLocal ---thread id---> errorContext2 ---stored---> errorContext3
     * errorContext1 ---stored---> null
     * </pre>
     * @return
     */
    public ErrorContext recall() {
        if (stored != null) {
            LOCAL.set(stored);
            stored = null;
        }
        return LOCAL.get();
    }

    /**
     * 将传入的{@code resource}设置到{@link #resource}然后返回当前{@link ErrorContext}实例以便进行函数式编程
     * @param resource
     * @return
     */
    public ErrorContext resource(String resource) {
        this.resource = resource;
        return this;
    }

    /**
     * 将传入的{@code activity}设置到{@link #activity}然后返回当前{@link ErrorContext}实例以便进行函数式编程
     * @param activity
     * @return
     */
    public ErrorContext activity(String activity) {
        this.activity = activity;
        return this;
    }

    /**
     * 将传入的{@code object}设置到{@link #object}然后返回当前{@link ErrorContext}实例以便进行函数式编程
     * @param object
     * @return
     */
    public ErrorContext object(String object) {
        this.object = object;
        return this;
    }

    /**
     * 将传入的{@code message}设置到{@link #message}然后返回当前{@link ErrorContext}实例以便进行函数式编程
     * @param message
     * @return
     */
    public ErrorContext message(String message) {
        this.message = message;
        return this;
    }

    /**
     * 将传入的{@code sql}设置到{@link #sql}然后返回当前{@link ErrorContext}实例以便进行函数式编程
     * @param sql
     * @return
     */
    public ErrorContext sql(String sql) {
        this.sql = sql;
        return this;
    }

    /**
     * 将传入的{@code cause}设置到{@link #cause}然后返回当前{@link ErrorContext}实例以便进行函数式编程
     * @param cause
     * @return
     */
    public ErrorContext cause(Throwable cause) {
        this.cause = cause;
        return this;
    }

    /**
     * 将当前{@link ErrorContext}实例的所有成员变量设置为null并清空{@link #LOCAL}中的所有{@link ErrorContext}实例，然后返回当前实例以便进行函数式编程
     * @return
             */
    public ErrorContext reset() {
        resource = null;
        activity = null;
        object = null;
        message = null;
        sql = null;
        cause = null;
        LOCAL.remove();
        return this;
    }

    /**
     * 拼接所有成员变量作为字符串返回
     * @return
     */
    @Override
    public String toString() {
        StringBuilder description = new StringBuilder();

        // message
        if (this.message != null) {
            description.append(LINE_SEPARATOR);
            description.append("### ");
            description.append(this.message);
        }

        // resource
        if (resource != null) {
            description.append(LINE_SEPARATOR);
            description.append("### The error may exist in ");
            description.append(resource);
        }

        // object
        if (object != null) {
            description.append(LINE_SEPARATOR);
            description.append("### The error may involve ");
            description.append(object);
        }

        // activity
        if (activity != null) {
            description.append(LINE_SEPARATOR);
            description.append("### The error occurred while ");
            description.append(activity);
        }

        // activity
        if (sql != null) {
            description.append(LINE_SEPARATOR);
            description.append("### SQL: ");
            description.append(sql.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ').trim());
        }

        // cause
        if (cause != null) {
            description.append(LINE_SEPARATOR);
            description.append("### Cause: ");
            description.append(cause.toString());
        }

        return description.toString();
    }

}
