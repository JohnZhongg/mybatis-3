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
package org.apache.ibatis.logging.jdbc;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.reflection.ExceptionUtil;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

/**
 * Connection proxy to add logging
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 *
 */
public final class ConnectionLogger extends BaseJdbcLogger implements InvocationHandler {

    private final Connection connection;

    private ConnectionLogger(Connection conn, Log statementLog, int queryStack) {
        super(statementLog, queryStack);
        this.connection = conn;
    }

    /**
     * <ol>
     *     <li>
     *         如果传入的方法{@code method}是{@link Object}的方法，将执行对象转换为当前代理执行对象进行方法执行（主要针对toString() hashCode()??）
     *     </li>
     *     <li>
     *         <p>
     *             <b>如果是debug级别，则记录日志</b>
     *         </p>
     *         如果传入的方法名是"prepareStatement"和"prepareCall"，则分别使用真实的{@link #connection}对象执行该方法获取实际的{@link PreparedStatement}然后调用{@link PreparedStatementLogger#newInstance(PreparedStatement, Log, int)}构建{@link PreparedStatement}的日志代理对象{@link PreparedStatementLogger}并返回
     *     </li>
     *     <li>
     *         <p>
     *             <b>createStatement无参，无需记录日志</b>
     *         </p>
     *         如果传入的方法名是"createStatement"，则分别使用真实的{@link #connection}对象执行该方法获取实际的{@link Statement}然后调用{@link StatementLogger#newInstance(Statement, Log, int)}构建{@link Statement}的日志代理对象{@link StatementLogger}并返回
     *     </li>
     *     <li>
     *         其他方法则使用{@link #connection}照样执行
     *     </li>
     * </ol>
     *
     * @param proxy
     * @param method
     * @param params
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] params)
            throws Throwable {
        try {
            if (Object.class.equals(method.getDeclaringClass())) {
                return method.invoke(this, params);
            }
            if ("prepareStatement".equals(method.getName())) {
                if (isDebugEnabled()) {
                    //第一个参数是sql
                    debug(" Preparing: " + removeBreakingWhitespace((String) params[0]), true);
                }
                PreparedStatement stmt = (PreparedStatement) method.invoke(connection, params);
                stmt = PreparedStatementLogger.newInstance(stmt, statementLog, queryStack);
                return stmt;
            } else if ("prepareCall".equals(method.getName())) {
                if (isDebugEnabled()) {
                    //第一个参数是sql
                    debug(" Preparing: " + removeBreakingWhitespace((String) params[0]), true);
                }
                PreparedStatement stmt = (PreparedStatement) method.invoke(connection, params);
                stmt = PreparedStatementLogger.newInstance(stmt, statementLog, queryStack);
                return stmt;
            } else if ("createStatement".equals(method.getName())) {
                Statement stmt = (Statement) method.invoke(connection, params);
                stmt = StatementLogger.newInstance(stmt, statementLog, queryStack);
                return stmt;
            } else {
                return method.invoke(connection, params);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.unwrapThrowable(t);
        }
    }

    /**
     * Creates a logging version of a connection
     * <p>调用{@link ConnectionLogger#ConnectionLogger(Connection, Log, int)}创建一个{@link InvocationHandler}并使用jdk动态代理创建一个{@link Connection}的代理对象</p>
     *
     * @param conn - the original connection
     * @return - the connection with logging
     */
    public static Connection newInstance(Connection conn, Log statementLog, int queryStack) {
        InvocationHandler handler = new ConnectionLogger(conn, statementLog, queryStack);
        ClassLoader cl = Connection.class.getClassLoader();
        return (Connection) Proxy.newProxyInstance(cl, new Class[]{Connection.class}, handler);
    }

    /**
     * return the wrapped connection
     * <p>
     *     获取实际的Connection对象{@link #connection}
     * </p>
     *
     * @return the connection
     */
    public Connection getConnection() {
        return connection;
    }

}
