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
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * PreparedStatement proxy to add logging
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 *
 */
public final class PreparedStatementLogger extends BaseJdbcLogger implements InvocationHandler {

    private final PreparedStatement statement;

    private PreparedStatementLogger(PreparedStatement stmt, Log statementLog, int queryStack) {
        super(statementLog, queryStack);
        this.statement = stmt;
    }

    /**
     * <ol>
     *     <li>
     *         如果传入的方法{@code method}是{@link Object}的方法，将执行对象转换为当前代理执行对象进行方法执行（主要针对toString() hashCode()??）
     *     </li>
     *     <li>
     *         <p>
     *             如果执行的方法包含在{@link #EXECUTE_METHODS}方法，先打印所有通过setXxx方法设置的字段值（{@link #getParameterValueString()}获取）（这里默认认为在执行{@link #EXECUTE_METHODS}类方法之前已经执行了{@link #SET_METHODS}，要么就是无参）。
     *             但是{@link PreparedStatement}的{@link #EXECUTE_METHODS}类方法也是可以直接传sql的（在构建{@link PreparedStatement}对象的时候随便传一个字符串），不用传参，这时候就不打印参数了，可能这里的语义就是只打印预编译的时候setXxx的参数
     *         </p>
     *         <ul>
     *             判断是否是executeQuery方法：
     *             <li>
     *                 如果是，则执行方法然后根据返回的{@link ResultSet}调用{@link ResultSetLogger#newInstance(ResultSet, Log, int)}构建ResultSet的代理对象{@link ResultSetLogger}
     *             </li>
     *             <li>
     *                 否则，正常执行方法
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         如果执行的方法包含在{@link #SET_METHODS}（即为setXxx）方法，则调用{@link #setColumn(Object, Object)}方法设置字段值相关缓存，然后使用真实的{@link PreparedStatement}对象{@link #statement}执行方法
     *     </li>
     *     <li>
     *         如果方法是"getResultSet"，执行方法然后根据返回的{@link ResultSet}调用{@link ResultSetLogger#newInstance(ResultSet, Log, int)}构建ResultSet的代理对象{@link ResultSetLogger}
     *     </li>
     *     <li>
     *         如果方法是"getUpdateCount"，正常执行然后返回count并打印日志
     *     </li>
     *     <li>
     *         其他方法则照常执行
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
    public Object invoke(Object proxy, Method method, Object[] params) throws Throwable {
        try {
            if (Object.class.equals(method.getDeclaringClass())) {
                return method.invoke(this, params);
            }
            if (EXECUTE_METHODS.contains(method.getName())) {
                if (isDebugEnabled()) {
                    debug("Parameters: " + getParameterValueString(), true);
                }
                clearColumnInfo();
                if ("executeQuery".equals(method.getName())) {
                    ResultSet rs = (ResultSet) method.invoke(statement, params);
                    return rs == null ? null : ResultSetLogger.newInstance(rs, statementLog, queryStack);
                } else {
                    return method.invoke(statement, params);
                }
            } else if (SET_METHODS.contains(method.getName())) {
                if ("setNull".equals(method.getName())) {
                    setColumn(params[0], null);
                } else {
                    setColumn(params[0], params[1]);
                }
                return method.invoke(statement, params);
            } else if ("getResultSet".equals(method.getName())) {
                ResultSet rs = (ResultSet) method.invoke(statement, params);
                return rs == null ? null : ResultSetLogger.newInstance(rs, statementLog, queryStack);
            } else if ("getUpdateCount".equals(method.getName())) {
                int updateCount = (Integer) method.invoke(statement, params);
                if (updateCount != -1) {
                    debug("   Updates: " + updateCount, false);
                }
                return updateCount;
            } else {
                return method.invoke(statement, params);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.unwrapThrowable(t);
        }
    }

    /**
     * Creates a logging version of a PreparedStatement
     * 构建{@link PreparedStatement}的代理对象{@link PreparedStatementLogger}返回
     *
     * @param stmt - the statement
     * @param statementLog - the statement log
     * @param queryStack - the query stack
     * @return - the proxy
     */
    public static PreparedStatement newInstance(PreparedStatement stmt, Log statementLog, int queryStack) {
        InvocationHandler handler = new PreparedStatementLogger(stmt, statementLog, queryStack);
        ClassLoader cl = PreparedStatement.class.getClassLoader();
        return (PreparedStatement) Proxy.newProxyInstance(cl, new Class[]{PreparedStatement.class, CallableStatement.class}, handler);
    }

    /**
     * Return the wrapped prepared statement
     *
     * @return the PreparedStatement
     */
    public PreparedStatement getPreparedStatement() {
        return statement;
    }

}
