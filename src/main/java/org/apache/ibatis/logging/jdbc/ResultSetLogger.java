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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.Set;

/**
 * ResultSet proxy to add logging
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 *
 */
public final class ResultSetLogger extends BaseJdbcLogger implements InvocationHandler {

    /**
     * {@link ResultSetMetaData#getColumnType(int)}为"blob"类型的所有{@link Types}集合
     */
    private static Set<Integer> BLOB_TYPES = new HashSet<>();
    /**
     * 用来判断是否是第一行，打印字段名城
     */
    private boolean first = true;
    /**
     * 用来累加行数
     */
    private int rows;
    /**
     * 被代理的{@link ResultSet}对象
     */
    private final ResultSet rs;
    /**
     * 记录blob类型的字段索引，待{@link #printColumnValues(int)}要用到
     */
    private final Set<Integer> blobColumns = new HashSet<>();

    static {
        BLOB_TYPES.add(Types.BINARY);
        BLOB_TYPES.add(Types.BLOB);
        BLOB_TYPES.add(Types.CLOB);
        BLOB_TYPES.add(Types.LONGNVARCHAR);
        BLOB_TYPES.add(Types.LONGVARBINARY);
        BLOB_TYPES.add(Types.LONGVARCHAR);
        BLOB_TYPES.add(Types.NCLOB);
        BLOB_TYPES.add(Types.VARBINARY);
    }

    private ResultSetLogger(ResultSet rs, Log statementLog, int queryStack) {
        super(statementLog, queryStack);
        this.rs = rs;
    }

    /**
     * <ol>
     *     <li>
     *         如果方法是{@link ResultSet#next()}方法，查看方法是否返回true：
     *         <ul>
     *             <li>
     *                 是true则说明还没有到达最后一行，累加行数，然后判断是否{@link #isTraceEnabled()}，是则调用{@link #printColumnHeaders(ResultSetMetaData, int)}和{@link #printColumnValues(int)}打印{@link #rs}的内容；
     *             </li>
     *             <li>
     *                 否则说明到达最后一行，判断{@link #isDebugEnabled()}，输出累加的行数
     *             </li>
     *         </ul>
     *     </li>
     *     <li>
     *         其他方法照样执行
     *     </li>
     *     <li>
     *         最后调用{@link #clearColumnInfo()}清除字段值缓存
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
            Object o = method.invoke(rs, params);
            if ("next".equals(method.getName())) {
                if (((Boolean) o)) {
                    rows++;
                    if (isTraceEnabled()) {
                        ResultSetMetaData rsmd = rs.getMetaData();
                        final int columnCount = rsmd.getColumnCount();
                        if (first) {
                            first = false;
                            printColumnHeaders(rsmd, columnCount);
                        }
                        printColumnValues(columnCount);
                    }
                } else {
                    debug("     Total: " + rows, false);
                }
            }
            clearColumnInfo();
            return o;
        } catch (Throwable t) {
            throw ExceptionUtil.unwrapThrowable(t);
        }
    }

    /**
     * 构建字段名字符串，示例："<====    Columns: id, root_id, parent_id, code, name, link, attrs, lv, rv, sort, status, instance_id, create_time, update_time, create_person, update_person, tenant_id, dr, extension"
     * <p>
     *     如果遇到是Blob类型的（包含在{@link #BLOB_TYPES}中）则记录下字段的索引，在打印字段值的时候作特殊处理
     * </p>
     *
     * @param rsmd
     * @param columnCount
     * @throws SQLException
     */
    private void printColumnHeaders(ResultSetMetaData rsmd, int columnCount) throws SQLException {
        StringBuilder row = new StringBuilder();
        row.append("   Columns: ");
        for (int i = 1; i <= columnCount; i++) {
            if (BLOB_TYPES.contains(rsmd.getColumnType(i))) {
                blobColumns.add(i);
            }
            String colname = rsmd.getColumnLabel(i);
            row.append(colname);
            if (i != columnCount) {
                row.append(", ");
            }
        }
        trace(row.toString(), false);
    }

    /**
     * 构建所有行字段值字符串，示例："<====        Row: 16, 0, 0, 212, 马甲, null, null, null, null, 0, 1, null, 2019-08-17 00:24:50.0, 2019-08-17 00:24:50.0, vicutu data migration, vicutu data migration, 1, 0, null"
     * <p>
     *     如果遇到是Blob类型的字段（{@link #BLOB_TYPES}中记录了该类型字段的索引），则显示为字符串"{@code <<BLOB>>}"，否则正常调用{@link #rs}.getString(i)获取，如果获取失败则显示"{@code <<Cannot Display>>}"
     * </p>
     *
     * @param columnCount
     */
    private void printColumnValues(int columnCount) {
        StringBuilder row = new StringBuilder();
        row.append("       Row: ");
        for (int i = 1; i <= columnCount; i++) {
            String colname;
            try {
                if (blobColumns.contains(i)) {
                    colname = "<<BLOB>>";
                } else {
                    colname = rs.getString(i);
                }
            } catch (SQLException e) {
                // generally can't call getString() on a BLOB column
                colname = "<<Cannot Display>>";
            }
            row.append(colname);
            if (i != columnCount) {
                row.append(", ");
            }
        }
        trace(row.toString(), false);
    }

    /**
     * Creates a logging version of a ResultSet
     * 构建{@link ResultSet}的代理对象{@link ResultSetLogger}
     *
     * @param rs - the ResultSet to proxy
     * @return - the ResultSet with logging
     */
    public static ResultSet newInstance(ResultSet rs, Log statementLog, int queryStack) {
        InvocationHandler handler = new ResultSetLogger(rs, statementLog, queryStack);
        ClassLoader cl = ResultSet.class.getClassLoader();
        return (ResultSet) Proxy.newProxyInstance(cl, new Class[]{ResultSet.class}, handler);
    }

    /**
     * Get the wrapped result set
     *
     * @return the resultSet
     */
    public ResultSet getRs() {
        return rs;
    }

}
