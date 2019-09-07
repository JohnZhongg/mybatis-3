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
import org.apache.ibatis.reflection.ArrayUtil;

import java.sql.Array;
import java.sql.SQLException;
import java.util.*;

/**
 * Base class for proxies to do logging
 *
 * 用于jdbc活动日志记录的基本logger类
 *
 * @author Clinton Begin
 * @author Eduardo Macarron
 */
public abstract class BaseJdbcLogger {

    /**
     * {@link java.sql.PreparedStatement}的各种setXXX方法的名称集合
     */
    protected static final Set<String> SET_METHODS = new HashSet<>();
    /**
     * {@link java.sql.Statement}的execute和executeQuery和executeUpdate和addBatch方法名集合
     */
    protected static final Set<String> EXECUTE_METHODS = new HashSet<>();

    /**
     * 字段值map缓存，主要在{@link java.sql.PreparedStatement}中用的，key是字段索引或者字段名称（即为setXxx方法的第一个入参），value即为setXxx中给字段设置的值（即为setXxx方法的第二个入参）
     */
    private final Map<Object, Object> columnMap = new HashMap<>();

    /**
     * 字段索引或者字段名称（即为setXxx方法的第一个入参）
     */
    private final List<Object> columnNames = new ArrayList<>();
    /**
     * 字段值（即为setXxx方法的第二个入参）
     */
    private final List<Object> columnValues = new ArrayList<>();

    /**
     * 日志记录类
     */
    protected Log statementLog;
    /**
     * 在{@link #prefix(boolean)}中用来计算"="的个数，在{@link #BaseJdbcLogger(Log, int)}中初始化，如果传入的值是0则赋值为1，其他的入参都是直接赋值，即保证{@link #queryStack}的最小值是1
     */
    protected int queryStack;

    /*
     * Default constructor
     */
    public BaseJdbcLogger(Log log, int queryStack) {
        this.statementLog = log;
        if (queryStack == 0) {
            this.queryStack = 1;
        } else {
            this.queryStack = queryStack;
        }
    }

    static {
        SET_METHODS.add("setString");
        SET_METHODS.add("setNString");
        SET_METHODS.add("setInt");
        SET_METHODS.add("setByte");
        SET_METHODS.add("setShort");
        SET_METHODS.add("setLong");
        SET_METHODS.add("setDouble");
        SET_METHODS.add("setFloat");
        SET_METHODS.add("setTimestamp");
        SET_METHODS.add("setDate");
        SET_METHODS.add("setTime");
        SET_METHODS.add("setArray");
        SET_METHODS.add("setBigDecimal");
        SET_METHODS.add("setAsciiStream");
        SET_METHODS.add("setBinaryStream");
        SET_METHODS.add("setBlob");
        SET_METHODS.add("setBoolean");
        SET_METHODS.add("setBytes");
        SET_METHODS.add("setCharacterStream");
        SET_METHODS.add("setNCharacterStream");
        SET_METHODS.add("setClob");
        SET_METHODS.add("setNClob");
        SET_METHODS.add("setObject");
        SET_METHODS.add("setNull");

        EXECUTE_METHODS.add("execute");
        EXECUTE_METHODS.add("executeUpdate");
        EXECUTE_METHODS.add("executeQuery");
        EXECUTE_METHODS.add("addBatch");
    }

    /**
     * 设置字段值缓存
     *
     * @param key
     * @param value
     */
    protected void setColumn(Object key, Object value) {
        columnMap.put(key, value);
        columnNames.add(key);
        columnValues.add(value);
    }

    /**
     * {@link #columnMap}.get(key)
     *
     * @param key
     * @return
     */
    protected Object getColumn(Object key) {
        return columnMap.get(key);
    }

    /**
     * 将所有通过setXxx设置的字段值列表（即{@link #columnValues}）转换成字符串并去掉"["和"]"返回，其中对于null和{@link Array}的字段值进行特殊处理
     *
     * @return
     */
    protected String getParameterValueString() {
        List<Object> typeList = new ArrayList<>(columnValues.size());
        for (Object value : columnValues) {
            if (value == null) {
                typeList.add("null");
            } else {
                typeList.add(objectValueString(value) + "(" + value.getClass().getSimpleName() + ")");
            }
        }
        final String parameters = typeList.toString();
        return parameters.substring(1, parameters.length() - 1);
    }

    /**
     * 如果{@code value}是{@link Array}，则调用{@link ArrayUtil#toString(Object)}；否则正常返回{@link Object#toString()}
     *
     * @param value
     * @return
     */
    protected String objectValueString(Object value) {
        if (value instanceof Array) {
            try {
                return ArrayUtil.toString(((Array) value).getArray());
            } catch (SQLException e) {
                return value.toString();
            }
        }
        return value.toString();
    }

    protected String getColumnString() {
        return columnNames.toString();
    }

    /**
     * 清除所有字段值缓存：{@link #columnMap}、{@link #columnNames}、{@link #columnValues}
     */
    protected void clearColumnInfo() {
        columnMap.clear();
        columnNames.clear();
        columnValues.clear();
    }

    /**
     * 传入{@code original}为一个sql，本方法调用{@link StringTokenizer}消除sql中的空白字符转成一个空格然后返回
     *
     * @param original
     * @return
     */
    protected String removeBreakingWhitespace(String original) {
        //构建StringTokenizer对象，分隔符是各种空白字符
        StringTokenizer whitespaceStripper = new StringTokenizer(original);
        StringBuilder builder = new StringBuilder();
        //使用空白字符分割传入sql，被空白字符分割开的字符串称之为token，调用hasMoreTokens的时候表示如果还有token就移动指针指向token，调用nextToken即可获得，然后拼接一个空格即可
        while (whitespaceStripper.hasMoreTokens()) {
            builder.append(whitespaceStripper.nextToken());
            builder.append(" ");
        }
        return builder.toString();
    }

    /**
     * {@link #statementLog}的{@link Log#isDebugEnabled()}
     *
     * @return
     */
    protected boolean isDebugEnabled() {
        return statementLog.isDebugEnabled();
    }

    /**
     * {@link #statementLog}的{@link Log#isTraceEnabled()}
     *
     * @return
     */
    protected boolean isTraceEnabled() {
        return statementLog.isTraceEnabled();
    }

    /**
     * 如果{@link #statementLog}的{@link Log#isDebugEnabled()}为true，则调用{@link #prefix(boolean)}构建前缀，拼接{@code text}，并调用{@link #statementLog}的{@link Log#debug(String)}记录
     *
     * @param text
     * @param input
     */
    protected void debug(String text, boolean input) {
        if (statementLog.isDebugEnabled()) {
            statementLog.debug(prefix(input) + text);
        }
    }

    /**
     * 如果{@link #statementLog}的{@link Log#isTraceEnabled()}为true，则调用{@link #prefix(boolean)}构建前缀，拼接{@code text}，并调用{@link #statementLog}的{@link Log#trace(String)}记录
     *
     * @param text
     * @param input
     */
    protected void trace(String text, boolean input) {
        if (statementLog.isTraceEnabled()) {
            statementLog.trace(prefix(input) + text);
        }
    }

    /**
     * 构建所谓的prefix字符串，字符串的总长度为{@link #queryStack}*2+2
     * <ol>
     *     <li>
     *         最后一个字符为空格" "
     *     </li>
     *     <li>
     *         如果{@code isInput}是true，则倒数第二个字符为">"，前面所有的字符为"="
     *     </li>
     *     <li>
     *         如果{@code isInput}是false，则第一个字符为"<"，后面截止到倒数第二个字符都为"="
     *     </li>
     *     <li>
     *         {@link #queryStack}*2表示"="的个数。
     *     </li>
     * </ol>
     * <ul>
     *     <li>
     *         {@link #queryStack}为2，传入{@code isInput}为true得到"====> "
     *     </li>
     *     <li>
     *         {@link #queryStack}为2，传入{@code isInput}为false得到"<==== "
     *     </li>
     * </ul>
     * @param isInput 是否使用右箭头
     * @return
     */
    private String prefix(boolean isInput) {
        char[] buffer = new char[queryStack * 2 + 2];
        Arrays.fill(buffer, '=');
        buffer[queryStack * 2 + 1] = ' ';
        if (isInput) {
            buffer[queryStack * 2] = '>';
        } else {
            buffer[0] = '<';
        }
        return new String(buffer);
    }

}
