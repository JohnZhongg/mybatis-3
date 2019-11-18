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
package org.apache.ibatis.mapping;

import org.apache.ibatis.logging.Log;
import org.apache.ibatis.logging.LogFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Vendor DatabaseId provider
 *
 *
 * It returns database product name as a databaseId
 * If the user provides a properties it uses it to translate database product name
 * key="Microsoft SQL Server", value="ms" will return "ms" 
 * It can return null, if no database product name or 
 * a properties was specified and no translation was found 
 *
 * @author Eduardo Macarron
 */
public class VendorDatabaseIdProvider implements DatabaseIdProvider {

    private static final Log log = LogFactory.getLog(VendorDatabaseIdProvider.class);

    /**
     * Properties 对象
     */
    private Properties properties;

    /**
     * 如果传入的{@code dataSource}是null，将抛出{@link NullPointerException}，否则调用{@link VendorDatabaseIdProvider#getDatabaseName(DataSource)}获取唯一标识，
     * 如果此间发生异常，则记录异常并返回null
     *
     * @param dataSource 数据源
     * @return
     */
    @Override
    public String getDatabaseId(DataSource dataSource) {
        if (dataSource == null) {
            throw new NullPointerException("dataSource cannot be null");
        }
        try {
            return getDatabaseName(dataSource);
        } catch (Exception e) {
            log.error("Could not get a databaseId from dataSource", e);
        }
        return null;
    }

    @Override
    public void setProperties(Properties p) {
        this.properties = p;
    }

    /**
     * 调用{@link VendorDatabaseIdProvider#getDatabaseProductName(DataSource)}获取数据库产品名称：
     * <ol>
     *     <li>
     *         如果有对{@link DatabaseIdProvider}设置properties，则遍历所有property找到数据库产品名包含其key的property的value作为数据库名称返回，如果没有匹配的则返回null
     *     </li>
     *     <li>
     *         如果没有设置properties，则直接返回该数据库产品名称
     *     </li>
     * </ol>
     *
     * @param dataSource
     * @return
     * @throws SQLException
     */
    private String getDatabaseName(DataSource dataSource) throws SQLException {
        String productName = getDatabaseProductName(dataSource);
        if (this.properties != null) {
            for (Map.Entry<Object, Object> property : properties.entrySet()) {
                if (productName.contains((String) property.getKey())) {
                    return (String) property.getValue();
                }
            }
            // no match, return null
            return null;
        }
        return productName;
    }

    /**
     * <ol>
     *     <li>
     *         {@link DataSource#getConnection()}
     *     </li>
     *     <li>
     *         {@link Connection#getMetaData()}
     *     </li>
     *     <li>
     *         {@link DatabaseMetaData#getDatabaseProductName()}
     *     </li>
     * </ol>
     *
     * @param dataSource
     * @return
     * @throws SQLException
     */
    private String getDatabaseProductName(DataSource dataSource) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            DatabaseMetaData metaData = con.getMetaData();
            return metaData.getDatabaseProductName();
        }
    }

}
