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
package org.apache.ibatis.datasource.jndi;

import org.apache.ibatis.datasource.DataSourceException;
import org.apache.ibatis.datasource.DataSourceFactory;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.util.Map.Entry;
import java.util.Properties;

/**
 * 基于 JNDI 的 DataSourceFactory 实现类<br>
 *
 * 参考单元测试：{@link JndiDataSourceFactoryTest#shouldRetrieveDataSourceFromJNDI()}
 * @author Clinton Begin
 */
public class JndiDataSourceFactory implements DataSourceFactory {

    /**
     * 挂载在Java jndi根命名空间下的自定义命名空间
     */
    public static final String INITIAL_CONTEXT = "initial_context";
    /**
     * jndi数据源在Java命名空间中的name
     */
    public static final String DATA_SOURCE = "data_source";
    /**
     * java jndi上下文的初始化环境变量
     */
    public static final String ENV_PREFIX = "env.";

    private DataSource dataSource;

    @Override
    public void setProperties(Properties properties) {
        try {
            // InitialContext实现了Context接口，是java中提供的类似于命名空间的上下文，即{name:Object}的形式，可以内部递归，即{name:Context}
            InitialContext initCtx;
            // 从 properteis 中获得InitailContext的系统环境变量，例如"java.naming.factory.initial"就是定义了创建Context所使用的ContextFactory的class全限定名
            Properties env = getEnvProperties(properties);
            // 创建 InitialContext 对象，应该是Java内部的Context的一个副本
            if (env == null) {
                initCtx = new InitialContext();
            } else {
                initCtx = new InitialContext(env);
            }

            // 如果Properties中定义了"initial_context"和"data_source"，说明用户自定义了一个命名空间挂载在Java根命名空间下，将datasource对象挂载在自定义命名空间中
            if (properties.containsKey(INITIAL_CONTEXT)
                    && properties.containsKey(DATA_SOURCE)) {
                // 从Java的根命名空间获取自定义命名空间"initial_context"
                Context ctx = (Context) initCtx.lookup(properties.getProperty(INITIAL_CONTEXT));
                // 从自定义命名空间获取"data_source"
                dataSource = (DataSource) ctx.lookup(properties.getProperty(DATA_SOURCE));
            } else if (properties.containsKey(DATA_SOURCE)) {
            // 如果Properties中仅仅定义了"data_source"，说明用户将datasource对象直接命名在了Java根命名空间下，直接查找即可
                dataSource = (DataSource) initCtx.lookup(properties.getProperty(DATA_SOURCE));
            }
        } catch (NamingException e) {
            throw new DataSourceException("There was an error configuring JndiDataSourceTransactionPool. Cause: " + e, e);
        }
    }

    @Override
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * 获取以{@link JndiDataSourceFactory#ENV_PREFIX} 为前缀的properties
     *
     * @param allProps 配置的所有properties
     * @return
     */
    private static Properties getEnvProperties(Properties allProps) {
        final String PREFIX = ENV_PREFIX;
        Properties contextProperties = null;
        for (Entry<Object, Object> entry : allProps.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.startsWith(PREFIX)) {
                if (contextProperties == null) {
                    contextProperties = new Properties();
                }
                contextProperties.put(key.substring(PREFIX.length()), value);
            }
        }
        return contextProperties;
    }

}
