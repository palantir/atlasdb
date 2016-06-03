/**
 * Copyright 2015 Palantir Technologies
 *
 * Licensed under the BSD-3 License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.palantir.nexus.db.pool;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.palantir.nexus.db.manager.DBConfig;

public class JdbcConfig {

    public static Properties getPropertiesFromDbConfig(DBConfig dbConfig) {
        return getPropertiesFromDbConfig(
                dbConfig.getDbLogin(),
                dbConfig.getDbDecryptedPassword(),
                dbConfig.getSocketTimeoutSeconds(),
                dbConfig.getConnectTimeoutSeconds());
    }

    public static Properties getPropertiesFromDbConfig(String dbLogin,
                                                       String dbDecryptedPassword,
                                                       Integer socketTimeout,
                                                       Integer connectTimeout) {
        Properties properties = new Properties();

        properties.setProperty("user", dbLogin);
        if (dbDecryptedPassword != null) {
            properties.setProperty("password", dbDecryptedPassword);
        }

        if (socketTimeout != null) {
            /* Oracle */
            properties.setProperty("oracle.net.keepAlive", "true");
            properties.setProperty("oracle.jdbc.ReadTimeout", Long.toString(TimeUnit.SECONDS.toMillis(socketTimeout)));

            /* Postgres */
            properties.setProperty("tcpKeepAlive", "true");
            properties.setProperty("socketTimeout", socketTimeout.toString());
        }
        if (connectTimeout != null) {
            /* Oracle */
            properties.setProperty("oracle.net.CONNECT_TIMEOUT", Long.toString(TimeUnit.SECONDS.toMillis(connectTimeout)));

            /* Postgres */
            properties.setProperty("connectTimeout", connectTimeout.toString());
            properties.setProperty("loginTimeout", connectTimeout.toString());
        }

        properties.setProperty("oracle.jdbc.maxCachedBufferSize", "100000");
        return properties;
    }

}
