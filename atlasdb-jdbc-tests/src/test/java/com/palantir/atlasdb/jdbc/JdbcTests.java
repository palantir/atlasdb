/*
 * (c) Copyright 2016 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.jdbc;

import java.io.File;
import java.util.Properties;

import com.palantir.atlasdb.jdbc.config.ImmutableHikariDataSourceConfiguration;
import com.palantir.atlasdb.keyvalue.jdbc.ImmutableJdbcKeyValueConfiguration;
import com.palantir.atlasdb.keyvalue.jdbc.JdbcKeyValueConfiguration;
import com.palantir.atlasdb.keyvalue.jdbc.JdbcKeyValueService;

public class JdbcTests {

    private JdbcTests() {
        // cannot instantiate
    }

    public static JdbcKeyValueService createEmptyKvs() {
        for (File file : new File("var/data").listFiles()) {
            if (file.getName().endsWith(".db")) {
                file.delete();
            }
        }
        Properties properties = new Properties();
        properties.put("jdbcUrl", "jdbc:h2:./var/data/h2testDb");
        properties.put("username", "sa");
        JdbcKeyValueConfiguration config = ImmutableJdbcKeyValueConfiguration.builder()
                .dataSourceConfig(ImmutableHikariDataSourceConfiguration.builder()
                        .sqlDialect("H2")
                        .properties(properties)
                        .build())
                .build();
        return JdbcKeyValueService.create(config);
    }
}
