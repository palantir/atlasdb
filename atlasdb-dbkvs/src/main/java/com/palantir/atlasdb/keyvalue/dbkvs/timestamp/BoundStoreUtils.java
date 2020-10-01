/*
 * (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.palantir.atlasdb.keyvalue.dbkvs.timestamp;

import java.sql.Connection;
import java.sql.SQLException;

public class BoundStoreUtils {

    public BoundStoreUtils() {
        // do not initialize utilities class
    }

    public static boolean hasColumn(Connection connection, String tablePattern, String colNamePattern) throws SQLException {
        return connection.getMetaData()
                .getColumns(null, null, tablePattern, colNamePattern)
                .next();
    }
}
