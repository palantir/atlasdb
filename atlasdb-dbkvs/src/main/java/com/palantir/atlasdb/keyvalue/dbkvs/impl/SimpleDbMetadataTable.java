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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class SimpleDbMetadataTable implements DbMetadataTable {
    protected final String tableName;
    protected final ConnectionSupplier conns;
    private final DdlConfig config;

    public SimpleDbMetadataTable(String tableName,
                                 ConnectionSupplier conns,
                                 DdlConfig config) {
        this.tableName = tableName;
        this.conns = conns;
        this.config = config;
    }

    @Override
    public boolean exists() {
        return conns.get().selectExistsUnregisteredQuery(
                "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                tableName.toLowerCase());
    }

    @Override
    @SuppressWarnings("deprecation")
    public byte[] getMetadata() {
        AgnosticResultSet results = conns.get().selectResultSetUnregisteredQuery(
                "SELECT value FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                tableName.toLowerCase());
        if (results.size() < 1) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        } else {
            return MoreObjects.firstNonNull(
                    Iterables.getOnlyElement(results.rows()).getBytes("value"),
                    PtBytes.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void putMetadata(byte[] metadata) {
        Preconditions.checkArgument(exists(), "Table %s does not exist.", tableName);
        conns.get().updateUnregisteredQuery(
                "UPDATE " + config.metadataTable().getQualifiedName() + " SET value = ? WHERE table_name = ?",
                metadata,
                tableName.toLowerCase());
    }
}
