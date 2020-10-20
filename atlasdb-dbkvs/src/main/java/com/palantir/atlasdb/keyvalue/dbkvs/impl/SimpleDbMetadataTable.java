/*
 * (c) Copyright 2018 Palantir Technologies Inc. All rights reserved.
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
package com.palantir.atlasdb.keyvalue.dbkvs.impl;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.TableReference;
import com.palantir.atlasdb.keyvalue.dbkvs.DdlConfig;
import com.palantir.nexus.db.sql.AgnosticResultSet;

public class SimpleDbMetadataTable implements DbMetadataTable {
    protected final TableReference tableRef;
    protected final ConnectionSupplier conns;
    private final DdlConfig config;

    public SimpleDbMetadataTable(TableReference tableRef, ConnectionSupplier conns, DdlConfig config) {
        this.tableRef = tableRef;
        this.conns = conns;
        this.config = config;
    }

    @Override
    public boolean exists() {
        return conns.get()
                .selectExistsUnregisteredQuery(
                        "SELECT 1 FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        tableRef.getQualifiedName());
    }

    @Override
    @SuppressWarnings("deprecation")
    public byte[] getMetadata() {
        AgnosticResultSet results = conns.get()
                .selectResultSetUnregisteredQuery(
                        "SELECT value FROM " + config.metadataTable().getQualifiedName() + " WHERE table_name = ?",
                        tableRef.getQualifiedName());
        if (results.size() < 1) {
            return PtBytes.EMPTY_BYTE_ARRAY;
        } else {
            return MoreObjects.firstNonNull(
                    Iterables.getOnlyElement(results.rows()).getBytes("value"), PtBytes.EMPTY_BYTE_ARRAY);
        }
    }

    @Override
    public void putMetadata(byte[] metadata) {
        Preconditions.checkArgument(exists(), "Table %s does not exist.", tableRef);
        conns.get()
                .updateUnregisteredQuery(
                        "UPDATE " + config.metadataTable().getQualifiedName() + " SET value = ? WHERE table_name = ?",
                        metadata,
                        tableRef.getQualifiedName());
    }
}
