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
package com.palantir.atlasdb.keyvalue.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.google.common.primitives.Ints;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;
import com.palantir.conjure.java.jackson.optimizations.ObjectMapperOptimizations;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.IOException;

public final class WriteReferencePersister {
    private static final byte[] writePrefix = {1};
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModules(ObjectMapperOptimizations.createModules());

    private final SweepTableIndices tableIndices;

    public WriteReferencePersister(SweepTableIndices tableIndices) {
        this.tableIndices = tableIndices;
    }

    public WriteReference unpersist(StoredWriteReference writeReference) {
        return writeReference.accept(new StoredWriteReference.Visitor<WriteReference>() {
            @Override
            public WriteReference visitJson(byte[] ref) {
                try {
                    return OBJECT_MAPPER.readValue(ref, WriteReference.class);
                } catch (IOException e) {
                    throw new SafeRuntimeException("Exception hydrating object.");
                }
            }

            @Override
            public WriteReference visitTableNameAsStringBinary(byte[] ref) {
                int offset = 1;
                String tableReferenceString = EncodingUtils.decodeVarString(ref, offset);
                TableReference tableReference = TableReference.fromString(tableReferenceString);
                offset += EncodingUtils.sizeOfVarString(tableReferenceString);
                byte[] row = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(row);
                byte[] column = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(column);
                long isTombstone = EncodingUtils.decodeUnsignedVarLong(ref, offset);
                return ImmutableWriteReference.builder()
                        .tableRef(tableReference)
                        .cell(Cell.create(row, column))
                        .isTombstone(isTombstone == 1)
                        .build();
            }

            @Override
            public WriteReference visitTableIdBinary(byte[] ref) {
                int offset = 1;
                int tableId = Ints.checkedCast(EncodingUtils.decodeUnsignedVarLong(ref, offset));
                TableReference tableReference = tableIndices.getTableReference(tableId);
                offset += EncodingUtils.sizeOfUnsignedVarLong(tableId);
                byte[] row = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(row);
                byte[] column = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(column);
                long isTombstone = EncodingUtils.decodeUnsignedVarLong(ref, offset);
                return ImmutableWriteReference.builder()
                        .tableRef(tableReference)
                        .cell(Cell.create(row, column))
                        .isTombstone(isTombstone == 1)
                        .build();
            }
        });
    }

    public StoredWriteReference persist(WriteReference writeReference) {
        byte[] tableId = EncodingUtils.encodeUnsignedVarLong(tableIndices.getTableId(writeReference.tableRef()));
        byte[] row = EncodingUtils.encodeSizedBytes(writeReference.cell().getRowName());
        byte[] column = EncodingUtils.encodeSizedBytes(writeReference.cell().getColumnName());
        byte[] isTombstone = EncodingUtils.encodeUnsignedVarLong(writeReference.isTombstone() ? 1 : 0);
        return ImmutableStoredWriteReference.of(EncodingUtils.add(writePrefix, tableId, row, column, isTombstone));
    }
}
