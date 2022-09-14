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
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;
import com.palantir.conjure.java.jackson.optimizations.ObjectMapperOptimizations;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.IOException;
import java.util.Optional;

public final class WriteReferencePersister {
    private static final byte[] writePrefix = {1};
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModules(ObjectMapperOptimizations.createModules());
    private static final StoredWriteReference DUMMY = ImmutableStoredWriteReference.of(PtBytes.EMPTY_BYTE_ARRAY);

    private final SweepTableIndices tableIndices;

    public WriteReferencePersister(SweepTableIndices tableIndices) {
        this.tableIndices = tableIndices;
    }

    public Optional<WriteReference> unpersist(StoredWriteReference writeReference) {
        return writeReference.accept(new StoredWriteReference.Visitor<>() {
            @Override
            public Optional<WriteReference> visitJson(byte[] ref) {
                try {
                    return Optional.of(OBJECT_MAPPER.readValue(ref, WriteReference.class));
                } catch (IOException e) {
                    throw new SafeRuntimeException("Exception hydrating object.");
                }
            }

            @Override
            public Optional<WriteReference> visitTableNameAsStringBinary(byte[] ref) {
                int offset = 1;
                String tableReferenceString = EncodingUtils.decodeVarString(ref, offset);
                TableReference tableReference = TableReference.fromString(tableReferenceString);
                offset += EncodingUtils.sizeOfVarString(tableReferenceString);
                byte[] row = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(row);
                byte[] column = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(column);
                long isTombstone = EncodingUtils.decodeUnsignedVarLong(ref, offset);
                return Optional.of(ImmutableWriteReference.builder()
                        .tableRef(tableReference)
                        .cell(Cell.create(row, column))
                        .isTombstone(isTombstone == 1)
                        .build());
            }

            @Override
            public Optional<WriteReference> visitTableIdBinary(byte[] ref) {
                int offset = 1;
                int tableId = Ints.checkedCast(EncodingUtils.decodeUnsignedVarLong(ref, offset));
                TableReference tableReference = tableIndices.getTableReference(tableId);
                offset += EncodingUtils.sizeOfUnsignedVarLong(tableId);
                byte[] row = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(row);
                byte[] column = EncodingUtils.decodeSizedBytes(ref, offset);
                offset += EncodingUtils.sizeOfSizedBytes(column);
                long isTombstone = EncodingUtils.decodeUnsignedVarLong(ref, offset);
                return Optional.of(ImmutableWriteReference.builder()
                        .tableRef(tableReference)
                        .cell(Cell.create(row, column))
                        .isTombstone(isTombstone == 1)
                        .build());
            }

            @Override
            public Optional<WriteReference> visitDummy() {
                return Optional.empty();
            }
        });
    }

    public StoredWriteReference persist(Optional<WriteReference> writeReference) {
        if (writeReference.isEmpty()) {
            return DUMMY;
        }
        WriteReference writeRef = writeReference.get();
        byte[] tableId = EncodingUtils.encodeUnsignedVarLong(tableIndices.getTableId(writeRef.tableRef()));
        byte[] row = EncodingUtils.encodeSizedBytes(writeRef.cell().getRowName());
        byte[] column = EncodingUtils.encodeSizedBytes(writeRef.cell().getColumnName());
        byte[] isTombstone = EncodingUtils.encodeUnsignedVarLong(writeRef.isTombstone() ? 1 : 0);
        return ImmutableStoredWriteReference.of(EncodingUtils.add(writePrefix, tableId, row, column, isTombstone));
    }
}
