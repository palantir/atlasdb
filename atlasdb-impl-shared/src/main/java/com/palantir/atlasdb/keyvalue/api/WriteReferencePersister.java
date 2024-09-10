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
import com.palantir.atlasdb.sweep.queue.config.TargetedSweepInstallConfig;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;
import com.palantir.conjure.java.jackson.optimizations.ObjectMapperOptimizations;
import com.palantir.logsafe.SafeArg;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import com.palantir.logsafe.exceptions.SafeRuntimeException;
import java.io.IOException;
import java.util.Optional;

public final class WriteReferencePersister {
    private static final byte[] ZERO_BYTE = {0};
    private static final byte[] ONE_BYTE = {1};

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModules(ObjectMapperOptimizations.createModules());
    private static final StoredWriteReference DUMMY = ImmutableStoredWriteReference.of(PtBytes.EMPTY_BYTE_ARRAY);

    private final SweepTableIndices tableIndices;
    private final WriteMethod writeMethod;

    WriteReferencePersister(SweepTableIndices tableIndices, WriteMethod writeMethod) {
        this.tableIndices = tableIndices;
        this.writeMethod = writeMethod;
    }

    public static WriteReferencePersister create(
            SweepTableIndices sweepTableIndices,
            TargetedSweepInstallConfig.SweepIndexResetProgressStage resetProgressStage) {
        return new WriteReferencePersister(
                sweepTableIndices,
                resetProgressStage.shouldWriteImmediateFormat()
                        ? WriteMethod.TABLE_NAME_AS_STRING_BINARY
                        : WriteMethod.TABLE_ID_BINARY);
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
        byte[] tableIdentifier = getTableIdentifier(writeRef.tableRef());
        byte[] row = EncodingUtils.encodeSizedBytes(writeRef.cell().getRowName());
        byte[] column = EncodingUtils.encodeSizedBytes(writeRef.cell().getColumnName());
        byte[] isTombstone = EncodingUtils.encodeUnsignedVarLong(writeRef.isTombstone() ? 1 : 0);
        return ImmutableStoredWriteReference.of(
                EncodingUtils.add(writeMethod.getBytePrefix(), tableIdentifier, row, column, isTombstone));
    }

    private byte[] getTableIdentifier(TableReference tableReference) {
        switch (writeMethod) {
            case TABLE_ID_BINARY:
                return EncodingUtils.encodeUnsignedVarLong(tableIndices.getTableId(tableReference));
            case TABLE_NAME_AS_STRING_BINARY:
                return EncodingUtils.encodeVarString(tableReference.toString());
            default:
                throw new SafeIllegalStateException("Unhandled write method", SafeArg.of("writeMethod", writeMethod));
        }
    }

    @SuppressWarnings("ImmutableEnumChecker") // Overhead of needless wrapping is probably undesirable.
    enum WriteMethod {
        TABLE_NAME_AS_STRING_BINARY(ZERO_BYTE),
        TABLE_ID_BINARY(ONE_BYTE);

        private final byte[] bytePrefix;

        WriteMethod(byte[] bytePrefix) {
            this.bytePrefix = bytePrefix;
        }

        byte[] getBytePrefix() {
            return bytePrefix;
        }
    }
}
