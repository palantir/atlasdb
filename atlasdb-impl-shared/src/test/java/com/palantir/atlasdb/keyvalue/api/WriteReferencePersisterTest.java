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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.palantir.atlasdb.keyvalue.api.WriteReferencePersister.UnknownIdentifierHandlingMethod;
import com.palantir.atlasdb.keyvalue.api.WriteReferencePersister.WriteMethod;
import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public final class WriteReferencePersisterTest {
    private static final TableReference TABLE = TableReference.create(Namespace.create("test_ctx"), "test__table_name");
    private static final byte[] row = {63, -73, 110};
    private static final byte[] column = {118};
    private static final Cell CELL = Cell.create(row, column);
    private static final boolean IS_TOMBSTONE = true;
    private static final WriteReference WRITE_REFERENCE = WriteReference.of(TABLE, CELL, IS_TOMBSTONE);

    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final SweepTableIndices tableIndices = new SweepTableIndices(kvs);

    @ParameterizedTest
    @MethodSource("writeMethods")
    void testCanUnpersistJsonValues(WriteMethod writeMethod) {
        WriteReferencePersister persister = new WriteReferencePersister(tableIndices, writeMethod, UnknownIdentifierHandlingMethod.THROW);
        String original = "{\"t\":{\"namespace\":{\"name\":\"test_ctx\"},\"tablename\":\"test__table_name\"},\"c\":{\""
                + "rowName\":\"P7du\",\"columnName\":\"dg==\"},\"d\":true}";
        StoredWriteReference stored =
                StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(original.getBytes(StandardCharsets.UTF_8));
        assertThat(persister.unpersist(stored)).hasValue(WRITE_REFERENCE);
    }

    @ParameterizedTest
    @MethodSource("writeMethods")
    void testCanUnpersistBinary_tableNameAsString(WriteMethod writeMethod) {
        WriteReferencePersister persister = new WriteReferencePersister(tableIndices, writeMethod, UnknownIdentifierHandlingMethod.THROW);
        byte[] data = EncodingUtils.add(
                new byte[1],
                EncodingUtils.encodeVarString(TABLE.getQualifiedName()),
                EncodingUtils.encodeSizedBytes(row),
                EncodingUtils.encodeSizedBytes(column),
                EncodingUtils.encodeVarLong(1));
        StoredWriteReference stored = StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(data);
        assertThat(persister.unpersist(stored)).hasValue(WRITE_REFERENCE);
    }

    @Test
    void testCanUnpersistBinary_id() {
        WriteReferencePersister persister = new WriteReferencePersister(tableIndices, WriteMethod.TABLE_ID_BINARY, UnknownIdentifierHandlingMethod.THROW);
        StoredWriteReference storedWriteReference = StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(
                persister.persist(Optional.of(WRITE_REFERENCE)).persistToBytes());
        assertThat(persister.unpersist(storedWriteReference)).hasValue(WRITE_REFERENCE);

        WriteReferencePersister stringPersister =
                new WriteReferencePersister(tableIndices, WriteMethod.TABLE_NAME_AS_STRING_BINARY, UnknownIdentifierHandlingMethod.THROW);
        assertThat(stringPersister.unpersist(storedWriteReference))
                .as("the string persister, given a known ID, should be able to interpret it")
                .hasValue(WRITE_REFERENCE);
    }

    @ParameterizedTest
    @MethodSource("writeMethods")
    void canUnpersistEmpty(WriteMethod writeMethod) {
        WriteReferencePersister persister = new WriteReferencePersister(tableIndices, writeMethod, UnknownIdentifierHandlingMethod.THROW);
        assertThat(persister.unpersist(persister.persist(Optional.empty()))).isEmpty();
    }

    @Test
    void canPersistBinary_tableNameAsString() {
        WriteReferencePersister persister =
                new WriteReferencePersister(tableIndices, WriteMethod.TABLE_NAME_AS_STRING_BINARY, UnknownIdentifierHandlingMethod.THROW);
        byte[] data = EncodingUtils.add(
                new byte[1],
                EncodingUtils.encodeVarString(TABLE.getQualifiedName()),
                EncodingUtils.encodeSizedBytes(row),
                EncodingUtils.encodeSizedBytes(column),
                EncodingUtils.encodeVarLong(1));
        assertThat(persister.persist(Optional.of(WRITE_REFERENCE)).persistToBytes())
                .isEqualTo(data);
    }

    @ParameterizedTest
    @MethodSource("writeMethods")
    void ignoresUnknownIdentifiersIfConfigured(WriteMethod writeMethod) {
        WriteReferencePersister persister =
                new WriteReferencePersister(tableIndices, writeMethod, UnknownIdentifierHandlingMethod.IGNORE);

        byte[] data = createExpectedDataWithIdentifier(777666555);
        assertThat(persister.unpersist(StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(data))).isEmpty();
    }

    @ParameterizedTest
    @MethodSource("writeMethods")
    void throwsOnUnknownIdentifiersIfConfigured(WriteMethod writeMethod) {
        WriteReferencePersister persister =
                new WriteReferencePersister(tableIndices, writeMethod, UnknownIdentifierHandlingMethod.THROW);

        byte[] data = createExpectedDataWithIdentifier(314159265);
        assertThatThrownBy(() -> persister.unpersist(StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(data)))
                .isInstanceOf(NoSuchElementException.class);
    }

    static Stream<WriteMethod> writeMethods() {
        return Stream.of(WriteMethod.values());
    }

    private static byte[] createExpectedDataWithIdentifier(long identifier) {
        return EncodingUtils.add(
                new byte[]{1},
                EncodingUtils.encodeVarLong(identifier),
                EncodingUtils.encodeSizedBytes(row),
                EncodingUtils.encodeSizedBytes(column),
                EncodingUtils.encodeVarLong(1));
    }
}
