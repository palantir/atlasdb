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

import com.palantir.atlasdb.keyvalue.impl.InMemoryKeyValueService;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.sweep.queue.id.SweepTableIndices;
import java.nio.charset.StandardCharsets;
import org.junit.Test;

public final class WriteReferencePersisterTest {
    private static final TableReference TABLE = TableReference.create(Namespace.create("test_ctx"), "test__table_name");
    private static final byte[] row = {63, -73, 110};
    private static final byte[] column = {118};
    private static final Cell CELL = Cell.create(row, column);
    private static final boolean IS_TOMBSTONE = true;
    private static final WriteReference WRITE_REFERENCE = WriteReference.of(TABLE, CELL, IS_TOMBSTONE);

    private final KeyValueService kvs = new InMemoryKeyValueService(true);
    private final SweepTableIndices tableIndices = new SweepTableIndices(kvs);
    private final WriteReferencePersister persister = new WriteReferencePersister(tableIndices);

    @Test
    public void testCanUnpersistJsonValues() {
        String original = "{\"t\":{\"namespace\":{\"name\":\"test_ctx\"},\"tablename\":\"test__table_name\"},\"c\":{\""
                + "rowName\":\"P7du\",\"columnName\":\"dg==\"},\"d\":true}";
        StoredWriteReference stored =
                StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(original.getBytes(StandardCharsets.UTF_8));
        assertThat(persister.unpersist(stored)).isEqualTo(WRITE_REFERENCE);
    }

    @Test
    public void testCanUnpersistBinary_tableNameAsString() {
        byte[] data = EncodingUtils.add(
                new byte[1],
                EncodingUtils.encodeVarString(TABLE.getQualifiedName()),
                EncodingUtils.encodeSizedBytes(row),
                EncodingUtils.encodeSizedBytes(column),
                EncodingUtils.encodeVarLong(1));
        StoredWriteReference stored = StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(data);
        assertThat(persister.unpersist(stored)).isEqualTo(WRITE_REFERENCE);
    }

    @Test
    public void testCanUnpersistBinary_id() {
        assertThat(persister.unpersist(StoredWriteReference.BYTES_HYDRATOR.hydrateFromBytes(
                        persister.persist(WRITE_REFERENCE).persistToBytes())))
                .isEqualTo(WRITE_REFERENCE);
    }
}
