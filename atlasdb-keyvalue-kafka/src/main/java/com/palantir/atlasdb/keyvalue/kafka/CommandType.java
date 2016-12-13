/**
 * Copyright 2016 Palantir Technologies
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
package com.palantir.atlasdb.keyvalue.kafka;

import java.util.function.BiConsumer;

import com.palantir.atlasdb.keyvalue.api.KeyValueService;
import com.palantir.atlasdb.keyvalue.kafka.data.CreateTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.DeleteData;
import com.palantir.atlasdb.keyvalue.kafka.data.DropTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.MultiPutData;
import com.palantir.atlasdb.keyvalue.kafka.data.PutMetadataForTablesData;
import com.palantir.atlasdb.keyvalue.kafka.data.PutUnlessExistsData;
import com.palantir.atlasdb.keyvalue.kafka.data.PutWithTimestampsData;
import com.palantir.atlasdb.keyvalue.kafka.data.TruncateTablesData;

public enum CommandType {
    CREATE_TABLES((kvs, data) -> {
        CreateTablesData decodedData = CommandTypeDeserialisers.createTables(data);
        kvs.createTables(decodedData.getTableRefToTableMetadata());
    }),

    PUT_METADATA_FOR_TABLES((kvs, data) -> {
        PutMetadataForTablesData decodedData = CommandTypeDeserialisers.putMetadataForTables(data);
        kvs.putMetadataForTables(decodedData.getTableRefToTableMetadata());
    }),

    TRUNCATE_TABLES((kvs, data) -> {
        TruncateTablesData decodedData = CommandTypeDeserialisers.truncateTables(data);
        kvs.truncateTables(decodedData.getTableRefs());
    }),

    DROP_TABLES((kvs, data) -> {
        DropTablesData decodedData = CommandTypeDeserialisers.dropTables(data);
        kvs.dropTables(decodedData.getTableRefs());
    }),

    MULTI_PUT((kvs, data) -> {
        MultiPutData decodedData = CommandTypeDeserialisers.multiPut(data);
        kvs.multiPut(decodedData.getValuesByTable(), decodedData.getTimestamp());
    }),

    PUT_UNLESS_EXISTS((kvs, data) -> {
        PutUnlessExistsData decodedData = CommandTypeDeserialisers.putUnlessExists(data);
        kvs.putUnlessExists(decodedData.getTableReference(), decodedData.getValues());
    }),

    PUT_WITH_TIMESTAMPS((kvs, data) -> {
        PutWithTimestampsData decodedData = CommandTypeDeserialisers.putWithTimestamps(data);
        kvs.putWithTimestamps(
                decodedData.getTableReference(),
                decodedData.getCellValues());
    }),

    DELETE((kvs, data) -> {
        DeleteData decodedData = CommandTypeDeserialisers.delete(data);
        kvs.delete(
                decodedData.getTableReference(),
                decodedData.getKeys());
    }),

    READ_OPERATION((kvs, data) -> { });

    private final BiConsumer<KeyValueService, byte[]> operation;

    CommandType(BiConsumer<KeyValueService, byte[]> operation) {
        this.operation = operation;
    }

    void runOperation(KeyValueService kvs, byte[] data) {
        operation.accept(kvs, data);
    }
}
