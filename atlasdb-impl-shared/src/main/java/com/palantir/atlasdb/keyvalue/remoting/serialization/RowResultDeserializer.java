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
package com.palantir.atlasdb.keyvalue.remoting.serialization;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

@SuppressWarnings("rawtypes")
public final class RowResultDeserializer extends JsonDeserializer<RowResult> {
    private static final RowResultDeserializer instance = new RowResultDeserializer();

    private RowResultDeserializer() {
        // singleton
    }

    public static RowResultDeserializer instance() {
        return instance;
    }

    @Override
    public RowResult deserialize(JsonParser parser, DeserializationContext context) throws IOException {
        JsonNode node = parser.getCodec().readTree(parser);

        int type = node.get("type").asInt();
        byte[] row = node.get("row").binaryValue();

        switch (type) {
            case RowResultSerializer.VALUE_TYPE_ID:
                return RowResult.create(row, deserializeWithValue(node));
            case RowResultSerializer.TIMESTAMPS_SET_TYPE_ID:
                return RowResult.create(row, deserializeWithTimestamps(node));
            case RowResultSerializer.VALUES_SET_TYPE_ID:
                return RowResult.create(row, deserializeWithValuesSet(node));
            default:
                throw new IllegalArgumentException("Invalid RowResult type!");
        }
    }

    private SortedMap<byte[], Value> deserializeWithValue(JsonNode node) throws IOException {
        SortedMap<byte[], Value> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        Iterator<JsonNode> it = node.get("columns").elements();
        while (it.hasNext()) {
            JsonNode col = it.next();
            byte[] colName = col.get("column").binaryValue();
            byte[] contents = col.get("contents").binaryValue();
            long timestamp = col.get("timestamp").asLong();
            result.put(colName, Value.create(contents, timestamp));
        }
        return result;
    }

    private SortedMap<byte[], Set<Long>> deserializeWithTimestamps(JsonNode node) throws IOException {
        SortedMap<byte[], Set<Long>> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        Iterator<JsonNode> it = node.get("columns").elements();
        while (it.hasNext()) {
            JsonNode col = it.next();
            byte[] colName = col.get("column").binaryValue();
            Set<Long> timestamps = Sets.newHashSet();
            Iterator<JsonNode> colIt = col.get("timestamps").elements();
            while (colIt.hasNext()) {
                JsonNode colVal = colIt.next();
                long timestamp = colVal.asLong();
                timestamps.add(timestamp);
            }
            result.put(colName, timestamps);
        }
        return result;
    }

    private SortedMap<byte[], Set<Value>> deserializeWithValuesSet(JsonNode node) throws IOException {
        SortedMap<byte[], Set<Value>> result = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
        Iterator<JsonNode> it = node.get("columns").elements();
        while (it.hasNext()) {
            JsonNode col = it.next();
            byte[] colName = col.get("column").binaryValue();
            Set<Value> values = Sets.newHashSet();
            Iterator<JsonNode> colIt = col.get("timestamps").elements();
            while (colIt.hasNext()) {
                JsonNode colVal = colIt.next();
                long timestamp = colVal.get("timestamp").asLong();
                byte[] contents = colVal.get("contents").binaryValue();
                values.add(Value.create(contents, timestamp));
            }
            result.put(colName, values);
        }
        return result;
    }
}
