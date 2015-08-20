package com.palantir.atlasdb.keyvalue.remoting;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.UnsignedBytes;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

@SuppressWarnings("rawtypes")
final class RowResultDeserializer extends JsonDeserializer<RowResult> {

    @Override
    public RowResult deserialize(JsonParser p, DeserializationContext ctxt) throws IOException,
            JsonProcessingException {
        JsonNode node = p.getCodec().readTree(p);
        int type = node.get("type").asInt();
        byte[] row = PtBytes.decodeBase64(node.get("row").asText());
        switch (type) {
        case RowResultSerializer.VALUE_TYPE_ID:
            return RowResult.create(row, deserializeWithValue(node, ctxt));
        case RowResultSerializer.TIMESTAMPS_SET_TYPE_ID:
            return RowResult.create(row, deserializeWithTimestamps(node, ctxt));
        case RowResultSerializer.VALUES_SET_TYPE_ID:
            return RowResult.create(row, deserializeWithValuesSet(node, ctxt));
        }
        throw new UnsupportedOperationException("Invalid RowResult type!");
    }

    private SortedMap<byte[], Value> deserializeWithValue(JsonNode node, DeserializationContext ctxt) throws IOException {
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

    private SortedMap<byte[], Set<Long>> deserializeWithTimestamps(JsonNode node,
                                                                   DeserializationContext ctxt) throws IOException {
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

    private SortedMap<byte[], Set<Value>> deserializeWithValuesSet(JsonNode node,
                                                                   DeserializationContext ctxt)
            throws IOException {
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

    private static final RowResultDeserializer instance = new RowResultDeserializer();
    static RowResultDeserializer instance() {
        return instance;
    }
}