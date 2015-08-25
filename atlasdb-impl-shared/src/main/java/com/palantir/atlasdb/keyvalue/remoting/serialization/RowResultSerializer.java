package com.palantir.atlasdb.keyvalue.remoting;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.keyvalue.api.Value;

/**
 * WARNING: This is gross.
 * @author htarasiuk
 *
 */
@SuppressWarnings("rawtypes")
final class RowResultSerializer extends JsonSerializer<RowResult> {

    static final int VALUE_TYPE_ID = 0;
    static final int TIMESTAMPS_SET_TYPE_ID = 1;
    static final int VALUES_SET_TYPE_ID = 2;

    @SuppressWarnings("unchecked") @Override
    public void serialize(RowResult value, JsonGenerator gen, SerializerProvider serializers)
            throws IOException, JsonProcessingException {
        if (!value.getColumns().entrySet().isEmpty()) {
            Object firstObject = ((Entry) value.getColumns().entrySet().iterator().next()).getValue();
            if (firstObject instanceof Value) {
                serializeWithValue(value, gen, serializers);
                return;
            } else if (firstObject instanceof Set<?>) {
                Set<?> set = (Set<?>) firstObject;
                if (!set.isEmpty()) {
                    Object firstSetObject = set.iterator().next();
                    if (firstSetObject instanceof Long) {
                        serializeWithTimestampsSet(value, gen, serializers);
                        return;
                    } else if (firstSetObject instanceof Value) {
                        serializeWithValuesSet(value, gen, serializers);
                        return;
                    }
                }
            }
            throw new UnsupportedOperationException("Invalid RowResult type!");
        }
        // Does not matter since no templated objects are in there.
        serializeWithValue(value, gen, serializers);
    }

    private void serializeWithValue(RowResult<Value> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeNumberField("type", VALUE_TYPE_ID);
        gen.writeBinaryField("row", value.getRowName());
        gen.writeFieldName("columns");
        gen.writeStartArray();
        for (Entry<byte[], Value> entry : value.getColumns().entrySet()) {
            gen.writeStartObject();
            gen.writeBinaryField("column", entry.getKey());
            gen.writeBinaryField("contents", entry.getValue().getContents());
            gen.writeNumberField("timestamp", entry.getValue().getTimestamp());
            gen.writeEndObject();
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }

    private void serializeWithTimestampsSet(RowResult<Set<Long>> value,
                                            JsonGenerator gen,
                                            SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeNumberField("type", TIMESTAMPS_SET_TYPE_ID);
        gen.writeBinaryField("row", value.getRowName());
        gen.writeFieldName("columns");
        gen.writeStartArray();
        for (Entry<byte[], Set<Long>> entry : value.getColumns().entrySet()) {
            gen.writeStartObject();
            gen.writeBinaryField("column", entry.getKey());
            gen.writeFieldName("timestamps");
            gen.writeStartArray();
            for (Long timestamp : entry.getValue()) {
                gen.writeNumber(timestamp);
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }

    private void serializeWithValuesSet(RowResult<Set<Value>> value,
                                        JsonGenerator gen,
                                        SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        gen.writeNumberField("type", VALUES_SET_TYPE_ID);
        gen.writeBinaryField("row", value.getRowName());
        gen.writeFieldName("columns");
        gen.writeStartArray();
        for (Entry<byte[], Set<Value>> entry : value.getColumns().entrySet()) {
            gen.writeStartObject();
            gen.writeBinaryField("column", entry.getKey());
            gen.writeFieldName("timestamps");
            gen.writeStartArray();
            for (Value val : entry.getValue()) {
                gen.writeStartObject();
                gen.writeBinaryField("contents", val.getContents());
                gen.writeNumberField("timestamp", val.getTimestamp());
                gen.writeEndObject();
            }
            gen.writeEndArray();
            gen.writeEndObject();
        }
        gen.writeEndArray();
        gen.writeEndObject();
    }

    private static RowResultSerializer instance = new RowResultSerializer();
    static RowResultSerializer instance() {
        return instance;
    }

}
