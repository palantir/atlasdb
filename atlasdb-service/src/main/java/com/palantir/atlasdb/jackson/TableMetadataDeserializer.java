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
package com.palantir.atlasdb.jackson;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Format;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.persist.Persistable;

public class TableMetadataDeserializer extends StdDeserializer<TableMetadata> {
    private static final long serialVersionUID = 1L;

    protected TableMetadataDeserializer() {
        super(TableMetadata.class);
    }

    @Override
    public TableMetadata deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
        JsonNode node = jp.readValueAsTree();
        NameMetadataDescription row = deserializeRowish(node);
        ColumnMetadataDescription col;
        if (node.get("is_dynamic").asBoolean()) {
            col = deserializeDynamicCol(node);
        } else {
            col = deserializeNamedCols(node);
        }
        // TODO(dxiao): make defaults not duplicated with TableMetadata code.
        ConflictHandler conflictHandler = Optional.ofNullable(node.get("conflictHandler"))
                .map(JsonNode::textValue).map(ConflictHandler::valueOf).orElse(ConflictHandler.RETRY_ON_WRITE_WRITE);
        CachePriority cachePriority = Optional.ofNullable(node.get("cachePriority"))
                .map(JsonNode::textValue).map(CachePriority::valueOf).orElse(CachePriority.WARM);
        boolean rangeScanAllowed = Optional.ofNullable(node.get("rangeScanAllowed"))
                .map(JsonNode::booleanValue).orElse(false);
        int compressionBlockSize = Optional.ofNullable(node.get("explicitCompressionBlockSizeKB"))
                .map(JsonNode::numberValue).map(Number::intValue).orElse(0);
        boolean negativeLookups = Optional.ofNullable(node.get("negativeLookups"))
                .map(JsonNode::booleanValue).orElse(false);
        SweepStrategy sweepStrategy = Optional.ofNullable(node.get("sweepStrategy"))
                .map(JsonNode::textValue).map(SweepStrategy::valueOf).orElse(SweepStrategy.CONSERVATIVE);
        boolean appendHeavyAndReadLight = Optional.ofNullable(node.get("appendHeavyAndReadLight"))
                .map(JsonNode::booleanValue).orElse(false);
        LogSafety logSafety = Optional.ofNullable(node.get("nameLogSafety"))
                .map(JsonNode::textValue).map(LogSafety::valueOf).orElse(LogSafety.UNSAFE);

        return new TableMetadata(
                row,
                col,
                conflictHandler,
                cachePriority,
                rangeScanAllowed,
                compressionBlockSize,
                negativeLookups,
                sweepStrategy,
                appendHeavyAndReadLight,
                logSafety);
    }

    private NameMetadataDescription deserializeRowish(JsonNode node) {
        List<NameComponentDescription> rowComponents = Lists.newArrayList();
        for (JsonNode rowNode : node.get("row")) {
            String name = rowNode.get("name").asText();
            ValueType type = ValueType.valueOf(rowNode.get("type").asText());
            ValueByteOrder order = ValueByteOrder.valueOf(rowNode.get("order").asText());
            rowComponents.add(new NameComponentDescription.Builder()
                    .componentName(name)
                    .type(type)
                    .byteOrder(order)
                    .build());
        }

        return NameMetadataDescription.create(rowComponents);
    }

    private ColumnMetadataDescription deserializeDynamicCol(JsonNode node) {
        NameMetadataDescription col = deserializeRowish(node.get("column"));
        ColumnValueDescription val = deserializeValue(node.get("value"));
        DynamicColumnDescription dynamicCol = new DynamicColumnDescription(col, val);
        return new ColumnMetadataDescription(dynamicCol);
    }

    private ColumnMetadataDescription deserializeNamedCols(JsonNode node) {
        Collection<NamedColumnDescription> cols = Lists.newArrayList();
        for (JsonNode colNode : node.get("columns")) {
            String name = colNode.get("name").asText();
            String longName = colNode.get("long_name").asText();
            ColumnValueDescription val = deserializeValue(colNode.get("value"));
            cols.add(new NamedColumnDescription(name, longName, val));
        }
        return new ColumnMetadataDescription(cols);
    }

    private ColumnValueDescription deserializeValue(JsonNode node) {
        Format format = Format.valueOf(node.get("format").asText());
        switch (format) {
            case PERSISTER:
                String className = node.get("type").asText();
                try {
                    @SuppressWarnings("unchecked")
                    Class<? extends Persister<?>> asSubclass = (Class<? extends Persister<?>>) Class.forName(
                            className).asSubclass(Persister.class);
                    return ColumnValueDescription.forPersister(asSubclass);
                } catch (Exception e) {
                    // Also wrong, but what else can you do?
                    return ColumnValueDescription.forType(ValueType.BLOB);
                }
            case PERSISTABLE:
                className = node.get("type").asText();
                try {
                    return ColumnValueDescription.forPersistable(
                            Class.forName(className).asSubclass(Persistable.class));
                } catch (Exception e) {
                    // Also wrong, but what else can you do?
                    return ColumnValueDescription.forType(ValueType.BLOB);
                }
            case PROTO:
                // Not even going to bother to try.
                return ColumnValueDescription.forType(ValueType.BLOB);
            case VALUE_TYPE:
                ValueType type = ValueType.valueOf(node.get("type").asText());
                return ColumnValueDescription.forType(type);
            default:
                throw new EnumConstantNotPresentException(Format.class, format.name());
        }
    }
}
