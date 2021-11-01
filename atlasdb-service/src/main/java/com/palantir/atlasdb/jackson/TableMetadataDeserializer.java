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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Format;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.ImmutableTableMetadata;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.atlasdb.table.description.ValueType;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.persist.Persistable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class TableMetadataDeserializer extends StdDeserializer<TableMetadata> {
    private static final long serialVersionUID = 1L;

    protected TableMetadataDeserializer() {
        super(TableMetadata.class);
    }

    @Override
    public TableMetadata deserialize(JsonParser jp, DeserializationContext _ctxt) throws IOException {
        JsonNode node = jp.readValueAsTree();

        ImmutableTableMetadata.Builder builder = TableMetadata.builder()
                .rowMetadata(deserializeRowish(node))
                .columns(node.get("is_dynamic").asBoolean() ? deserializeDynamicCol(node) : deserializeNamedCols(node));

        Optional.ofNullable(node.get("conflictHandler"))
                .map(JsonNode::textValue)
                .map(ConflictHandler::valueOf)
                .ifPresent(builder::conflictHandler);
        Optional.ofNullable(node.get("cachePriority"))
                .map(JsonNode::textValue)
                .map(CachePriority::valueOf)
                .ifPresent(builder::cachePriority);
        Optional.ofNullable(node.get("rangeScanAllowed"))
                .map(JsonNode::booleanValue)
                .ifPresent(builder::rangeScanAllowed);
        Optional.ofNullable(node.get("explicitCompressionBlockSizeKB"))
                .map(JsonNode::numberValue)
                .map(Number::intValue)
                .ifPresent(builder::explicitCompressionBlockSizeKB);
        Optional.ofNullable(node.get("negativeLookups"))
                .map(JsonNode::booleanValue)
                .ifPresent(builder::negativeLookups);
        Optional.ofNullable(node.get("sweepStrategy"))
                .map(JsonNode::textValue)
                .map(SweepStrategy::valueOf)
                .ifPresent(builder::sweepStrategy);
        Optional.ofNullable(node.get("appendHeavyAndReadLight"))
                .map(JsonNode::booleanValue)
                .ifPresent(builder::appendHeavyAndReadLight);
        Optional.ofNullable(node.get("nameLogSafety"))
                .map(JsonNode::textValue)
                .map(LogSafety::valueOf)
                .ifPresent(builder::nameLogSafety);

        return builder.build();
    }

    private NameMetadataDescription deserializeRowish(JsonNode node) {
        List<NameComponentDescription> rowComponents = new ArrayList<>();
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
        Collection<NamedColumnDescription> cols = new ArrayList<>();
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
                    Class<? extends Persister<?>> asSubclass = (Class<? extends Persister<?>>)
                            Class.forName(className).asSubclass(Persister.class);
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
