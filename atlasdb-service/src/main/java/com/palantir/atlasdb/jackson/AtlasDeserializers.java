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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
import com.palantir.atlasdb.proto.fork.ForkedJsonFormat;
import com.palantir.atlasdb.ptobject.EncodingUtils;
import com.palantir.atlasdb.table.description.ColumnMetadataDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Format;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NameComponentDescription;
import com.palantir.atlasdb.table.description.NameMetadataDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;
import com.palantir.common.base.Throwables;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class AtlasDeserializers {

    private AtlasDeserializers() {
        // cannot instantiate
    }

    public static byte[] deserializeRowPrefix(NameMetadataDescription description, JsonNode node) {
        return deserializeRowish(description, node, false);
    }

    public static byte[] deserializeRow(NameMetadataDescription description, JsonNode node) {
        return deserializeRowish(description, node, true);
    }

    private static byte[] deserializeRowish(NameMetadataDescription description, JsonNode node, boolean mustBeFull) {
        if (node == null) {
            return new byte[0];
        }
        int size = node.size();
        List<NameComponentDescription> components = description.getRowParts();
        Preconditions.checkArgument(
                size <= components.size(),
                "Received %s values for a row with only %s components.",
                size,
                components.size());
        Preconditions.checkArgument(
                !mustBeFull || size == components.size(),
                "Received %s values for a row with %s components.",
                size,
                components.size());
        byte[][] bytes = new byte[size][];
        Iterator<JsonNode> rowValues = getComponentNodes(node, components);
        for (int i = 0; i < size; i++) {
            NameComponentDescription component = components.get(i);
            bytes[i] = component.getType().convertFromJson(rowValues.next().toString());
            if (component.isReverseOrder()) {
                EncodingUtils.flipAllBitsInPlace(bytes[i]);
            }
        }
        return Bytes.concat(bytes);
    }

    private static Iterator<JsonNode> getComponentNodes(JsonNode node, List<NameComponentDescription> components) {
        if (node.isArray()) {
            return node.elements();
        } else {
            return Iterators.transform(components.iterator(), c -> node.get(c.getComponentName()));
        }
    }

    public static Iterable<byte[]> deserializeRows(final NameMetadataDescription description, JsonNode node) {
        return new JsonNodeIterable<>(node, subNode -> deserializeRow(description, subNode));
    }

    private static NamedColumnDescription getNamedCol(ColumnMetadataDescription colDescription, String longName) {
        for (NamedColumnDescription description : colDescription.getNamedColumns()) {
            if (longName.equals(description.getLongName())) {
                return description;
            }
        }
        throw new IllegalArgumentException("Unknown column with long name " + longName);
    }

    private static byte[] deserializeNamedCol(ColumnMetadataDescription colDescription, JsonNode node) {
        JsonNode nonArrayNode = node;
        if (nonArrayNode.isArray()) {
            nonArrayNode = node.get(0);
        }
        NamedColumnDescription namedCol = getNamedCol(colDescription, nonArrayNode.textValue());
        return PtBytes.toCachedBytes(namedCol.getShortName());
    }

    public static Iterable<byte[]> deserializeNamedCols(final ColumnMetadataDescription colDescription, JsonNode node) {
        if (colDescription.hasDynamicColumns()) {
            return ImmutableList.of();
        }
        return new JsonNodeIterable<>(node, subNode -> deserializeNamedCol(colDescription, subNode));
    }

    public static byte[] deserializeCol(ColumnMetadataDescription colDescription, JsonNode node) {
        if (colDescription.hasDynamicColumns()) {
            return deserializeDynamicCol(colDescription.getDynamicColumn(), node);
        } else {
            return deserializeNamedCol(colDescription, node);
        }
    }

    private static byte[] deserializeDynamicCol(DynamicColumnDescription colDescription, JsonNode node) {
        return deserializeRowish(colDescription.getColumnNameDesc(), node, true);
    }

    private static Cell deserializeCell(TableMetadata metadata, JsonNode node) {
        byte[] row = deserializeRow(metadata.getRowMetadata(), node.get("row"));
        byte[] col = deserializeCol(metadata.getColumns(), node.get("col"));
        return Cell.create(row, col);
    }

    public static Iterable<Cell> deserializeCells(final TableMetadata metadata, JsonNode node) {
        return new JsonNodeIterable<>(node, subNode -> deserializeCell(metadata, subNode));
    }

    private static Iterable<Map.Entry<Cell, byte[]>> deserializeCellVal(TableMetadata metadata, JsonNode node) {
        byte[] row = deserializeRow(metadata.getRowMetadata(), node.get("row"));
        ColumnMetadataDescription colDescription = metadata.getColumns();
        if (colDescription.hasDynamicColumns()) {
            byte[] col = deserializeDynamicCol(colDescription.getDynamicColumn(), node.get("col"));
            byte[] val = deserializeVal(colDescription.getDynamicColumn().getValue(), node.get("val"));
            return ImmutableList.of(Maps.immutableEntry(Cell.create(row, col), val));
        } else {
            Collection<Map.Entry<Cell, byte[]>> results = new ArrayList<>(1);
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String longName = entry.getKey();
                if (longName.equals("row")) {
                    continue;
                }
                NamedColumnDescription description = getNamedCol(colDescription, longName);
                byte[] col = PtBytes.toCachedBytes(description.getShortName());
                JsonNode valNode = entry.getValue();
                byte[] val = deserializeVal(description.getValue(), valNode);
                results.add(Maps.immutableEntry(Cell.create(row, col), val));
            }
            return results;
        }
    }

    public static Map<Cell, byte[]> deserializeCellVals(final TableMetadata metadata, JsonNode node) {
        Iterable<Iterable<Map.Entry<Cell, byte[]>>> cellVals =
                new JsonNodeIterable<>(node, subNode -> deserializeCellVal(metadata, subNode));
        ImmutableMap.Builder<Cell, byte[]> builder = ImmutableMap.builder();
        for (Iterable<Map.Entry<Cell, byte[]>> entries : cellVals) {
            for (Map.Entry<Cell, byte[]> entry : entries) {
                builder.put(entry);
            }
        }
        return builder.build();
    }

    public static byte[] deserializeVal(ColumnValueDescription description, JsonNode node) {
        byte[] bytes;
        switch (description.getFormat()) {
            case PERSISTABLE:
            case PERSISTER:
                bytes = node.asToken().asByteArray();
                break;
            case PROTO:
                Message.Builder builder = DynamicMessage.newBuilder(description.getProtoDescriptor());
                try {
                    ForkedJsonFormat.merge(node.toString(), builder);
                } catch (ForkedJsonFormat.ParseException e) {
                    throw Throwables.rewrapAndThrowUncheckedException(e);
                }
                bytes = builder.build().toByteArray();
                break;
            case VALUE_TYPE:
                bytes = description.getValueType().convertFromJson(node.toString());
                break;
            default:
                throw new EnumConstantNotPresentException(
                        Format.class, description.getFormat().name());
        }
        return CompressionUtils.compress(bytes, description.getCompression());
    }

    private static class JsonNodeIterable<T> implements Iterable<T> {
        private final JsonNode node;
        private final Function<JsonNode, T> generator;

        JsonNodeIterable(JsonNode node, Function<JsonNode, T> generator) {
            this.node = node;
            this.generator = generator;
        }

        @Override
        public Iterator<T> iterator() {
            if (node == null) {
                return ImmutableSet.<T>of().iterator();
            }
            return new AbstractIterator<T>() {
                private int index = 0;

                @Override
                protected T computeNext() {
                    JsonNode subNode = node.get(index++);
                    if (subNode == null) {
                        return endOfData();
                    }
                    return generator.apply(subNode);
                }
            };
        }

        @Override
        public String toString() {
            return Iterables.toString(this);
        }
    }
}
