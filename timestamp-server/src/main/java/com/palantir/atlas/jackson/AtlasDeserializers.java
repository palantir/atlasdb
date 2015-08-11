package com.palantir.atlas.jackson;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.googlecode.protobuf.format.JsonFormat;
import com.palantir.atlasdb.compress.CompressionUtils;
import com.palantir.atlasdb.encoding.PtBytes;
import com.palantir.atlasdb.keyvalue.api.Cell;
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

public class AtlasDeserializers {

    private AtlasDeserializers() {
        // cannot instantiate
    }

    public static byte[] deserializeRowPrefix(NameMetadataDescription description,
                                              JsonNode node) {
        return deserializeRowish(description, node, false);
    }

    public static byte[] deserializeRow(NameMetadataDescription description,
                                        JsonNode node) {
        return deserializeRowish(description, node, true);
    }

    private static byte[] deserializeRowish(NameMetadataDescription description,
                                            JsonNode node,
                                            boolean mustBeFull) {
        if (node == null) {
            return new byte[0];
        }
        int size = node.size();
        List<NameComponentDescription> components = description.getRowParts();
        Preconditions.checkArgument(size <= components.size(),
                "Received %s values for a row with only %s components.", size, components.size());
        Preconditions.checkArgument(!mustBeFull || size == components.size(),
                "Received %s values for a row with %s components.", size, components.size());
        byte[][] bytes = new byte[size][];
        for (int i = 0; i < size; i++) {
            JsonNode value = node.get(i);
            NameComponentDescription component = components.get(i);
            bytes[i] = component.getType().convertFromJson(value.toString());
            if (component.isReverseOrder()) {
                EncodingUtils.flipAllBitsInPlace(bytes[i]);
            }
        }
        return Bytes.concat(bytes);
    }

    public static Iterable<byte[]> deserializeRows(final NameMetadataDescription description,
                                                   JsonNode node) {
        return new JsonNodeIterable<byte[]>(node, new Function<JsonNode, byte[]>() {
            @Override
            public byte[] apply(JsonNode subNode) {
                return deserializeRow(description, subNode);
            }
        });
    }

    private static NamedColumnDescription getNamedCol(ColumnMetadataDescription colDescription,
                                                      String longName) {
        for (NamedColumnDescription description : colDescription.getNamedColumns()) {
            if (longName.equals(description.getLongName())) {
                return description;
            }
        }
        throw new IllegalArgumentException("Unknown column " + longName);
    }

    private static byte[] deserializeNamedCol(ColumnMetadataDescription colDescription,
                                              JsonNode node) {
        if (node.isArray()) {
            node = node.get(0);
        }
        NamedColumnDescription namedCol = getNamedCol(colDescription, node.textValue());
        return PtBytes.toCachedBytes(namedCol.getShortName());
    }

    public static Iterable<byte[]> deserializeNamedCols(final ColumnMetadataDescription colDescription,
                                                        JsonNode node) {
        if (colDescription.hasDynamicColumns()) {
            return ImmutableList.of();
        }
        return new JsonNodeIterable<byte[]>(node, new Function<JsonNode, byte[]>() {
            @Override
            public byte[] apply(JsonNode subNode) {
                return deserializeNamedCol(colDescription, subNode);
            }
        });
    }

    private static byte[] deserializeCol(ColumnMetadataDescription colDescription,
                                         JsonNode node) {
        if (colDescription.hasDynamicColumns()) {
            return deserializeDynamicCol(colDescription.getDynamicColumn(), node);
        } else {
            return deserializeNamedCol(colDescription, node);
        }
    }

    private static byte[] deserializeDynamicCol(DynamicColumnDescription colDescription,
                                                JsonNode node) {
        return deserializeRowish(colDescription.getColumnNameDesc(), node, true);
    }

    private static Cell deserializeCell(TableMetadata metadata,
                                        JsonNode node) {
        byte[] row = deserializeRow(metadata.getRowMetadata(), node.get("row"));
        byte[] col = deserializeCol(metadata.getColumns(), node.get("col"));
        return Cell.create(row, col);
    }

    public static Iterable<Cell> deserializeCells(final TableMetadata metadata,
                                                  JsonNode node) {
        return new JsonNodeIterable<Cell>(node, new Function<JsonNode, Cell>() {
            @Override
            public Cell apply(JsonNode subNode) {
                return deserializeCell(metadata, subNode);
            }
        });
    }

    private static Iterable<Entry<Cell, byte[]>> deserializeCellVal(TableMetadata metadata,
                                                                    JsonNode node) {
        byte[] row = deserializeRow(metadata.getRowMetadata(), node.get("row"));
        ColumnMetadataDescription colDescription = metadata.getColumns();
        if (colDescription.hasDynamicColumns()) {
            byte[] col = deserializeDynamicCol(colDescription.getDynamicColumn(), node.get("col"));
            byte[] val = deserializeVal(colDescription.getDynamicColumn().getValue(), node.get("val"));
            return ImmutableList.of(Maps.immutableEntry(Cell.create(row, col), val));
        } else {
            Collection<Entry<Cell, byte[]>> results = Lists.newArrayListWithCapacity(1);
            Iterator<Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Entry<String, JsonNode> entry = fields.next();
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

    public static Map<Cell, byte[]> deserializeCellVals(final TableMetadata metadata,
                                                        JsonNode node) {
        Iterable<Iterable<Entry<Cell, byte[]>>> cellVals =
                new JsonNodeIterable<Iterable<Entry<Cell, byte[]>>>(node,
                        new Function<JsonNode, Iterable<Entry<Cell, byte[]>>>() {
            @Override
            public Iterable<Entry<Cell, byte[]>> apply(JsonNode subNode) {
                return deserializeCellVal(metadata, subNode);
            }
        });
        ImmutableMap.Builder<Cell, byte[]> builder = ImmutableMap.builder();
        for (Iterable<Entry<Cell, byte[]>> entries : cellVals) {
            for (Entry<Cell, byte[]> entry : entries) {
                builder.put(entry);
            }
        }
        return builder.build();
    }

    private static byte[] deserializeVal(ColumnValueDescription description,
                                         JsonNode node) {
        byte[] bytes;
        switch (description.getFormat()) {
        case BLOCK_STORED_PROTO:
        case PERSISTABLE:
            bytes = node.asToken().asByteArray();
            break;
        case PROTO:
            Message.Builder builder = DynamicMessage.newBuilder(description.getProtoDescriptor());
            try {
//                ForkedJsonFormat.merge(node.toString(), builder);
                JsonFormat.merge(node.toString(), builder);
            } catch (JsonFormat.ParseException e) {
                throw Throwables.rewrapAndThrowUncheckedException(e);
            }
            bytes = builder.build().toByteArray();
            break;
        case VALUE_TYPE:
            bytes = description.getValueType().convertFromJson(node.toString());
            break;
        default:
            throw new EnumConstantNotPresentException(Format.class, description.getFormat().name());
        }
        return CompressionUtils.compress(bytes, description.getCompression());
    }

    private static class JsonNodeIterable<T> implements Iterable<T> {
        private final JsonNode node;
        private final Function<JsonNode, T> generator;

        public JsonNodeIterable(JsonNode node, Function<JsonNode, T> generator) {
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
