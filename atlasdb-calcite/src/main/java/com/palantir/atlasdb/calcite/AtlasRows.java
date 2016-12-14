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
package com.palantir.atlasdb.calcite;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.palantir.atlasdb.keyvalue.api.RowResult;
import com.palantir.atlasdb.table.description.ValueType;

public class AtlasRows {
    private AtlasRows() {
        // uninstantiable
    }

    public static List<AtlasRow> deserialize(AtlasTableMetadata metadata, RowResult<byte[]> row) {
        List<AtlasColumn> rowComps = parseComponents(metadata.rowComponents(), row.getRowName());
        if (metadata.hasDynamicColumns()) {
            List<List<AtlasColumn>> dynCols = parseColumnComponents(
                    metadata.dynamicColumnComponents(),
                    metadata.valueColumn(),
                    row.getColumns());
            return dynCols.stream()
                    .map(colComps -> concat(rowComps, colComps))
                    .map(ImmutableAtlasRow::of)
                    .collect(Collectors.toList());
        } else {
            List<AtlasColumn> namedCols = parseNamedColumns(metadata.namedColumns(), row.getColumns());
            return ImmutableList.of(ImmutableAtlasRow.of(concat(rowComps, namedCols)));
        }
    }

    private static List<AtlasColumn> parseComponents(List<AtlasColumnMetdata> colsMeta, byte[] row) {
        ImmutableList.Builder<AtlasColumn> ret = ImmutableList.builder();
        int index = 0;
        for (int i = 0; i < colsMeta.size(); i++) {
            AtlasColumnMetdata meta = colsMeta.get(i);
            Preconditions.checkState(meta.isComponent(), "metadata must be for components");
            ValueType type = meta.valueType();
            Object val = type.convertToJava(row, index);
            int len = type.sizeOf(val);
            if (len == 0) {
                Preconditions.checkArgument(type == ValueType.STRING || type == ValueType.BLOB,
                        "only BLOB and STRING can have unknown length");
                Preconditions.checkArgument(i == colsMeta.size() - 1, "only terminal types can have unknown length");
                len = row.length - index;
            }
            index += len;
            ret.add(ImmutableAtlasColumn.of(meta, val));
        }
        return ret.build();
    }

    private static List<AtlasColumn> parseNamedColumns(List<AtlasColumnMetdata> colsMeta,
                                                       SortedMap<byte[], byte[]> columns) {
        ImmutableList.Builder<AtlasColumn> ret = ImmutableList.builder();
        Map<ByteBuffer, byte[]> wrappedCols = Maps.newHashMap();
        for (Map.Entry<byte[], byte[]> entry : columns.entrySet()) {
            wrappedCols.put(ByteBuffer.wrap(entry.getKey()), entry.getValue());
        }
        for (AtlasColumnMetdata meta : colsMeta) {
            Preconditions.checkState(meta.isNamedColumn(), "metadata must be for named columns");
            ByteBuffer shortName = ByteBuffer.wrap(meta.getName().getBytes());
            if (wrappedCols.containsKey(shortName)) {
                ret.add(ImmutableAtlasColumn.of(meta, meta.valueType().convertToJava(wrappedCols.get(shortName), 0)));
            } else {
                ret.add(ImmutableAtlasColumn.of(meta, null));
            }
        }
        return ret.build();
    }

    private static List<List<AtlasColumn>> parseColumnComponents(List<AtlasColumnMetdata> colsMeta,
                                                                 AtlasColumnMetdata valMeta,
                                                                 SortedMap<byte[], byte[]> columns) {
        return columns.entrySet().stream()
                .map(e -> ImmutableList.<AtlasColumn>builder()
                        .addAll(parseComponents(colsMeta, e.getKey()))
                        .add(ImmutableAtlasColumn.of(
                                valMeta,
                                valMeta.valueType().convertToJava(e.getValue(), 0)))
                        .build()
                )
                .collect(Collectors.toList());
    }

    private static <T> List<T> concat(List<T> list1, List<T> list2) {
        return ImmutableList.<T>builder().addAll(list1).addAll(list2).build();
    }
}
