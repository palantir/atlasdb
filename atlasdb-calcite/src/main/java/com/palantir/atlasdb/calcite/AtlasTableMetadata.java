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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.immutables.value.Value;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.table.description.DynamicColumnDescription;
import com.palantir.atlasdb.table.description.NamedColumnDescription;
import com.palantir.atlasdb.table.description.TableMetadata;

@Value.Immutable(builder = false)
public abstract class AtlasTableMetadata {
    @Value.Parameter
    public abstract TableMetadata metadata();

    @Value.Derived
    public List<AtlasColumnMetdata> columns() {
        ImmutableList.Builder<AtlasColumnMetdata> builder = ImmutableList.builder();
        metadata().getRowMetadata().getRowParts().forEach(comp ->
                builder.add(
                        ImmutableAtlasColumnMetdata.builder()
                                .metadata(metadata())
                                .component(comp)
                                .build()));
        Set<NamedColumnDescription> namedCols = metadata().getColumns().getNamedColumns();
        if (namedCols != null) {
            metadata().getColumns().getNamedColumns().forEach(col ->
                    builder.add(
                            ImmutableAtlasColumnMetdata
                                    .builder()
                                    .metadata(metadata())
                                    .column(col)
                                    .build()));
        }
        DynamicColumnDescription dynCol = metadata().getColumns().getDynamicColumn();
        if (dynCol != null) {
            dynCol.getColumnNameDesc().getRowParts().forEach(comp ->
                    builder.add(
                            ImmutableAtlasColumnMetdata
                                    .builder()
                                    .dynamicColumn(true)
                                    .metadata(metadata())
                                    .component(comp)
                                    .build()));
            builder.add(ImmutableAtlasColumnMetdata.builder().metadata(metadata()).value(dynCol.getValue()).build());
        }
        return builder.build();
    }

    @Value.Derived
    public List<AtlasColumnMetdata> rowComponents() {
        return columns().stream()
                .filter(AtlasColumnMetdata::isComponent)
                .filter(col -> !col.dynamicColumn())
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<AtlasColumnMetdata> namedColumns() {
        return columns().stream()
                .filter(AtlasColumnMetdata::isNamedColumn)
                .collect(Collectors.toList());
    }

    @Value.Derived
    public List<AtlasColumnMetdata> dynamicColumnComponents() {
        return columns().stream()
                .filter(AtlasColumnMetdata::isComponent)
                .filter(AtlasColumnMetdata::dynamicColumn)
                .collect(Collectors.toList());
    }

    @Value.Lazy
    public AtlasColumnMetdata valueColumn() {
        Preconditions.checkArgument(hasDynamicColumns(), "Only tables with dynamic columns have value columns.");
        return Iterables.getOnlyElement(
                columns().stream()
                        .filter(AtlasColumnMetdata::isValue)
                        .collect(Collectors.toList()));
    }

    @Value.Derived
    public boolean hasDynamicColumns() {
        return  dynamicColumnComponents().size() != 0;
    }

    @Value.Derived
    public List<String> namedColumnNames() {
        return columns().stream()
                .filter(col -> col.column().isPresent())
                .map(AtlasColumnMetdata::getName)
                .collect(Collectors.toList());
    }

    public RelDataType relDataType(RelDataTypeFactory factory) {
        Map<String, RelDataType> nameToType = columns().stream().collect(
                Collectors.toMap(
                        AtlasColumnMetdata::getDisplayName,
                        col -> col.relDataType(factory),
                        (v1,v2) -> { throw new RuntimeException(String.format("Duplicate key for values %s and %s", v1, v2)); },
                        LinkedHashMap::new
                ));
        return factory.createStructType(Lists.newArrayList(nameToType.entrySet()));
    }

}
