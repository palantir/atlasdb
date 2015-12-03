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
package com.palantir.atlasdb.table.description;

import java.util.List;

import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.CachePriority;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ExpirationStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.PartitionStrategy;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.SweepStrategy;
import com.palantir.atlasdb.table.description.IndexDefinition.IndexType;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

public class IndexMetadata {
    final String name;
    final String javaName;
    final ImmutableList<IndexComponent> rowComponents;
    final ImmutableList<IndexComponent> colComponents;
    @Nullable final String columnNameToGetData;
    final CachePriority cachePriority;
    final PartitionStrategy partitionStrategy;
    boolean rangeScanAllowed;
    int explicitCompressionBlockSizeKB;
    boolean negativeLookups;
    final ConflictHandler conflictHandler;
    final IndexCondition indexCondition;
    IndexType indexType;
    final SweepStrategy sweepStrategy;
    private final ExpirationStrategy expirationStrategy;
    private boolean appendHeavyAndReadLight;

    public static IndexMetadata createIndex(String name,
                                            String javaName,
                                            Iterable<IndexComponent> rowComponents,
                                            CachePriority cachePriority,
                                            PartitionStrategy partitionStrategy,
                                            ConflictHandler conflictHandler,
                                            boolean rangeScanAllowed,
                                            int explicitCompressionBlockSizeKB,
                                            boolean negativeLookups,
                                            IndexCondition indexCondition,
                                            IndexType indexType,
                                            SweepStrategy sweepStrategy,
                                            ExpirationStrategy expirationStrategy,
                                            boolean appendHeavyAndReadLight) {
        Validate.isTrue(!Iterables.isEmpty(rowComponents));
        Iterable<IndexComponent> colComponents = ImmutableList.<IndexComponent>of();
        return new IndexMetadata(
                name,
                javaName,
                rowComponents,
                colComponents,
                getColNameToAccessFrom(rowComponents, colComponents, indexCondition),
                cachePriority,
                partitionStrategy,
                conflictHandler,
                rangeScanAllowed,
                explicitCompressionBlockSizeKB,
                negativeLookups,
                indexCondition,
                indexType,
                sweepStrategy,
                expirationStrategy,
                appendHeavyAndReadLight);
    }

    public static IndexMetadata createDynamicIndex(String name,
                                                   String javaName,
                                                   Iterable<IndexComponent> rowComponents,
                                                   Iterable<IndexComponent> colComponents,
                                                   CachePriority cachePriority,
                                                   PartitionStrategy partitionStrategy,
                                                   ConflictHandler conflictHandler,
                                                   boolean rangeScanAllowed,
                                                   int explicitCompressionBlockSizeKB,
                                                   boolean negativeLookups,
                                                   IndexCondition indexCondition,
                                                   IndexType indexType,
                                                   SweepStrategy sweepStrategy,
                                                   ExpirationStrategy expirationStrategy,
                                                   boolean appendHeavyAndReadLight) {
        Validate.isTrue(!Iterables.isEmpty(rowComponents));
        Validate.isTrue(!Iterables.isEmpty(colComponents));
        return new IndexMetadata(
                name,
                javaName,
                rowComponents,
                colComponents,
                getColNameToAccessFrom(rowComponents, colComponents, indexCondition),
                cachePriority,
                partitionStrategy,
                conflictHandler,
                rangeScanAllowed,
                explicitCompressionBlockSizeKB,
                negativeLookups,
                indexCondition,
                indexType,
                sweepStrategy,
                expirationStrategy,
                appendHeavyAndReadLight);
    }

    private IndexMetadata(String name,
                          String javaName,
                          Iterable<IndexComponent> rowComponents,
                          Iterable<IndexComponent> colComponents,
                          String colNameToAccessFrom,
                          CachePriority cachePriority,
                          PartitionStrategy partitionStrategy,
                          ConflictHandler conflictHandler,
                          boolean rangeScanAllowed,
                          int explicitCompressionBlockSizeKB,
                          boolean negativeLookups,
                          IndexCondition indexCondition,
                          IndexType indexType,
                          SweepStrategy sweepStrategy,
                          ExpirationStrategy expirationStrategy,
                          boolean appendHeavyAndReadLight) {
        this.name = name;
        this.javaName = javaName;
        this.rowComponents = ImmutableList.copyOf(rowComponents);
        this.colComponents = ImmutableList.copyOf(colComponents);
        this.columnNameToGetData = colNameToAccessFrom;
        this.cachePriority = cachePriority;
        this.partitionStrategy = partitionStrategy;
        this.conflictHandler = conflictHandler;
        this.rangeScanAllowed = rangeScanAllowed;
        this.explicitCompressionBlockSizeKB = explicitCompressionBlockSizeKB;
        this.negativeLookups = negativeLookups;
        this.indexCondition = indexCondition;
        this.indexType = indexType;
        this.sweepStrategy = sweepStrategy;
        this.expirationStrategy = expirationStrategy;
        this.appendHeavyAndReadLight = appendHeavyAndReadLight;
    }

    private static String getColNameToAccessFrom(Iterable<IndexComponent> rowComponents,
                                                 Iterable<IndexComponent> colComponents,
                                                 IndexCondition indexCondition) {

        String colNameToAccessFrom = null;
        for (IndexComponent indexComponent : Iterables.concat(rowComponents, colComponents)) {
            if (indexComponent.columnNameToGetData == null) {
                continue;
            }
            if (colNameToAccessFrom == null) {
                colNameToAccessFrom = indexComponent.columnNameToGetData;
            }
            if (!indexComponent.columnNameToGetData.equals(colNameToAccessFrom)) {
                throw new IllegalArgumentException("An index must only reference one column."
                        + "This references " + indexComponent.columnNameToGetData + " and "
                        + colNameToAccessFrom);
            }
        }
        if (indexCondition != null) {
            if (colNameToAccessFrom == null) {
                colNameToAccessFrom = indexCondition.getSourceColumn();
            }
            if (!indexCondition.getSourceColumn().equals(colNameToAccessFrom)) {
                throw new IllegalArgumentException("An index must only reference one column."
                        + "This references " + indexCondition.getSourceColumn() + " and "
                        + colNameToAccessFrom);
            }
        }
        return colNameToAccessFrom;
    }

    public TableMetadata getTableMetadata() {
        List<NameComponentDescription> rowDescList = Lists.newArrayList();
        for (IndexComponent indexComp : rowComponents) {
            rowDescList.add(indexComp.rowKeyDesc);
        }

        ColumnMetadataDescription column;
        if (indexType.equals(IndexType.ADDITIVE)) {
            if (colComponents.isEmpty()) {
                column = getAdditiveIndexColumn();
            } else {
                List<NameComponentDescription> colDescList = Lists.newArrayList();
                for (IndexComponent indexComp : colComponents) {
                    colDescList.add(indexComp.rowKeyDesc);
                }
                column = getDynamicAdditiveIndexColumn(colDescList);
            }
        } else if (indexType.equals(IndexType.CELL_REFERENCING)) {
            List<NameComponentDescription> colDescList = Lists.newArrayList();
            for (IndexComponent indexComp : colComponents) {
                colDescList.add(indexComp.rowKeyDesc);
            }
            column = getCellReferencingIndexColumn(colDescList);
        } else {
            throw new IllegalArgumentException("Unknown index type " + indexType);
        }
        return new TableMetadata(
                NameMetadataDescription.create(rowDescList),
                column,
                conflictHandler,
                cachePriority,
                partitionStrategy,
                rangeScanAllowed,
                explicitCompressionBlockSizeKB,
                negativeLookups,
                sweepStrategy,
                expirationStrategy,
                appendHeavyAndReadLight);
    }

    public boolean isDynamicIndex() {
        return !colComponents.isEmpty();
    }

    public List<IndexComponent> getRowComponents() {
        return rowComponents;
    }

    /**
     * Calling this is only meaningful if {@link #isDynamicIndex()} returns true.
     */
    public List<IndexComponent> getColumnComponents() {
        return colComponents;
    }

    @Nullable
    public String getColumnNameToAccessData() {
        return columnNameToGetData;
    }

    public String getIndexName() {
        return name;
    }

    public String getJavaIndexName() {
        return javaName;
    }

    public IndexCondition getIndexCondition() {
        return indexCondition;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public String getIndexTable() {
        String indexTable = getJavaIndexName();
        if (indexTable == null) {
            indexTable = Renderers.CamelCase(getIndexName());
        }
        return indexTable;
    }

    private static ColumnMetadataDescription getAdditiveIndexColumn() {
        ColumnValueDescription columnValue = ColumnValueDescription.forType(ValueType.VAR_LONG);
        NamedColumnDescription namedColumn = new NamedColumnDescription("e", "exists", columnValue);
        return new ColumnMetadataDescription(ImmutableList.of(namedColumn));
    }

    private static ColumnMetadataDescription getDynamicAdditiveIndexColumn(List<NameComponentDescription> components) {
        NameMetadataDescription columnDescription = NameMetadataDescription.create(components);
        ColumnValueDescription columnValue = ColumnValueDescription.forType(ValueType.VAR_LONG);
        DynamicColumnDescription dynamicColumn = new DynamicColumnDescription(columnDescription, columnValue);
        return new ColumnMetadataDescription(dynamicColumn);
    }

    private static ColumnMetadataDescription getCellReferencingIndexColumn(List<NameComponentDescription> components) {
        components = ImmutableList.<NameComponentDescription>builder()
                .add(new NameComponentDescription("row_name", ValueType.SIZED_BLOB))
                .add(new NameComponentDescription("column_name", ValueType.SIZED_BLOB))
                .addAll(components)
                .build();
        NameMetadataDescription columnDescription = NameMetadataDescription.create(components);
        ColumnValueDescription columnValue = ColumnValueDescription.forType(ValueType.VAR_LONG);
        DynamicColumnDescription dynamicColumn = new DynamicColumnDescription(columnDescription, columnValue);
        return new ColumnMetadataDescription(dynamicColumn);
    }
}
