/*
 * Copyright 2015 Palantir Technologies, Inc. All rights reserved.
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
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.atlasdb.transaction.api.ConflictHandler;

/**
 * Defines a secondary index for a schema.
 *
 * Can be thought of as a builder for {@link IndexMetadata} objects.
 */
public class IndexDefinition extends AbstractDefinition {

    @Override
    protected ConflictHandler defaultConflictHandler() {
        return ConflictHandler.IGNORE_ALL;
    }

    public IndexDefinition(IndexType indexType) {
        this.indexType = indexType;
        // indices have compression on by default
        this.explicitCompressionRequested = true;
    }

    public void onTable(String tableName) {
        sourceTableName = tableName;
    }

    public String getSourceTable() {
        return sourceTableName;
    }

    public void rowName() {
        state = State.DEFINING_ROW_COMPONENTS;
    }

    public void dynamicColumns() {
        state = State.DEFINING_COLUMN_COMPONENTS;
    }

    public void componentFromRow(String componentName, ValueType valueType) {
        componentFromRow(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void componentFromRow(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        componentFromRow(componentName, valueType, valueByteOrder, componentName);
    }

    public void componentFromRow(String componentName, ValueType valueType, String sourceComponentName) {
        componentFromRow(componentName, valueType, ValueByteOrder.ASCENDING, sourceComponentName);
    }

    public void componentFromRow(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceComponentName) {
        addComponent(IndexComponent.createFromRow(
                ImmutableNameComponentDescription.builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .build(),
                sourceComponentName));
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType) {
        componentFromDynamicColumn(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        componentFromDynamicColumn(componentName, valueType, valueByteOrder, componentName);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, String sourceComponentName) {
        componentFromDynamicColumn(componentName, valueType, ValueByteOrder.ASCENDING, sourceComponentName);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceComponentName) {
        addComponent(IndexComponent.createFromDynamicColumn(
                ImmutableNameComponentDescription.builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .build(),
                sourceComponentName));
    }

    /**
     * Prefix the row with a hash of the first row component
     * <p>
     * This helps to ensure that rows are evenly distributed. In particular, using strings as the first row component
     * will only cover the entire range of byte arrays because they're encode with UTF_8. In addition, using any
     * variable-length {@link ValueType} as the first row component type will not work because they are prefixed by the
     * length of the component. Finally, we can't use BLOB if there are multiple components because it must go at the
     * end of the row
     */
    public void hashFirstRowComponent() {
        checkHashRowComponentsPreconditions("hashFirstRowComponent");
        hashFirstNRowComponents(1);
    }

    /**
     * Prefix the row with a hash of the first N row components.
     * If using prefix range requests, the components that are hashed must also be specified in the prefix.
     */
    public void hashFirstNRowComponents(int numberOfComponents) {
        Preconditions.checkState(numberOfComponents >= 0,
                "Need to specify a non-negative number of components to hash.");
        checkHashRowComponentsPreconditions("hashFirstNRowComponents");
        numberOfComponentsHashed = numberOfComponents;
        ignoreHotspottingChecks = true;
    }

    public void partition(RowNamePartitioner... partitioners) {
        Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        IndexComponent last = rowComponents.get(rowComponents.size() - 1);
        rowComponents.set(rowComponents.size() - 1, last.withPartitioners(partitioners));
    }

    public ExplicitRowNamePartitioner explicit(String... componentValues) {
        Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        return new ExplicitRowNamePartitioner(rowComponents.get(rowComponents.size() - 1).rowKeyDesc.getType(),
                ImmutableSet.copyOf(componentValues));
    }

    public ExplicitRowNamePartitioner explicit(long... componentValues) {
        Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        Set<String> set = Sets.newHashSet();
        for (long l : componentValues) {
            set.add(Long.toString(l));
        }
        return new ExplicitRowNamePartitioner(rowComponents.get(rowComponents.size() - 1).rowKeyDesc.getType(), set);
    }

    public UniformRowNamePartitioner uniform() {
        Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        return new UniformRowNamePartitioner(rowComponents.get(rowComponents.size() - 1).rowKeyDesc.getType());
    }

    public void componentFromColumn(String componentName, ValueType valueType, String sourceColumnName,
            String codeToAccessValue) {
        componentFromColumn(componentName, valueType, ValueByteOrder.ASCENDING, sourceColumnName, codeToAccessValue);
    }

    public void componentFromColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceColumnName, String codeToAccessValue) {
        addComponent(IndexComponent.createFromColumn(
                ImmutableNameComponentDescription.builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .build(),
                sourceColumnName,
                codeToAccessValue));
    }

    /**
     * Allows multiple index rows when indexing by a cell with iterable values.
     * It doesn't support arbitrary protobuf structures - you need to be able to extract an iterable<valueType>
     *     using codeToAccessValue.
     */
    public void componentFromIterableColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceColumnName, String codeToAccessValue) {
        addComponent(IndexComponent.createIterableFromColumn(
                ImmutableNameComponentDescription.builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .build(),
                sourceColumnName,
                codeToAccessValue));
    }

    private void addComponent(IndexComponent component) {
        if (state == State.DEFINING_ROW_COMPONENTS) {
            rowComponents.add(component);
        } else if (state == State.DEFINING_COLUMN_COMPONENTS) {
            colComponents.add(component);
        } else {
            throw new IllegalStateException("Can only specify components when defining row or column names.");
        }
    }

    public int getMaxValueSize() {
        // N.B., indexes are always max value size of 1.
        return 1;
    }

    public void javaTableName(String name) {
        String suffix = Renderers.camelCase(indexType.getIndexSuffix());
        Preconditions.checkArgument(
                !name.endsWith(suffix),
                "Java index name cannot end with '%s'", suffix);
        this.javaIndexTableName = name + suffix;
    }

    public String getJavaTableName() {
        return javaIndexTableName;
    }

    public void onCondition(String sourceColumn, String booleanExpression) {
        indexCondition = new IndexCondition(sourceColumn, booleanExpression);
    }

    public IndexCondition getIndexCondition() {
        return indexCondition;
    }

    public IndexType getIndexType() {
        return indexType;
    }

    public void validate() {
        Preconditions.checkState(!rowComponents.isEmpty(), "No row components specified.");
        validateFirstRowComp(rowComponents.get(0).getRowKeyDescription());
    }

    private void checkHashRowComponentsPreconditions(String methodName) {
        Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS,
                "Can only indicate %s inside the rowName scope.", methodName);
        Preconditions.checkState(rowComponents.isEmpty(),
                "%s must be the first row component.", methodName);
    }

    private State state = State.NONE;
    private boolean hashFirstRowComponent = false;
    private int numberOfComponentsHashed = 0;
    private String sourceTableName = null;
    private String javaIndexTableName = null;
    private List<IndexComponent> rowComponents = Lists.newArrayList();
    private List<IndexComponent> colComponents = Lists.newArrayList();
    private IndexCondition indexCondition = null;
    private final IndexType indexType;

    public enum IndexType {
        ADDITIVE("_aidx"),
        CELL_REFERENCING("_idx");

        private final String indexSuffix;
        IndexType(String indexSuffix) {
            this.indexSuffix = indexSuffix;
        }

        public String getIndexSuffix() {
            return indexSuffix;
        }
    }

    private enum State {
        NONE,
        DEFINING_ROW_COMPONENTS,
        DEFINING_COLUMN_COMPONENTS
    }

    public IndexMetadata toIndexMetadata(String indexTableName) {
        Preconditions.checkState(indexTableName != null, "No index table name specified.");
        Preconditions.checkState(!rowComponents.isEmpty(), "No row components specified.");
        if (explicitCompressionRequested && explicitCompressionBlockSizeKb == 0) {
            explicitCompressionBlockSizeKb = AtlasDbConstants.DEFAULT_INDEX_COMPRESSION_BLOCK_SIZE_KB;
        }

        if (colComponents.isEmpty()) {
            return IndexMetadata.createIndex(
                    indexTableName,
                    javaIndexTableName,
                    rowComponents,
                    cachePriority,
                    partitionStrategy,
                    conflictHandler,
                    rangeScanAllowed,
                    explicitCompressionBlockSizeKb,
                    negativeLookups,
                    indexCondition,
                    indexType,
                    sweepStrategy,
                    expirationStrategy,
                    appendHeavyAndReadLight,
                    numberOfComponentsHashed);
        } else {
            return IndexMetadata.createDynamicIndex(
                    indexTableName,
                    javaIndexTableName,
                    rowComponents,
                    colComponents,
                    cachePriority,
                    partitionStrategy,
                    conflictHandler,
                    rangeScanAllowed,
                    explicitCompressionBlockSizeKb,
                    negativeLookups,
                    indexCondition,
                    indexType,
                    sweepStrategy,
                    expirationStrategy,
                    appendHeavyAndReadLight,
                    numberOfComponentsHashed);
        }
    }
}
