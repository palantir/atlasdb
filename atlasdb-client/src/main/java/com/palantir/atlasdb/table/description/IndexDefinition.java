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
package com.palantir.atlasdb.table.description;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.render.Renderers;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.logsafe.exceptions.SafeIllegalStateException;
import java.util.List;
import java.util.Set;

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

    /**
     * Indicates that the name of the table being defined is safe or unsafe for logging.
     *
     * Note that this method throws an exception if exposed to contradictory information (e.g. is called twice, with
     * values of both SAFE and UNSAFE).
     */
    public void indexNameLogSafety(TableMetadataPersistence.LogSafety logSafety) {
        com.palantir.logsafe.Preconditions.checkState(!(logSafetyDeclared && tableNameSafety != logSafety),
                "This table name's safety for logging has already been declared.");
        com.palantir.logsafe.Preconditions.checkState(state == IndexDefinition.State.NONE, "Specifying a table name is safe or unsafe should be done outside"
                + " of the subscopes of TableDefinition.");
        logSafetyDeclared = true;
        tableNameSafety = logSafety;
    }

    /**
     * If specified, this indicates that the names of all row components and named columns should be marked as safe by
     * default. Individual row components or named columns may still be marked as unsafe by explicitly creating them
     * as unsafe (by constructing them with UNSAFE values of LogSafety).
     *
     * Note that specifying this by itself DOES NOT make the table name safe for logging.
     *
     * Note that this DOES NOT make the values of either the rows or the columns safe for logging.
     */
    public void namedComponentsSafeByDefault() {
        com.palantir.logsafe.Preconditions.checkState(state == IndexDefinition.State.NONE, "Specifying components are safe by default should be done outside"
                + " of the subscopes of TableDefinition.");
        defaultNamedComponentLogSafety = TableMetadataPersistence.LogSafety.SAFE;
    }

    /**
     * If specified, makes both the table name and ALL named components safe by default.
     * Individual row components or named columns may still be marked as unsafe by explicitly creating them as unsafe.
     *
     * If you wish to have a table with an unsafe name but safe components, please use namedComponentsSafeByDefault()
     * instead.
     *
     * Note that this DOES NOT make the values of either the rows or the columns safe for logging.
     */
    public void allSafeForLoggingByDefault() {
        indexNameLogSafety(TableMetadataPersistence.LogSafety.SAFE);
        namedComponentsSafeByDefault();
    }

    public void componentFromRow(String componentName, ValueType valueType) {
        componentFromRow(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void componentFromRow(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        componentFromRow(componentName, valueType, valueByteOrder, componentName, defaultNamedComponentLogSafety);
    }

    public void componentFromRow(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            TableMetadataPersistence.LogSafety logSafety) {
        componentFromRow(componentName, valueType, valueByteOrder, componentName, logSafety);
    }

    public void componentFromRow(String componentName, ValueType valueType, String sourceComponentName) {
        componentFromRow(componentName, valueType, sourceComponentName, defaultNamedComponentLogSafety);
    }

    public void componentFromRow(String componentName, ValueType valueType, String sourceComponentName,
            TableMetadataPersistence.LogSafety logSafety) {
        componentFromRow(componentName, valueType, ValueByteOrder.ASCENDING, sourceComponentName, logSafety);
    }

    public void componentFromRow(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceComponentName) {
        componentFromRow(componentName, valueType, valueByteOrder, sourceComponentName, defaultNamedComponentLogSafety);
    }

    public void componentFromRow(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceComponentName,
            TableMetadataPersistence.LogSafety logSafety) {
        addComponent(IndexComponent.createFromRow(
                new NameComponentDescription.Builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .logSafety(logSafety)
                        .build(),
                sourceComponentName));
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType) {
        componentFromDynamicColumn(componentName, valueType, ValueByteOrder.ASCENDING, defaultNamedComponentLogSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType,
            TableMetadataPersistence.LogSafety logSafety) {
        componentFromDynamicColumn(componentName, valueType, ValueByteOrder.ASCENDING, logSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        componentFromDynamicColumn(componentName, valueType, valueByteOrder, componentName,
                defaultNamedComponentLogSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            TableMetadataPersistence.LogSafety logSafety) {
        componentFromDynamicColumn(componentName, valueType, valueByteOrder, componentName, logSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, String sourceComponentName) {
        componentFromDynamicColumn(componentName, valueType, ValueByteOrder.ASCENDING, sourceComponentName,
                defaultNamedComponentLogSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, String sourceComponentName,
            TableMetadataPersistence.LogSafety logSafety) {
        componentFromDynamicColumn(componentName, valueType, ValueByteOrder.ASCENDING, sourceComponentName, logSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceComponentName) {
        componentFromDynamicColumn(componentName, valueType, valueByteOrder, sourceComponentName,
                defaultNamedComponentLogSafety);
    }

    public void componentFromDynamicColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceComponentName,
            TableMetadataPersistence.LogSafety logSafety) {
        addComponent(IndexComponent.createFromDynamicColumn(
                new NameComponentDescription.Builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .logSafety(logSafety)
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
        com.palantir.logsafe.Preconditions.checkState(numberOfComponents >= 0,
                "Need to specify a non-negative number of components to hash.");
        checkHashRowComponentsPreconditions("hashFirstNRowComponents");
        numberOfComponentsHashed = numberOfComponents;
        ignoreHotspottingChecks = true;
    }

    public void partition(RowNamePartitioner... partitioners) {
        com.palantir.logsafe.Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        IndexComponent last = rowComponents.get(rowComponents.size() - 1);
        rowComponents.set(rowComponents.size() - 1, last.withPartitioners(partitioners));
    }

    public ExplicitRowNamePartitioner explicit(String... componentValues) {
        com.palantir.logsafe.Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        return new ExplicitRowNamePartitioner(rowComponents.get(rowComponents.size() - 1).rowKeyDesc.getType(),
                ImmutableSet.copyOf(componentValues));
    }

    public ExplicitRowNamePartitioner explicit(long... componentValues) {
        com.palantir.logsafe.Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        Set<String> set = Sets.newHashSet();
        for (long l : componentValues) {
            set.add(Long.toString(l));
        }
        return new ExplicitRowNamePartitioner(rowComponents.get(rowComponents.size() - 1).rowKeyDesc.getType(), set);
    }

    public UniformRowNamePartitioner uniform() {
        com.palantir.logsafe.Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS);
        return new UniformRowNamePartitioner(rowComponents.get(rowComponents.size() - 1).rowKeyDesc.getType());
    }

    public void componentFromColumn(String componentName, ValueType valueType, String sourceColumnName,
            String codeToAccessValue) {
        componentFromColumn(componentName, valueType, ValueByteOrder.ASCENDING, sourceColumnName, codeToAccessValue,
                defaultNamedComponentLogSafety);
    }

    public void componentFromColumn(String componentName, ValueType valueType, String sourceColumnName,
            String codeToAccessValue, TableMetadataPersistence.LogSafety logSafety) {
        componentFromColumn(componentName, valueType, ValueByteOrder.ASCENDING, sourceColumnName, codeToAccessValue,
                logSafety);
    }

    public void componentFromColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceColumnName, String codeToAccessValue) {
        componentFromColumn(componentName, valueType, valueByteOrder, sourceColumnName, codeToAccessValue,
                defaultNamedComponentLogSafety);
    }

    public void componentFromColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceColumnName, String codeToAccessValue,
            TableMetadataPersistence.LogSafety logSafety) {
        addComponent(IndexComponent.createFromColumn(
                new NameComponentDescription.Builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .logSafety(logSafety)
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
        componentFromIterableColumn(componentName, valueType, valueByteOrder, sourceColumnName, codeToAccessValue,
                defaultNamedComponentLogSafety);
    }

    /**
     * Allows multiple index rows when indexing by a cell with iterable values.
     * It doesn't support arbitrary protobuf structures - you need to be able to extract an iterable<valueType>
     *     using codeToAccessValue.
     */
    public void componentFromIterableColumn(String componentName, ValueType valueType, ValueByteOrder valueByteOrder,
            String sourceColumnName, String codeToAccessValue,
            TableMetadataPersistence.LogSafety logSafety) {
        addComponent(IndexComponent.createIterableFromColumn(
                new NameComponentDescription.Builder()
                        .componentName(componentName)
                        .type(valueType)
                        .byteOrder(valueByteOrder)
                        .logSafety(logSafety)
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
            throw new SafeIllegalStateException("Can only specify components when defining row or column names.");
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
        com.palantir.logsafe.Preconditions.checkState(!rowComponents.isEmpty(), "No row components specified.");
        validateFirstRowComp(rowComponents.get(0).getRowKeyDescription());
    }

    private void checkHashRowComponentsPreconditions(String methodName) {
        Preconditions.checkState(state == State.DEFINING_ROW_COMPONENTS,
                "Can only indicate %s inside the rowName scope.", methodName);
        Preconditions.checkState(rowComponents.isEmpty(),
                "%s must be the first row component.", methodName);
    }

    // State machine variables, that ensure the index definition is well-formed
    private State state = State.NONE;
    private boolean logSafetyDeclared = false;

    private boolean hashFirstRowComponent = false;
    private int numberOfComponentsHashed = 0;
    private String sourceTableName = null;
    private String javaIndexTableName = null;
    private List<IndexComponent> rowComponents = Lists.newArrayList();
    private List<IndexComponent> colComponents = Lists.newArrayList();
    private IndexCondition indexCondition = null;
    private final IndexType indexType;
    private TableMetadataPersistence.LogSafety tableNameSafety = TableMetadataPersistence.LogSafety.UNSAFE;
    private TableMetadataPersistence.LogSafety defaultNamedComponentLogSafety =
            TableMetadataPersistence.LogSafety.UNSAFE;

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
        com.palantir.logsafe.Preconditions.checkState(indexTableName != null, "No index table name specified.");
        com.palantir.logsafe.Preconditions.checkState(!rowComponents.isEmpty(), "No row components specified.");
        if (explicitCompressionRequested && explicitCompressionBlockSizeKb == 0) {
            explicitCompressionBlockSizeKb = AtlasDbConstants.DEFAULT_INDEX_COMPRESSION_BLOCK_SIZE_KB;
        }

        if (colComponents.isEmpty()) {
            return IndexMetadata.createIndex(
                    indexTableName,
                    javaIndexTableName,
                    rowComponents,
                    cachePriority,
                    conflictHandler,
                    rangeScanAllowed,
                    explicitCompressionBlockSizeKb,
                    negativeLookups,
                    indexCondition,
                    indexType,
                    sweepStrategy,
                    appendHeavyAndReadLight,
                    numberOfComponentsHashed,
                    tableNameSafety);
        } else {
            return IndexMetadata.createDynamicIndex(
                    indexTableName,
                    javaIndexTableName,
                    rowComponents,
                    colComponents,
                    cachePriority,
                    conflictHandler,
                    rangeScanAllowed,
                    explicitCompressionBlockSizeKb,
                    negativeLookups,
                    indexCondition,
                    indexType,
                    sweepStrategy,
                    appendHeavyAndReadLight,
                    numberOfComponentsHashed,
                    tableNameSafety);
        }
    }
}
