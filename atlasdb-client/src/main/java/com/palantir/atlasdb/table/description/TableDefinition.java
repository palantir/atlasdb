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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.AbstractMessage;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.LogSafety;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.description.constraints.ConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.ForeignKeyConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.RowConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.TableConstraint;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.persist.Persistable;
import com.palantir.logsafe.exceptions.SafeIllegalArgumentException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Defines a table for a schema.
 *
 * Can be thought of as a builder for {@link TableMetadata} objects.
 */
public class TableDefinition extends AbstractDefinition {

    @Override
    protected ConflictHandler defaultConflictHandler() {
        return ConflictHandler.RETRY_ON_WRITE_WRITE;
    }

    public void rowName() {
        state = State.DEFINING_ROW_NAME;
    }

    public void columns() {
        state = State.DEFINING_COLUMNS;
    }

    public void dynamicColumns() {
        state = State.DEFINING_DYNAMIC_COLUMN;
    }

    public void constraints() {
        state = State.DEFINING_CONSTRAINTS;
    }

    /**
     * Indicates that the name of the table being defined is safe or unsafe for logging.
     *
     * Note that this method throws an exception if exposed to contradictory information (e.g. is called twice, with
     * values of both SAFE and UNSAFE).
     */
    public void tableNameLogSafety(LogSafety logSafety) {
        com.palantir.logsafe.Preconditions.checkState(
                !(logSafetyDeclared && tableNameSafety != logSafety),
                "This table name's safety for logging has already been declared.");
        com.palantir.logsafe.Preconditions.checkState(
                state == State.NONE,
                "Specifying a table name is safe or unsafe should be done outside"
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
        com.palantir.logsafe.Preconditions.checkState(
                state == State.NONE,
                "Specifying components are safe by default should be done outside"
                        + " of the subscopes of TableDefinition.");
        defaultNamedComponentLogSafety = LogSafety.SAFE;
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
        tableNameLogSafety(LogSafety.SAFE);
        namedComponentsSafeByDefault();
    }

    public void column(String columnName, String shortName, Class<?> protoOrPersistable) {
        column(columnName, shortName, protoOrPersistable, Compression.NONE);
    }

    public void column(String columnName, String shortName, Class<?> protoOrPersistable, Compression compression) {
        column(columnName, shortName, protoOrPersistable, compression, defaultNamedComponentLogSafety);
    }

    public void column(
            String columnName,
            String shortName,
            Class<?> protoOrPersistable,
            Compression compression,
            LogSafety columnNameLoggable) {
        checkStateForNamedColumnDefinition();
        checkUniqueColumnNames(columnName, shortName);
        fixedColumns.add(new NamedColumnDescription(
                shortName, columnName, getColumnValueDescription(protoOrPersistable, compression), columnNameLoggable));
    }

    public void column(String columnName, String shortName, ValueType valueType) {
        column(columnName, shortName, valueType, defaultNamedComponentLogSafety);
    }

    public void column(String columnName, String shortName, ValueType valueType, LogSafety columnNameLoggable) {
        checkStateForNamedColumnDefinition();
        checkUniqueColumnNames(columnName, shortName);
        fixedColumns.add(new NamedColumnDescription(
                shortName, columnName, ColumnValueDescription.forType(valueType), columnNameLoggable));
    }

    private void checkStateForNamedColumnDefinition() {
        com.palantir.logsafe.Preconditions.checkState(
                state == State.DEFINING_COLUMNS, "Can only define named columns when in the columns scope.");
        com.palantir.logsafe.Preconditions.checkState(
                !noColumns, "Cannot define named columns if noColumns() was already indicated");
    }

    public void noColumns() {
        com.palantir.logsafe.Preconditions.checkState(
                state != State.DEFINING_COLUMNS, "Cannot declare noColumns() inside the column scope.");
        com.palantir.logsafe.Preconditions.checkState(
                fixedColumns.isEmpty(), "Cannot declare noColumns() if columns have already been declared.");
        fixedColumns.add(new NamedColumnDescription("e", "exists", ColumnValueDescription.forType(ValueType.VAR_LONG)));
        noColumns = true;
    }

    private void checkUniqueColumnNames(String columnName, String shortName) {
        Preconditions.checkState(
                !fixedColumnShortNames.contains(shortName), "Duplicate short column name found: %s", shortName);
        Preconditions.checkState(
                !fixedColumnLongNames.contains(columnName), "Duplicate long column name found: %s", columnName);
        fixedColumnShortNames.add(shortName);
        fixedColumnLongNames.add(columnName);
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
        com.palantir.logsafe.Preconditions.checkState(
                numberOfComponents >= 0, "Need to specify a non-negative number of components to hash.");
        checkHashRowComponentsPreconditions("hashFirstNRowComponents");
        numberOfComponentsHashed = numberOfComponents;
        ignoreHotspottingChecks = true;
    }

    /**
     * Returns the number of components to hash as a prefix to row keys.
     */
    public int getNumberOfComponentsHashed() {
        return numberOfComponentsHashed;
    }

    public void rowComponent(String componentName, ValueType valueType) {
        rowComponent(componentName, valueType, defaultNamedComponentLogSafety);
    }

    public void rowComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        rowComponent(componentName, valueType, valueByteOrder, defaultNamedComponentLogSafety);
    }

    public void rowComponent(String componentName, ValueType valueType, LogSafety rowNameLoggable) {
        rowComponent(componentName, valueType, ValueByteOrder.ASCENDING, rowNameLoggable);
    }

    public void rowComponent(
            String componentName, ValueType valueType, ValueByteOrder valueByteOrder, LogSafety rowNameLoggable) {
        com.palantir.logsafe.Preconditions.checkState(
                state == State.DEFINING_ROW_NAME, "Can only declare a row component inside the rowName scope.");
        rowNameComponents.add(new NameComponentDescription.Builder()
                .componentName(componentName)
                .type(valueType)
                .byteOrder(valueByteOrder)
                .logSafety(rowNameLoggable)
                .build());
    }

    /**
     * Passing an empty list of partitioners means that this type is not able to be partitioned. This is sometimes
     * needed if the value type doesn't support the {@link UniformRowNamePartitioner}.  If the first entry in the table
     * cannot be partitioned then it is likely this table will cause hot spots.
     * <p>
     * If no partition() is specified the default is to use a {@link UniformRowNamePartitioner}
     */
    public void partition(RowNamePartitioner... partitioners) {
        checkStateForPartitioner();
        NameComponentDescription last = rowNameComponents.get(rowNameComponents.size() - 1);
        rowNameComponents.set(rowNameComponents.size() - 1, last.withPartitioners(partitioners));
    }

    public ExplicitRowNamePartitioner explicit(String... componentValues) {
        checkStateForPartitioner();
        return new ExplicitRowNamePartitioner(
                rowNameComponents.get(rowNameComponents.size() - 1).getType(), ImmutableSet.copyOf(componentValues));
    }

    public ExplicitRowNamePartitioner explicit(long... componentValues) {
        checkStateForPartitioner();
        Set<String> set = new HashSet<>();
        for (long l : componentValues) {
            set.add(Long.toString(l));
        }
        return new ExplicitRowNamePartitioner(
                rowNameComponents.get(rowNameComponents.size() - 1).getType(), set);
    }

    public UniformRowNamePartitioner uniform() {
        checkStateForPartitioner();
        return new UniformRowNamePartitioner(
                rowNameComponents.get(rowNameComponents.size() - 1).getType());
    }

    private void checkStateForPartitioner() {
        com.palantir.logsafe.Preconditions.checkState(
                state == State.DEFINING_ROW_NAME, "Can only define a partitioner inside the rowName scope.");
    }

    public void columnComponent(String componentName, ValueType valueType) {
        columnComponent(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void columnComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        com.palantir.logsafe.Preconditions.checkState(
                state == State.DEFINING_DYNAMIC_COLUMN,
                "Can only define a dynamic column component inside the dynamicColumns scope.");
        dynamicColumnNameComponents.add(new NameComponentDescription.Builder()
                .componentName(componentName)
                .type(valueType)
                .byteOrder(valueByteOrder)
                .build());
    }

    public void value(Class<?> protoOrPersistable) {
        value(protoOrPersistable, Compression.NONE);
    }

    public void value(Class<?> protoOrPersistable, Compression compression) {
        checkStateForDynamicColumnValues();
        dynamicColumnValue = getColumnValueDescription(protoOrPersistable, compression);
    }

    public void value(ValueType valueType) {
        checkStateForDynamicColumnValues();
        dynamicColumnValue = ColumnValueDescription.forType(valueType);
        if (maxValueSize == Integer.MAX_VALUE) {
            maxValueSize = valueType.getMaxValueSize();
        }
    }

    private void checkStateForDynamicColumnValues() {
        com.palantir.logsafe.Preconditions.checkState(
                state == State.DEFINING_DYNAMIC_COLUMN, "Can only define a value inside the dynamicColumns scope.");
    }

    public void tableConstraint(TableConstraint constraint) {
        checkStateForTableConstraints();
        constraintBuilder.addTableConstraint(constraint);
    }

    public void rowConstraint(RowConstraintMetadata constraint) {
        checkStateForTableConstraints();
        constraintBuilder.addRowConstraint(constraint);
    }

    public void foreignKeyConstraint(ForeignKeyConstraintMetadata constraint) {
        checkStateForTableConstraints();
        constraintBuilder.addForeignKeyConstraint(constraint);
    }

    private void checkStateForTableConstraints() {
        com.palantir.logsafe.Preconditions.checkState(
                state == State.DEFINING_CONSTRAINTS, "Can only define a constraint inside the constraints scope.");
    }

    public void maxValueSize(int size) {
        maxValueSize = size;
    }

    public int getMaxValueSize() {
        return maxValueSize;
    }

    public void genericTableName(String name) {
        genericTableName = name;
    }

    public String getGenericTableName() {
        return genericTableName;
    }

    public void javaTableName(String name) {
        javaTableName = name;
    }

    public String getJavaTableName() {
        return javaTableName;
    }

    public boolean hasV2TableEnabled() {
        return this.v2TableEnabled;
    }

    /**
     * Enables generates of a separate set of "v2" tables, with simplified APIs for reading and writing data.
     *
     * This is a beta feature. API stability is not guaranteed, and the risk of defects is higher.
     */
    @Beta
    public void enableV2Table() {
        this.v2TableEnabled = true;
    }

    public void validate() {
        toTableMetadata();
        getConstraintMetadata();
        com.palantir.logsafe.Preconditions.checkState(!rowNameComponents.isEmpty(), "No row name components defined.");
        validateFirstRowComp(rowNameComponents.get(0));
    }

    private enum State {
        NONE,
        DEFINING_ROW_NAME,
        DEFINING_DYNAMIC_COLUMN,
        DEFINING_COLUMNS,
        DEFINING_CONSTRAINTS,
    }

    // State machine variables, that ensure the table definition is well-formed
    private State state = State.NONE;
    private boolean logSafetyDeclared = false;

    // Table definition properties
    private int maxValueSize = Integer.MAX_VALUE;
    private String genericTableName = null;
    private String javaTableName = null;
    private int numberOfComponentsHashed = 0;
    private List<NameComponentDescription> rowNameComponents = new ArrayList<>();
    private List<NamedColumnDescription> fixedColumns = new ArrayList<>();
    private List<NameComponentDescription> dynamicColumnNameComponents = new ArrayList<>();
    private ColumnValueDescription dynamicColumnValue = null;
    private ConstraintMetadata.Builder constraintBuilder = ConstraintMetadata.builder();
    private Set<String> fixedColumnShortNames = new HashSet<>();
    private Set<String> fixedColumnLongNames = new HashSet<>();
    private boolean noColumns = false;
    private LogSafety tableNameSafety = LogSafety.UNSAFE;
    private LogSafety defaultNamedComponentLogSafety = LogSafety.UNSAFE;
    private boolean v2TableEnabled = false;

    public TableMetadata toTableMetadata() {
        com.palantir.logsafe.Preconditions.checkState(!rowNameComponents.isEmpty(), "No row name components defined.");

        if (explicitCompressionRequested && explicitCompressionBlockSizeKb == 0) {
            if (rangeScanAllowed) {
                explicitCompressionBlockSizeKb =
                        AtlasDbConstants.DEFAULT_TABLE_WITH_RANGESCANS_COMPRESSION_BLOCK_SIZE_KB;
            } else {
                explicitCompressionBlockSizeKb = AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB;
            }
        }

        return TableMetadata.builder()
                .rowMetadata(NameMetadataDescription.create(rowNameComponents, numberOfComponentsHashed))
                .columns(getColumnMetadataDescription())
                .conflictHandler(conflictHandler)
                .cachePriority(cachePriority)
                .rangeScanAllowed(rangeScanAllowed)
                .explicitCompressionBlockSizeKB(explicitCompressionBlockSizeKb)
                .negativeLookups(negativeLookups)
                .sweepStrategy(sweepStrategy)
                .appendHeavyAndReadLight(appendHeavyAndReadLight)
                .nameLogSafety(tableNameSafety)
                .build();
    }

    private ColumnMetadataDescription getColumnMetadataDescription() {
        if (!fixedColumns.isEmpty()) {
            com.palantir.logsafe.Preconditions.checkState(
                    dynamicColumnNameComponents.isEmpty(), "Cannot define both dynamic and fixed columns.");
            return new ColumnMetadataDescription(fixedColumns);
        } else {
            com.palantir.logsafe.Preconditions.checkState(
                    !dynamicColumnNameComponents.isEmpty() && dynamicColumnValue != null,
                    "Columns not properly defined.");
            return new ColumnMetadataDescription(new DynamicColumnDescription(
                    NameMetadataDescription.create(dynamicColumnNameComponents), dynamicColumnValue));
        }
    }

    public ConstraintMetadata getConstraintMetadata() {
        return constraintBuilder.build();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private ColumnValueDescription getColumnValueDescription(Class protoOrPersistable, Compression compression) {
        if (AbstractMessage.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forProtoMessage(protoOrPersistable, compression);
        } else if (Persister.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forPersister(protoOrPersistable, compression);
        } else if (Persistable.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forPersistable(protoOrPersistable, compression);
        } else {
            throw new SafeIllegalArgumentException("Expected either protobuf or Persistable class.");
        }
    }

    private void checkHashRowComponentsPreconditions(String methodName) {
        Preconditions.checkState(
                state == State.DEFINING_ROW_NAME, "Can only indicate %s inside the rowName scope.", methodName);
        Preconditions.checkState(rowNameComponents.isEmpty(), "%s must be the first row component.", methodName);
    }
}
