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
import com.google.protobuf.GeneratedMessage;
import com.palantir.atlasdb.AtlasDbConstants;
import com.palantir.atlasdb.persist.api.Persister;
import com.palantir.atlasdb.protos.generated.TableMetadataPersistence.ValueByteOrder;
import com.palantir.atlasdb.table.description.ColumnValueDescription.Compression;
import com.palantir.atlasdb.table.description.constraints.ConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.ForeignKeyConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.RowConstraintMetadata;
import com.palantir.atlasdb.table.description.constraints.TableConstraint;
import com.palantir.atlasdb.transaction.api.ConflictHandler;
import com.palantir.common.persist.Persistable;

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
     * Indicates that the name of the table being defined is safe for logging.
     */
    public void tableNameIsSafeLoggable() {
        Preconditions.checkState(state == State.NONE, "Specifying a table name is safe should be done outside of the"
                + " subscopes of TableDefinition.");
        tableNameLoggable = true;
    }

    /**
     * If specified, this indicates that the names of all row components and named columns should be marked as safe by
     * default. Individual row components or named columns may still be marked as unsafe by explicitly creating them
     * as unsafe (i.e. by constructing them with rowNameLoggable or columnNameLoggable as false, respectively).
     * Note that specifying this by itself does NOT make the table name safe for logging.
     */
    public void namedComponentsSafeByDefault() {
        Preconditions.checkState(state == State.NONE, "Specifying a table name is safe should be done outside of the"
                + " subscopes of TableDefinition.");
        defaultNamedComponentLogSafety = true;
    }

    public void column(String columnName, String shortName, Class<?> protoOrPersistable) {
        column(columnName, shortName, protoOrPersistable, defaultNamedComponentLogSafety);
    }

    public void column(String columnName, String shortName, Class<?> protoOrPersistable, boolean columnNameLoggable) {
        column(columnName, shortName, protoOrPersistable, Compression.NONE, columnNameLoggable);
    }

    public void column(
            String columnName,
            String shortName,
            Class<?> protoOrPersistable,
            Compression compression,
            boolean columnNameLoggable) {
        Preconditions.checkState(state == State.DEFINING_COLUMNS);
        Preconditions.checkState(!noColumns);
        checkUniqueColumnNames(columnName, shortName);
        fixedColumns.add(
                new NamedColumnDescription(
                        shortName,
                        columnName,
                        getColumnValueDescription(protoOrPersistable, compression),
                        columnNameLoggable));
    }

    public void column(String columnName, String shortName, ValueType valueType) {
        column(columnName, shortName, valueType, defaultNamedComponentLogSafety);
    }

    public void column(String columnName, String shortName, ValueType valueType, boolean columnNameLoggable) {
        Preconditions.checkState(state == State.DEFINING_COLUMNS);
        Preconditions.checkState(!noColumns);
        checkUniqueColumnNames(columnName, shortName);
        fixedColumns.add(
                new NamedColumnDescription(
                        shortName, columnName, ColumnValueDescription.forType(valueType), columnNameLoggable));
    }

    public void noColumns() {
        Preconditions.checkState(state != State.DEFINING_COLUMNS);
        Preconditions.checkState(fixedColumns.isEmpty());
        fixedColumns.add(new NamedColumnDescription("e", "exists", ColumnValueDescription.forType(ValueType.VAR_LONG)));
        noColumns = true;
    }

    private void checkUniqueColumnNames(String columnName, String shortName) {
        Preconditions.checkState(!fixedColumnShortNames.contains(shortName));
        Preconditions.checkState(!fixedColumnLongNames.contains(columnName));
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
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        Preconditions.checkState(rowNameComponents.isEmpty(), "hashRowComponent must be the first row component");
        hashFirstRowComponent = true;
        ignoreHotspottingChecks = true;
    }

    public void rowComponent(String componentName, ValueType valueType) {
        rowComponent(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void rowComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        rowComponent(componentName, valueType, valueByteOrder, defaultNamedComponentLogSafety);
    }

    public void rowComponent(
            String componentName, ValueType valueType, ValueByteOrder valueByteOrder, boolean rowNameLoggable) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        rowNameComponents.add(new NameComponentDescription(componentName, valueType, valueByteOrder, rowNameLoggable));
    }

    /**
     * Passing an empty list of partitioners means that this type is not able to be partitioned. This is sometimes
     * needed if the value type doesn't support the {@link UniformRowNamePartitioner}.  If the first entry in the table
     * cannot be partitioned then it is likely this table will cause hot spots.  Consider adding
     * partitionStrategy(HASH) in this case which will allow the DB to break it up however it wants.
     * <p>
     * If no partition() is specified the default is to use a {@link UniformRowNamePartitioner}
     */
    public void partition(RowNamePartitioner... partitioners) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        NameComponentDescription last = rowNameComponents.get(rowNameComponents.size()-1);
        rowNameComponents.set(rowNameComponents.size()-1, last.withPartitioners(partitioners));
    }

    public ExplicitRowNamePartitioner explicit(String... componentValues) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        return new ExplicitRowNamePartitioner(rowNameComponents.get(rowNameComponents.size()-1).getType(), ImmutableSet.copyOf(componentValues));
    }

    public ExplicitRowNamePartitioner explicit(long... componentValues) {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        Set<String> set = Sets.newHashSet();
        for (long l : componentValues) {
            set.add(Long.toString(l));
        }
        return new ExplicitRowNamePartitioner(rowNameComponents.get(rowNameComponents.size()-1).getType(), set);
    }

    public UniformRowNamePartitioner uniform() {
        Preconditions.checkState(state == State.DEFINING_ROW_NAME);
        return new UniformRowNamePartitioner(rowNameComponents.get(rowNameComponents.size()-1).getType());
    }

    public void columnComponent(String componentName, ValueType valueType) {
        columnComponent(componentName, valueType, ValueByteOrder.ASCENDING);
    }

    public void columnComponent(String componentName, ValueType valueType, ValueByteOrder valueByteOrder) {
        Preconditions.checkState(state == State.DEFINING_DYNAMIC_COLUMN);
        dynamicColumnNameComponents.add(new NameComponentDescription(componentName, valueType, valueByteOrder));
    }

    public void value(Class<?> protoOrPersistable) {
        value(protoOrPersistable, Compression.NONE);
    }

    public void value(Class<?> protoOrPersistable, Compression compression) {
        Preconditions.checkState(state == State.DEFINING_DYNAMIC_COLUMN);
        dynamicColumnValue = getColumnValueDescription(protoOrPersistable, compression);
    }

    public void value(ValueType valueType) {
        Preconditions.checkState(state == State.DEFINING_DYNAMIC_COLUMN);
        dynamicColumnValue = ColumnValueDescription.forType(valueType);
        if (maxValueSize == Integer.MAX_VALUE) {
            maxValueSize = valueType.getMaxValueSize();
        }
    }

    public void tableConstraint(TableConstraint constraint) {
        Preconditions.checkState(state == State.DEFINING_CONSTRAINTS);
        constraintBuilder.addTableConstraint(constraint);
    }

    public void rowConstraint(RowConstraintMetadata constraint) {
        Preconditions.checkState(state == State.DEFINING_CONSTRAINTS);
        constraintBuilder.addRowConstraint(constraint);
    }

    public void foreignKeyConstraint(ForeignKeyConstraintMetadata constraint) {
        Preconditions.checkState(state == State.DEFINING_CONSTRAINTS);
        constraintBuilder.addForeignKeyConstraint(constraint);
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

    public void validate() {
        toTableMetadata();
        getConstraintMetadata();
        Preconditions.checkState(!rowNameComponents.isEmpty(), "No row name components defined.");
        validateFirstRowComp(rowNameComponents.get(0));
    }

    private enum State {
        NONE,
        DEFINING_ROW_NAME,
        DEFINING_DYNAMIC_COLUMN,
        DEFINING_COLUMNS,
        DEFINING_CONSTRAINTS,
    }

    private State state = State.NONE;
    private int maxValueSize = Integer.MAX_VALUE;
    private String genericTableName = null;
    private String javaTableName = null;
    private boolean hashFirstRowComponent = false;
    private List<NameComponentDescription> rowNameComponents = Lists.newArrayList();
    private List<NamedColumnDescription> fixedColumns = Lists.newArrayList();
    private List<NameComponentDescription> dynamicColumnNameComponents = Lists.newArrayList();
    private ColumnValueDescription dynamicColumnValue = null;
    private ConstraintMetadata.Builder constraintBuilder = ConstraintMetadata.builder();
    private Set<String> fixedColumnShortNames = Sets.newHashSet();
    private Set<String> fixedColumnLongNames = Sets.newHashSet();
    private boolean noColumns = false;
    private boolean tableNameLoggable = false;

    private boolean defaultNamedComponentLogSafety = false;

    public TableMetadata toTableMetadata() {
        Preconditions.checkState(!rowNameComponents.isEmpty(), "No row name components defined.");

        if (explicitCompressionRequested && explicitCompressionBlockSizeKB == 0) {
            if (rangeScanAllowed) {
                explicitCompressionBlockSizeKB = AtlasDbConstants.DEFAULT_TABLE_WITH_RANGESCANS_COMPRESSION_BLOCK_SIZE_KB;
            } else {
                explicitCompressionBlockSizeKB = AtlasDbConstants.DEFAULT_TABLE_COMPRESSION_BLOCK_SIZE_KB;
            }
        }

        return new TableMetadata(
                NameMetadataDescription.create(rowNameComponents, hashFirstRowComponent),
                getColumnMetadataDescription(),
                conflictHandler,
                cachePriority,
                partitionStrategy,
                rangeScanAllowed,
                explicitCompressionBlockSizeKB,
                negativeLookups,
                sweepStrategy,
                expirationStrategy,
                appendHeavyAndReadLight,
                tableNameLoggable);
    }

    private ColumnMetadataDescription getColumnMetadataDescription() {
        if (!fixedColumns.isEmpty()) {
            Preconditions.checkState(
                    dynamicColumnNameComponents.isEmpty(),
                    "Cannot define both dynamic and fixed columns.");
            return new ColumnMetadataDescription(fixedColumns);
        } else {
            Preconditions.checkState(
                    !dynamicColumnNameComponents.isEmpty() && dynamicColumnValue != null,
                    "Columns not properly defined.");
            return new ColumnMetadataDescription(
                    new DynamicColumnDescription(NameMetadataDescription.create(dynamicColumnNameComponents),
                            dynamicColumnValue));
        }
    }

    public ConstraintMetadata getConstraintMetadata() {
        return constraintBuilder.build();
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ColumnValueDescription getColumnValueDescription(Class protoOrPersistable, Compression compression) {
        if (GeneratedMessage.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forProtoMessage(protoOrPersistable, compression);
        } else if (Persister.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forPersister(protoOrPersistable, compression);
        } else if (Persistable.class.isAssignableFrom(protoOrPersistable)) {
            return ColumnValueDescription.forPersistable(protoOrPersistable, compression);
        } else {
            throw new IllegalArgumentException("Expected either protobuf or Persistable class.");
        }
    }
}
